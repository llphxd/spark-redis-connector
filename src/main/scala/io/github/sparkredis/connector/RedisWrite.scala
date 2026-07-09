package io.github.sparkredis.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType

class RedisWriteBuilder(schema: StructType, options: RedisOptions) extends WriteBuilder with SupportsTruncate {
  private var truncateRequested = false

  override def truncate(): WriteBuilder = {
    truncateRequested = true
    this
  }

  override def build(): Write = new RedisWrite(schema, options, truncateRequested)
}

class RedisWrite(schema: StructType, options: RedisOptions, truncateRequested: Boolean) extends Write {
  override def toBatch: BatchWrite = new RedisBatchWrite(schema, options, truncateRequested)
}

class RedisBatchWrite(schema: StructType, options: RedisOptions, truncateRequested: Boolean) extends BatchWrite {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    if (truncateRequested) {
      RedisKeyScanner.deleteKeys(options)
    }
    RedisDataWriterFactory(schema, options)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

final case class RedisDataWriterFactory(schema: StructType, options: RedisOptions) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new RedisDataWriter(schema, options)
  }
}

class RedisDataWriter(schema: StructType, options: RedisOptions) extends DataWriter[InternalRow] {
  private val jedis = RedisConnection.open(options)

  override def write(record: InternalRow): Unit = {
    val logicalKey = RedisRowCodec.stringAt(record, schema, options.keyColumn)
    val key = options.keyPrefix + logicalKey

    options.dataType match {
      case RedisDataType.StringValue =>
        val value = RedisRowCodec.stringAt(record, schema, options.valueColumn)
        if (options.ttlSeconds > 0) jedis.setex(key, options.ttlSeconds.toLong, value) else jedis.set(key, value)

      case RedisDataType.Hash if schema.fieldNames.contains(options.fieldColumn) =>
        val field = RedisRowCodec.stringAt(record, schema, options.fieldColumn)
        val value = RedisRowCodec.stringAt(record, schema, options.valueColumn)
        jedis.hset(key, field, value)
        expireIfNeeded(key)

      case RedisDataType.Hash =>
        schema.fields.zipWithIndex.foreach { case (field, index) =>
          if (field.name != options.keyColumn && !record.isNullAt(index)) {
            jedis.hset(key, field.name, RedisRowCodec.fromCatalyst(record, index, field.dataType))
          }
        }
        expireIfNeeded(key)

      case RedisDataType.SetValue =>
        jedis.sadd(key, RedisRowCodec.stringAt(record, schema, options.valueColumn))
        expireIfNeeded(key)

      case RedisDataType.ListValue =>
        val value = RedisRowCodec.stringAt(record, schema, options.valueColumn)
        options.listWriteCommand match {
          case RedisListWriteCommand.LPush => jedis.lpush(key, value)
          case RedisListWriteCommand.RPush => jedis.rpush(key, value)
        }
        expireIfNeeded(key)

      case RedisDataType.SortedSet =>
        val member = RedisRowCodec.stringAt(record, schema, options.memberColumn)
        val score = RedisRowCodec.stringAt(record, schema, options.scoreColumn).toDouble
        jedis.zadd(key, score, member)
        expireIfNeeded(key)
    }
  }

  override def commit(): WriterCommitMessage = RedisWriterCommitMessage

  override def abort(): Unit = {}

  override def close(): Unit = jedis.close()

  private def expireIfNeeded(key: String): Unit = {
    if (options.ttlSeconds > 0) {
      jedis.expire(key, options.ttlSeconds.toLong)
    }
  }
}

case object RedisWriterCommitMessage extends WriterCommitMessage
