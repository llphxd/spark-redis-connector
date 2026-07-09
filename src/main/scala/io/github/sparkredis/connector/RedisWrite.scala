package io.github.sparkredis.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

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
  private val pipeline = jedis.pipelined()
  private val maxCommandsBeforeFlush = math.max(1, options.writePipelineSize)
  private var pendingCommands = 0
  private var aborted = false

  override def write(record: InternalRow): Unit = {
    val logicalKey = RedisRowCodec.stringAt(record, schema, options.keyColumn)
    val key = options.keyPrefix + logicalKey

    options.dataType match {
      case RedisDataType.StringValue =>
        val value = RedisRowCodec.stringAt(record, schema, options.valueColumn)
        if (options.ttlSeconds > 0) {
          pipeline.setex(key, options.ttlSeconds.toLong, value)
        } else {
          pipeline.set(key, value)
        }
        recordCommand()

      case RedisDataType.Hash if schema.fieldNames.contains(options.fieldColumn) =>
        val field = RedisRowCodec.stringAt(record, schema, options.fieldColumn)
        val value = RedisRowCodec.stringAt(record, schema, options.valueColumn)
        pipeline.hset(key, field, value)
        recordCommand()
        expireIfNeeded(key)

      case RedisDataType.Hash =>
        val values = schema.fields.zipWithIndex.flatMap { case (field, index) =>
          if (field.name != options.keyColumn && !record.isNullAt(index)) {
            Some(field.name -> RedisRowCodec.fromCatalyst(record, index, field.dataType))
          } else {
            None
          }
        }.toMap
        if (values.nonEmpty) {
          pipeline.hset(key, values.asJava)
          recordCommand()
        }
        expireIfNeeded(key)

      case RedisDataType.SetValue =>
        pipeline.sadd(key, RedisRowCodec.stringAt(record, schema, options.valueColumn))
        recordCommand()
        expireIfNeeded(key)

      case RedisDataType.ListValue =>
        val value = RedisRowCodec.stringAt(record, schema, options.valueColumn)
        options.listWriteCommand match {
          case RedisListWriteCommand.LPush => pipeline.lpush(key, value)
          case RedisListWriteCommand.RPush => pipeline.rpush(key, value)
        }
        recordCommand()
        expireIfNeeded(key)

      case RedisDataType.SortedSet =>
        val member = RedisRowCodec.stringAt(record, schema, options.memberColumn)
        val score = RedisRowCodec.stringAt(record, schema, options.scoreColumn).toDouble
        pipeline.zadd(key, score, member)
        recordCommand()
        expireIfNeeded(key)
    }
    flushIfNeeded()
  }

  override def commit(): WriterCommitMessage = {
    flush()
    RedisWriterCommitMessage
  }

  override def abort(): Unit = {
    aborted = true
  }

  override def close(): Unit = {
    pipeline.close()
    jedis.close()
  }

  private def expireIfNeeded(key: String): Unit = {
    if (options.ttlSeconds > 0) {
      pipeline.expire(key, options.ttlSeconds.toLong)
      recordCommand()
    }
  }

  private def recordCommand(): Unit = {
    pendingCommands += 1
  }

  private def flushIfNeeded(): Unit = {
    if (!aborted && pendingCommands >= maxCommandsBeforeFlush) {
      flush()
    }
  }

  private def flush(): Unit = {
    if (!aborted && pendingCommands > 0) {
      pipeline.sync()
      pendingCommands = 0
    }
  }
}

case object RedisWriterCommitMessage extends WriterCommitMessage
