package io.github.sparkredis.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType
import redis.clients.jedis.{Jedis, Response}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

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

class RedisDataWriter private[connector] (schema: StructType, options: RedisOptions, jedis: Jedis)
    extends DataWriter[InternalRow] {
  // Use a dedicated (non-pooled) connection: on abort we close the socket to discard any buffered
  // pipeline commands, which would corrupt a shared pooled connection if it were returned.
  def this(schema: StructType, options: RedisOptions) =
    this(schema, options, RedisConnection.openStandalone(options))

  private val pipeline = jedis.pipelined()
  private val maxCommandsBeforeFlush = math.max(1, options.writePipelineSize)
  private val pendingResponses = ArrayBuffer.empty[Response[_]]
  private var aborted = false
  // Keys whose TTL has already been set in this task, so EXPIRE is issued once per key instead of
  // once per row.
  private val expiredKeys = scala.collection.mutable.HashSet.empty[String]

  override def write(record: InternalRow): Unit = {
    val logicalKey = RedisRowCodec.stringAt(record, schema, options.keyColumn)
    val key = RedisKeyCodec.toRedisKey(logicalKey, options)

    options.dataType match {
      case RedisDataType.StringValue =>
        val value = RedisRowCodec.stringAt(record, schema, options.valueColumn)
        if (options.ttlSeconds > 0) {
          pendingResponses += pipeline.setex(key, options.ttlSeconds.toLong, value)
        } else {
          pendingResponses += pipeline.set(key, value)
        }

      case RedisDataType.Hash if schema.fieldNames.contains(options.fieldColumn) =>
        val field = RedisRowCodec.stringAt(record, schema, options.fieldColumn)
        val value = RedisRowCodec.stringAt(record, schema, options.valueColumn)
        pendingResponses += pipeline.hset(key, field, value)
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
          pendingResponses += pipeline.hset(key, values.asJava)
        }
        expireIfNeeded(key)

      case RedisDataType.SetValue =>
        pendingResponses += pipeline.sadd(key, RedisRowCodec.stringAt(record, schema, options.valueColumn))
        expireIfNeeded(key)

      case RedisDataType.ListValue =>
        val value = RedisRowCodec.stringAt(record, schema, options.valueColumn)
        options.listWriteCommand match {
          case RedisListWriteCommand.LPush => pendingResponses += pipeline.lpush(key, value)
          case RedisListWriteCommand.RPush => pendingResponses += pipeline.rpush(key, value)
        }
        expireIfNeeded(key)

      case RedisDataType.SortedSet =>
        val member = RedisRowCodec.stringAt(record, schema, options.memberColumn)
        val score = RedisRowCodec.stringAt(record, schema, options.scoreColumn).toDouble
        pendingResponses += pipeline.zadd(key, score, member)
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
    if (aborted) {
      // Close the socket directly without syncing so buffered-but-unsent commands are discarded.
      // Note: writes are not transactional, so rows already flushed to Redis remain.
      jedis.close()
    } else {
      pipeline.close()
      jedis.close()
    }
  }

  private def expireIfNeeded(key: String): Unit = {
    if (options.ttlSeconds > 0 && expiredKeys.add(key)) {
      pendingResponses += pipeline.expire(key, options.ttlSeconds.toLong)
    }
  }

  private def flushIfNeeded(): Unit = {
    if (pendingResponses.size >= maxCommandsBeforeFlush) {
      flush()
    }
  }

  private def flush(): Unit = {
    if (!aborted && pendingResponses.nonEmpty) {
      pipeline.sync()
      // Jedis stores Redis command errors in the responses and throws them from get().
      pendingResponses.foreach(_.get())
      pendingResponses.clear()
    }
  }
}

case object RedisWriterCommitMessage extends WriterCommitMessage
