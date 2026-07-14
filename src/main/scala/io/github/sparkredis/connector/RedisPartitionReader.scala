package io.github.sparkredis.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import redis.clients.jedis.exceptions.JedisDataException

import scala.collection.JavaConverters._

class RedisPartitionReader(schema: StructType, options: RedisOptions, hashFieldMode: Boolean, keys: Seq[String])
    extends PartitionReader[InternalRow] {

  private val jedis = RedisConnection.open(options)
  private val rows = keys.iterator.flatMap(readKey)
  private var current: InternalRow = _

  override def next(): Boolean = {
    if (rows.hasNext) {
      current = rows.next()
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = current

  override def close(): Unit = jedis.close()

  private def readKey(key: String): Iterator[InternalRow] = {
    val logicalKey = RedisKeyCodec.toLogicalKey(key, options)
    try {
      options.dataType match {
        case RedisDataType.StringValue =>
          val values = Map.newBuilder[String, Any]
          values += options.keyColumn -> logicalKey
          if (schema.fieldNames.contains(options.valueColumn)) {
            values += options.valueColumn -> jedis.get(key)
          }
          Iterator(row(values.result()))

        case RedisDataType.Hash if hashFieldMode =>
          jedis.hgetAll(key).asScala.iterator.map { case (field, value) =>
            row(Map(options.keyColumn -> logicalKey, options.fieldColumn -> field, options.valueColumn -> value))
          }

        case RedisDataType.Hash =>
          Iterator(readHashRow(key, logicalKey))

        case RedisDataType.SetValue =>
          jedis.smembers(key).asScala.iterator.map { value =>
            row(Map(options.keyColumn -> logicalKey, options.valueColumn -> value))
          }

        case RedisDataType.ListValue =>
          readList(key, logicalKey)

        case RedisDataType.SortedSet =>
          jedis.zrangeWithScores(key, 0, -1).asScala.iterator.map { tuple =>
            row(Map(
              options.keyColumn -> logicalKey,
              options.memberColumn -> tuple.getElement,
              options.scoreColumn -> tuple.getScore
            ))
          }
      }
    } catch {
      case e: JedisDataException if isWrongType(e) => Iterator.empty
    }
  }

  private def readHashRow(key: String, logicalKey: String): InternalRow = {
    val fields = schema.fields.map(_.name).filterNot(_ == options.keyColumn)
    if (fields.isEmpty) {
      row(Map(options.keyColumn -> logicalKey))
    } else {
      val values = jedis.hmget(key, fields: _*).asScala
      row((fields.zip(values) :+ (options.keyColumn -> logicalKey)).toMap)
    }
  }

  private def readList(key: String, logicalKey: String): Iterator[InternalRow] = {
    val values = jedis.lrange(key, 0, -1).asScala
    options.listReadMode match {
      case RedisListReadMode.Explode =>
        values.iterator.zipWithIndex.map { case (value, index) =>
          row(Map(options.keyColumn -> logicalKey, "index" -> index.toLong, options.valueColumn -> value))
        }
      case RedisListReadMode.Array =>
        Iterator(row(Map(options.keyColumn -> logicalKey, "values" -> values.toSeq)))
    }
  }

  private def row(valuesByColumn: Map[String, Any]): InternalRow = {
    RedisRowCodec.row(schema.fields.map { field =>
      valuesByColumn.getOrElse(field.name, null)
    }, schema)
  }

  private def isWrongType(error: JedisDataException): Boolean = {
    Option(error.getMessage).exists(_.contains("WRONGTYPE"))
  }
}
