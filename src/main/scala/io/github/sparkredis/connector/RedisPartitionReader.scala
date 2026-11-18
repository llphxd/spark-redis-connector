package io.github.sparkredis.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisDataException

import scala.collection.JavaConverters._

class RedisPartitionReader private[connector] (
    schema: StructType, options: RedisOptions, hashFieldMode: Boolean, keys: Seq[String], jedis: Jedis)
    extends PartitionReader[InternalRow] {

  def this(schema: StructType, options: RedisOptions, hashFieldMode: Boolean, keys: Seq[String]) =
    this(schema, options, hashFieldMode, keys, RedisConnection.open(options))

  private val projectedHashFields = schema.fieldNames.filterNot(_ == options.keyColumn)
  private val readsStringValue = schema.fieldNames.contains(options.valueColumn)
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
          readStringRow(key, logicalKey)

        case RedisDataType.Hash if hashFieldMode =>
          jedis.hgetAll(key).asScala.iterator.map { case (field, value) =>
            row(Map(options.keyColumn -> logicalKey, options.fieldColumn -> field, options.valueColumn -> value))
          }

        case RedisDataType.Hash =>
          readHashRow(key, logicalKey)

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

  private def readStringRow(key: String, logicalKey: String): Iterator[InternalRow] = {
    if (readsStringValue) {
      Option(jedis.get(key)).iterator.map { value =>
        row(Map(options.keyColumn -> logicalKey, options.valueColumn -> value))
      }
    } else {
      readKeyOnlyRow(key, logicalKey)
    }
  }

  private def readHashRow(key: String, logicalKey: String): Iterator[InternalRow] = {
    if (projectedHashFields.isEmpty) {
      readKeyOnlyRow(key, logicalKey)
    } else {
      val values = jedis.hmget(key, projectedHashFields: _*).asScala
      // HMGET returns all nulls both for a missing key and for a hash lacking the requested fields.
      if (values.exists(_ != null) || jedis.`type`(key) == options.dataType.name) {
        Iterator(row((projectedHashFields.zip(values) :+ (options.keyColumn -> logicalKey)).toMap))
      } else {
        Iterator.empty
      }
    }
  }

  private def readKeyOnlyRow(key: String, logicalKey: String): Iterator[InternalRow] = {
    if (jedis.`type`(key) == options.dataType.name) Iterator(row(Map(options.keyColumn -> logicalKey)))
    else Iterator.empty
  }

  private def readList(key: String, logicalKey: String): Iterator[InternalRow] = {
    val values = jedis.lrange(key, 0, -1).asScala
    options.listReadMode match {
      case RedisListReadMode.Explode =>
        values.iterator.zipWithIndex.map { case (value, index) =>
          row(Map(options.keyColumn -> logicalKey, "index" -> index.toLong, options.valueColumn -> value))
        }
      case RedisListReadMode.Array if values.nonEmpty =>
        Iterator(row(Map(options.keyColumn -> logicalKey, "values" -> values.toSeq)))
      case RedisListReadMode.Array => Iterator.empty
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
