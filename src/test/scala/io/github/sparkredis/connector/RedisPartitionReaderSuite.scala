package io.github.sparkredis.connector

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import redis.clients.jedis.Jedis

class RedisPartitionReaderSuite extends AnyFunSuite {
  private val candidates = Seq("present", "wrong-type", "missing")
  private val wrongType = "-WRONGTYPE incompatible Redis type\r\n"
  private val keySchema = new StructType().add("key", StringType)

  test("string reads keep their row count when Spark prunes the value") {
    val options = optionsFor("string")
    val fullSchema = RedisSchemas.defaultSchema(options)
    val full = readRows(options, fullSchema, "$5\r\nvalue\r\n" + wrongType + "$-1\r\n")
    val keyOnly = readRows(options, keySchema, "+string\r\n+hash\r\n+none\r\n")
    val emptyProjection = readRows(options, new StructType(), "+string\r\n+hash\r\n+none\r\n")

    assert(full.map(_.getUTF8String(0).toString) == Seq("present"))
    assert(keyOnly.map(_.getUTF8String(0).toString) == Seq("present"))
    assert(emptyProjection.size == 1)
  }

  test("wide hash reads retain existing hashes with null fields and skip missing or wrong-type keys") {
    val options = optionsFor("hash")
    val fullSchema = keySchema.add("name", StringType)
    val missingField = "*1\r\n$-1\r\n"
    val full = readRows(options, fullSchema, s"${missingField}+hash\r\n${wrongType}${missingField}+none\r\n")
    val keyOnly = readRows(options, keySchema, "+hash\r\n+string\r\n+none\r\n")
    val emptyProjection = readRows(options, new StructType(), "+hash\r\n+string\r\n+none\r\n")

    assert(full.map(_.getUTF8String(0).toString) == Seq("present"))
    assert(full.head.isNullAt(1))
    assert(keyOnly.map(_.getUTF8String(0).toString) == Seq("present"))
    assert(emptyProjection.size == 1)
  }

  test("array list reads skip missing lists") {
    val options = optionsFor("list").copy(listReadMode = RedisListReadMode.Array)
    val listReply = "*1\r\n$5\r\nvalue\r\n"
    val rows = readRows(options, RedisSchemas.defaultSchema(options), s"${listReply}${wrongType}*0\r\n")

    assert(rows.map(_.getUTF8String(0).toString) == Seq("present"))
    assert(rows.head.getArray(1).getUTF8String(0).toString == "value")
  }

  private def optionsFor(redisType: String): RedisOptions = {
    RedisOptions.from(new CaseInsensitiveStringMap(util.Collections.singletonMap("type", redisType)))
  }

  private def readRows(
      options: RedisOptions, schema: StructType, replies: String): Vector[InternalRow] = {
    val socket = new RedisTestSocket(replies)
    val reader = new RedisPartitionReader(schema, options, false, candidates, new Jedis(() => socket))
    val rows = Vector.newBuilder[InternalRow]
    try {
      while (reader.next()) rows += reader.get()
      rows.result()
    } finally reader.close()
  }
}
