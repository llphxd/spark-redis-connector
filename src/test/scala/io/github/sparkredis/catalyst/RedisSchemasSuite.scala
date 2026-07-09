package io.github.sparkredis.catalyst

import java.util

import org.apache.spark.sql.types.{ArrayType, DoubleType, LongType, StringType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

class RedisSchemasSuite extends AnyFunSuite {
  test("string schema contains key and value") {
    val schema = RedisSchemas.defaultSchema(options("string"))

    assert(schema("key").dataType == StringType)
    assert(schema("value").dataType == StringType)
  }

  test("hash schema defaults to field value rows") {
    val schema = RedisSchemas.defaultSchema(options("hash"))

    assert(schema.fieldNames.toSeq == Seq("key", "field", "value"))
  }

  test("list schema contains index") {
    val schema = RedisSchemas.defaultSchema(options("list"))

    assert(schema("index").dataType == LongType)
  }

  test("list array mode schema contains values array") {
    val raw = new util.HashMap[String, String]()
    raw.put("type", "list")
    raw.put("list.read.mode", "array")

    val schema = RedisSchemas.defaultSchema(RedisOptions.from(new CaseInsensitiveStringMap(raw)))

    assert(schema.fieldNames.toSeq == Seq("key", "values"))
    assert(schema("values").dataType == ArrayType(StringType, containsNull = true))
  }

  test("zset schema contains member and score") {
    val schema = RedisSchemas.defaultSchema(options("zset"))

    assert(schema("member").dataType == StringType)
    assert(schema("score").dataType == DoubleType)
  }

  private def options(dataType: String): RedisOptions = {
    val raw = new util.HashMap[String, String]()
    raw.put("type", dataType)
    RedisOptions.from(new CaseInsensitiveStringMap(raw))
  }
}
