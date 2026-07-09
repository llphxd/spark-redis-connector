package io.github.sparkredis.connector

import java.util

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

class RedisKeyCodecSuite extends AnyFunSuite {
  test("adds key prefix for Redis writes") {
    val options = redisOptions("key.prefix" -> "user:")

    assert(RedisKeyCodec.toRedisKey("1001", options) == "user:1001")
  }

  test("removes key prefix for Spark reads") {
    val options = redisOptions("key.prefix" -> "user:")

    assert(RedisKeyCodec.toLogicalKey("user:1001", options) == "1001")
  }

  test("keeps unmatched Redis keys unchanged") {
    val options = redisOptions("key.prefix" -> "user:")

    assert(RedisKeyCodec.toLogicalKey("order:1001", options) == "order:1001")
  }

  private def redisOptions(values: (String, String)*): RedisOptions = {
    val raw = new util.HashMap[String, String]()
    values.foreach { case (key, value) => raw.put(key, value) }
    RedisOptions.from(new CaseInsensitiveStringMap(raw))
  }
}
