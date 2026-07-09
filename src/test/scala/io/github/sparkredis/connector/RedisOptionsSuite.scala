package io.github.sparkredis.connector

import java.util

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

class RedisOptionsSuite extends AnyFunSuite {
  test("parses defaults") {
    val options = RedisOptions.from(new CaseInsensitiveStringMap(new util.HashMap[String, String]()))

    assert(options.host == "localhost")
    assert(options.port == 6379)
    assert(options.dataType == RedisDataType.Hash)
    assert(options.keyColumn == "key")
    assert(options.scanCount == 1000)
    assert(options.keysPerPartition == 1000)
    assert(options.listReadMode == RedisListReadMode.Explode)
    assert(options.listWriteCommand == RedisListWriteCommand.RPush)
  }

  test("parses case-insensitive options") {
    val raw = new util.HashMap[String, String]()
    raw.put("HOST", "redis.local")
    raw.put("PORT", "6380")
    raw.put("TYPE", "zset")
    raw.put("KEYS", "a,b, c")
    raw.put("KEYS.PER.PARTITION", "50")
    raw.put("LIST.READ.MODE", "ARRAY")
    raw.put("LIST.WRITE.COMMAND", "LPUSH")

    val options = RedisOptions.from(new CaseInsensitiveStringMap(raw))

    assert(options.host == "redis.local")
    assert(options.port == 6380)
    assert(options.dataType == RedisDataType.SortedSet)
    assert(options.keys.contains(Seq("a", "b", "c")))
    assert(options.keysPerPartition == 50)
    assert(options.listReadMode == RedisListReadMode.Array)
    assert(options.listWriteCommand == RedisListWriteCommand.LPush)
  }
}
