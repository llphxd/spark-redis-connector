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
    assert(options.timeoutMillis == 2000)
    assert(options.poolMaxTotal == 8)
    assert(options.poolMaxIdle == 8)
    assert(options.poolMinIdle == 0)
    assert(!options.sslEnabled)
    assert(options.writePipelineSize == 1000)
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
    raw.put("TIMEOUT", "3000")
    raw.put("CONNECTION.POOL.MAX.TOTAL", "16")
    raw.put("CONNECTION.POOL.MAX.IDLE", "12")
    raw.put("CONNECTION.POOL.MIN.IDLE", "2")
    raw.put("SSL.ENABLED", "true")
    raw.put("SSL.TRUSTSTORE.PATH", "/tmp/redis.jks")
    raw.put("SSL.TRUSTSTORE.PASSWORD", "changeit")
    raw.put("SSL.TRUSTSTORE.TYPE", "PKCS12")
    raw.put("WRITE.PIPELINE.SIZE", "100")

    val options = RedisOptions.from(new CaseInsensitiveStringMap(raw))

    assert(options.host == "redis.local")
    assert(options.port == 6380)
    assert(options.dataType == RedisDataType.SortedSet)
    assert(options.keys.contains(Seq("a", "b", "c")))
    assert(options.keysPerPartition == 50)
    assert(options.listReadMode == RedisListReadMode.Array)
    assert(options.listWriteCommand == RedisListWriteCommand.LPush)
    assert(options.timeoutMillis == 3000)
    assert(options.poolMaxTotal == 16)
    assert(options.poolMaxIdle == 12)
    assert(options.poolMinIdle == 2)
    assert(options.sslEnabled)
    assert(options.sslTruststorePath.contains("/tmp/redis.jks"))
    assert(options.sslTruststorePassword.contains("changeit"))
    assert(options.sslTruststoreType == "PKCS12")
    assert(options.writePipelineSize == 100)
  }
}
