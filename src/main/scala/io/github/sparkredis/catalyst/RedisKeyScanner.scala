package io.github.sparkredis.catalyst

import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.resps.ScanResult

import scala.collection.JavaConverters._

object RedisKeyScanner {
  def loadKeys(options: RedisOptions): Seq[String] = {
    options.keys.getOrElse(scanKeys(options))
  }

  def deleteKeys(options: RedisOptions): Unit = {
    val keys = loadKeys(options)
    if (keys.nonEmpty) {
      val jedis = RedisConnection.open(options)
      try {
        keys.grouped(math.max(1, options.keysPerPartition)).foreach { batch =>
          jedis.del(batch: _*)
        }
      } finally {
        jedis.close()
      }
    }
  }

  private def scanKeys(options: RedisOptions): Seq[String] = {
    val pattern = options.keysPattern.getOrElse {
      throw new IllegalArgumentException("Either 'keys' or 'keys.pattern' must be provided.")
    }
    val jedis = RedisConnection.open(options)
    try {
      val params = new ScanParams()
        .`match`(pattern)
        .count(math.max(1, options.scanCount))
      val keys = Vector.newBuilder[String]
      var cursor = ScanParams.SCAN_POINTER_START
      do {
        val result: ScanResult[String] = jedis.scan(cursor, params)
        keys ++= result.getResult.asScala
        cursor = result.getCursor
      } while (cursor != ScanParams.SCAN_POINTER_START)
      keys.result()
    } finally {
      jedis.close()
    }
  }
}
