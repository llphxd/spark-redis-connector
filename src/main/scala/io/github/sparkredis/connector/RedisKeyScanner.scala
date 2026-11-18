package io.github.sparkredis.connector

import redis.clients.jedis.Jedis
import redis.clients.jedis.params.ScanParams

import scala.collection.JavaConverters._
import scala.collection.mutable.LinkedHashSet

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
      scanKeys(jedis, pattern, options.scanCount)
    } finally {
      jedis.close()
    }
  }

  private[connector] def scanKeys(jedis: Jedis, pattern: String, scanCount: Int): Seq[String] = {
    val params = new ScanParams().`match`(pattern).count(math.max(1, scanCount))
    val keys = LinkedHashSet.empty[String]
    var cursor = ScanParams.SCAN_POINTER_START
    do {
      val result = jedis.scan(cursor, params)
      keys ++= result.getResult.asScala
      cursor = result.getCursor
    } while (cursor != ScanParams.SCAN_POINTER_START)
    keys.toVector
  }
}
