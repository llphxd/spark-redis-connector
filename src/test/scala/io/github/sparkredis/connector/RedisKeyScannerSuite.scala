package io.github.sparkredis.connector

import org.scalatest.funsuite.AnyFunSuite
import redis.clients.jedis.Jedis

class RedisKeyScannerSuite extends AnyFunSuite {
  test("SCAN keeps each key once and follows the cursor through empty pages") {
    val firstPage = "*2\r\n$1\r\n7\r\n*2\r\n$1\r\na\r\n$1\r\na\r\n"
    val emptyPage = "*2\r\n$1\r\n4\r\n*0\r\n"
    val finalPage = "*2\r\n$1\r\n0\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n"
    val socket = new RedisTestSocket(firstPage + emptyPage + finalPage)
    val jedis = new Jedis(() => socket)
    try {
      assert(RedisKeyScanner.scanKeys(jedis, "*", 2) == Seq("a", "b"))
    } finally jedis.close()
  }
}
