package io.github.sparkredis.connector

object RedisKeyCodec {
  def toRedisKey(logicalKey: String, options: RedisOptions): String = {
    options.keyPrefix + logicalKey
  }

  def toLogicalKey(redisKey: String, options: RedisOptions): String = {
    if (options.keyPrefix.nonEmpty && redisKey.startsWith(options.keyPrefix)) {
      redisKey.substring(options.keyPrefix.length)
    } else {
      redisKey
    }
  }
}
