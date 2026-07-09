package io.github.sparkredis.catalyst

import redis.clients.jedis.Jedis

object RedisConnection {
  def open(options: RedisOptions): Jedis = {
    val jedis = new Jedis(options.host, options.port, options.timeoutMillis)
    options.password.foreach { password =>
      options.user match {
        case Some(user) => jedis.auth(user, password)
        case None => jedis.auth(password)
      }
    }
    if (options.database != 0) {
      jedis.select(options.database)
    }
    jedis
  }
}
