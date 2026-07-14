package io.github.sparkredis.connector

import java.io.FileInputStream
import java.security.KeyStore
import javax.net.ssl.{SSLContext, SSLSocketFactory, TrustManagerFactory}

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{DefaultJedisClientConfig, HostAndPort, Jedis, JedisPool}

import scala.collection.concurrent.TrieMap

object RedisConnection {
  private val pools = TrieMap.empty[RedisPoolKey, JedisPool]

  // Close all cached pools when the JVM shuts down (driver or executor) so connections are not
  // leaked for the lifetime of the process.
  sys.addShutdownHook(closeAll())

  def open(options: RedisOptions): Jedis = {
    pools.getOrElseUpdate(RedisPoolKey.from(options), createPool(options)).getResource
  }

  def closeAll(): Unit = {
    pools.values.foreach { pool =>
      try pool.close() catch { case _: Throwable => () }
    }
    pools.clear()
  }

  private def createPool(options: RedisOptions): JedisPool = {
    val poolConfig = new GenericObjectPoolConfig[Jedis]()
    poolConfig.setMaxTotal(options.poolMaxTotal)
    poolConfig.setMaxIdle(options.poolMaxIdle)
    poolConfig.setMinIdle(options.poolMinIdle)

    val clientConfigBuilder = DefaultJedisClientConfig.builder()
      .connectionTimeoutMillis(options.timeoutMillis)
      .socketTimeoutMillis(options.timeoutMillis)
      .database(options.database)
      .ssl(options.sslEnabled)

    options.user.foreach(clientConfigBuilder.user)
    options.password.foreach(clientConfigBuilder.password)
    sslSocketFactory(options).foreach(clientConfigBuilder.sslSocketFactory)

    new JedisPool(poolConfig, new HostAndPort(options.host, options.port), clientConfigBuilder.build())
  }

  private def sslSocketFactory(options: RedisOptions): Option[SSLSocketFactory] = {
    if (!options.sslEnabled) {
      None
    } else {
      options.sslTruststorePath.map { path =>
        val keyStore = KeyStore.getInstance(options.sslTruststoreType)
        val password = options.sslTruststorePassword.map(_.toCharArray).orNull
        val stream = new FileInputStream(path)
        try {
          keyStore.load(stream, password)
        } finally {
          stream.close()
        }

        val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
        trustManagerFactory.init(keyStore)

        val context = SSLContext.getInstance("TLS")
        context.init(null, trustManagerFactory.getTrustManagers, null)
        context.getSocketFactory
      }
    }
  }
}

private final case class RedisPoolKey(
    host: String,
    port: Int,
    user: Option[String],
    password: Option[String],
    database: Int,
    timeoutMillis: Int,
    poolMaxTotal: Int,
    poolMaxIdle: Int,
    poolMinIdle: Int,
    sslEnabled: Boolean,
    sslTruststorePath: Option[String],
    sslTruststorePassword: Option[String],
    sslTruststoreType: String)

private object RedisPoolKey {
  def from(options: RedisOptions): RedisPoolKey = {
    RedisPoolKey(
      host = options.host,
      port = options.port,
      user = options.user,
      password = options.password,
      database = options.database,
      timeoutMillis = options.timeoutMillis,
      poolMaxTotal = options.poolMaxTotal,
      poolMaxIdle = options.poolMaxIdle,
      poolMinIdle = options.poolMinIdle,
      sslEnabled = options.sslEnabled,
      sslTruststorePath = options.sslTruststorePath,
      sslTruststorePassword = options.sslTruststorePassword,
      sslTruststoreType = options.sslTruststoreType
    )
  }
}
