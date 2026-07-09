package io.github.sparkredis.connector

import org.apache.spark.sql.util.CaseInsensitiveStringMap

final case class RedisOptions(
    host: String,
    port: Int,
    user: Option[String],
    password: Option[String],
    database: Int,
    connectTimeoutMillis: Int,
    socketTimeoutMillis: Int,
    poolMaxTotal: Int,
    poolMaxIdle: Int,
    poolMinIdle: Int,
    sslEnabled: Boolean,
    sslTruststorePath: Option[String],
    sslTruststorePassword: Option[String],
    sslTruststoreType: String,
    dataType: RedisDataType,
    keysPattern: Option[String],
    keys: Option[Seq[String]],
    keyColumn: String,
    keyPrefix: String,
    valueColumn: String,
    memberColumn: String,
    scoreColumn: String,
    fieldColumn: String,
    listReadMode: RedisListReadMode,
    listWriteCommand: RedisListWriteCommand,
    scanCount: Int,
    keysPerPartition: Int,
    writeBatchSize: Int,
    writePipelineSize: Int,
    ttlSeconds: Int) extends Serializable

object RedisOptions {
  val ProviderShortName = "redis"

  val Type = "type"
  val Host = "host"
  val Port = "port"
  val User = "user"
  val Password = "password"
  val Auth = "auth"
  val Database = "database"
  val DbNum = "dbNum"
  val Timeout = "timeout"
  val ConnectTimeout = "connect.timeout"
  val SocketTimeout = "socket.timeout"
  val PoolMaxTotal = "connection.pool.max.total"
  val PoolMaxTotalAlias = "connection.pool.max-total"
  val PoolMaxIdle = "connection.pool.max.idle"
  val PoolMaxIdleAlias = "connection.pool.max-idle"
  val PoolMinIdle = "connection.pool.min.idle"
  val PoolMinIdleAlias = "connection.pool.min-idle"
  val SslEnabled = "ssl.enabled"
  val Ssl = "ssl"
  val SslTruststorePath = "ssl.truststore.path"
  val SslTruststorePassword = "ssl.truststore.password"
  val SslTruststoreType = "ssl.truststore.type"
  val KeysPattern = "keys.pattern"
  val Keys = "keys"
  val KeyColumn = "key.column"
  val KeyPrefix = "key.prefix"
  val ValueColumn = "value.column"
  val MemberColumn = "member.column"
  val ScoreColumn = "score.column"
  val FieldColumn = "field.column"
  val ListReadMode = "list.read.mode"
  val ListWriteCommand = "list.write.command"
  val ScanCount = "scan.count"
  val KeysPerPartition = "keys.per.partition"
  val WriteBatchSize = "write.batch.size"
  val WritePipelineSize = "write.pipeline.size"
  val PipelineSize = "pipeline.size"
  val Ttl = "ttl"

  def from(options: CaseInsensitiveStringMap): RedisOptions = {
    def opt(name: String): Option[String] = Option(options.get(name)).filter(_.nonEmpty)
    def str(name: String, default: String): String = opt(name).getOrElse(default)
    def int(name: String, default: Int): Int = opt(name).map(_.toInt).getOrElse(default)
    def firstInt(names: Seq[String], default: Int): Int = names.iterator.flatMap(opt).toSeq.headOption.map(_.toInt).getOrElse(default)
    def firstBool(names: Seq[String], default: Boolean): Boolean =
      names.iterator.flatMap(opt).toSeq.headOption.map(_.toBoolean).getOrElse(default)

    val legacyTimeoutMillis = int(Timeout, 2000)

    RedisOptions(
      host = str(Host, "localhost"),
      port = int(Port, 6379),
      user = opt(User),
      password = opt(Password).orElse(opt(Auth)),
      database = opt(Database).orElse(opt(DbNum)).map(_.toInt).getOrElse(0),
      connectTimeoutMillis = int(ConnectTimeout, legacyTimeoutMillis),
      socketTimeoutMillis = int(SocketTimeout, legacyTimeoutMillis),
      poolMaxTotal = firstInt(Seq(PoolMaxTotal, PoolMaxTotalAlias), 8),
      poolMaxIdle = firstInt(Seq(PoolMaxIdle, PoolMaxIdleAlias), 8),
      poolMinIdle = firstInt(Seq(PoolMinIdle, PoolMinIdleAlias), 0),
      sslEnabled = firstBool(Seq(SslEnabled, Ssl), false),
      sslTruststorePath = opt(SslTruststorePath),
      sslTruststorePassword = opt(SslTruststorePassword),
      sslTruststoreType = str(SslTruststoreType, "JKS"),
      dataType = RedisDataType.parse(str(Type, RedisDataType.Hash.name)),
      keysPattern = opt(KeysPattern),
      keys = opt(Keys).map(_.split(",").iterator.map(_.trim).filter(_.nonEmpty).toSeq),
      keyColumn = str(KeyColumn, "key"),
      keyPrefix = str(KeyPrefix, ""),
      valueColumn = str(ValueColumn, "value"),
      memberColumn = str(MemberColumn, "member"),
      scoreColumn = str(ScoreColumn, "score"),
      fieldColumn = str(FieldColumn, "field"),
      listReadMode = RedisListReadMode.parse(str(ListReadMode, RedisListReadMode.Explode.name)),
      listWriteCommand = RedisListWriteCommand.parse(str(ListWriteCommand, RedisListWriteCommand.RPush.name)),
      scanCount = int(ScanCount, 1000),
      keysPerPartition = int(KeysPerPartition, 1000),
      writeBatchSize = int(WriteBatchSize, 1000),
      writePipelineSize = firstInt(Seq(WritePipelineSize, PipelineSize), 1000),
      ttlSeconds = int(Ttl, 0)
    )
  }
}
