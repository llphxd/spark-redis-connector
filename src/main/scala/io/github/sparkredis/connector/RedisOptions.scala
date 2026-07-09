package io.github.sparkredis.connector

import org.apache.spark.sql.util.CaseInsensitiveStringMap

final case class RedisOptions(
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
    writePipelineSize: Int,
    ttlSeconds: Int) extends Serializable

object RedisOptions {
  val ProviderShortName = "redis"

  val Type = "type"
  val Host = "host"
  val Port = "port"
  val User = "user"
  val Password = "password"
  val Database = "database"
  val Timeout = "timeout"
  val PoolMaxTotal = "connection.pool.max.total"
  val PoolMaxIdle = "connection.pool.max.idle"
  val PoolMinIdle = "connection.pool.min.idle"
  val SslEnabled = "ssl.enabled"
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
  val WritePipelineSize = "write.pipeline.size"
  val Ttl = "ttl"

  def from(options: CaseInsensitiveStringMap): RedisOptions = {
    def opt(name: String): Option[String] = Option(options.get(name)).filter(_.nonEmpty)
    def str(name: String, default: String): String = opt(name).getOrElse(default)
    def int(name: String, default: Int): Int = opt(name).map(_.toInt).getOrElse(default)
    def bool(name: String, default: Boolean): Boolean = opt(name).map(_.toBoolean).getOrElse(default)

    RedisOptions(
      host = str(Host, "localhost"),
      port = int(Port, 6379),
      user = opt(User),
      password = opt(Password),
      database = int(Database, 0),
      timeoutMillis = int(Timeout, 2000),
      poolMaxTotal = int(PoolMaxTotal, 8),
      poolMaxIdle = int(PoolMaxIdle, 8),
      poolMinIdle = int(PoolMinIdle, 0),
      sslEnabled = bool(SslEnabled, false),
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
      writePipelineSize = int(WritePipelineSize, 1000),
      ttlSeconds = int(Ttl, 0)
    )
  }
}
