package io.github.sparkredis.connector

import java.util

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType

class RedisTable(tableSchema: StructType, options: RedisOptions)
    extends Table
    with SupportsRead
    with SupportsWrite {

  override def name(): String = s"redis.${options.dataType.name}"

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {
    val caps = new util.HashSet[TableCapability]()
    caps.add(TableCapability.BATCH_READ)
    caps.add(TableCapability.BATCH_WRITE)
    caps.add(TableCapability.TRUNCATE)
    caps
  }

  override def newScanBuilder(optionsMap: org.apache.spark.sql.util.CaseInsensitiveStringMap): ScanBuilder = {
    new RedisScanBuilder(tableSchema, options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new RedisWriteBuilder(tableSchema, options)
  }
}
