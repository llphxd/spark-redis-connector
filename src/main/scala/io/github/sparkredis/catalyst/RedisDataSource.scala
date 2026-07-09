package io.github.sparkredis.catalyst

import java.util

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class RedisDataSource extends TableProvider with DataSourceRegister {
  override def shortName(): String = RedisOptions.ProviderShortName

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    RedisSchemas.defaultSchema(RedisOptions.from(options))
  }

  override def supportsExternalMetadata(): Boolean = true

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val options = RedisOptions.from(new CaseInsensitiveStringMap(properties))
    val tableSchema =
      if (schema == null || schema.isEmpty) RedisSchemas.defaultSchema(options) else schema
    new RedisTable(tableSchema, options)
  }
}
