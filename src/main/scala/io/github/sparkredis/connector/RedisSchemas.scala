package io.github.sparkredis.connector

import org.apache.spark.sql.types._

object RedisSchemas {
  def defaultSchema(options: RedisOptions): StructType = {
    options.dataType match {
      case RedisDataType.StringValue =>
        StructType(Seq(
          StructField(options.keyColumn, StringType, nullable = false),
          StructField(options.valueColumn, StringType, nullable = true)
        ))

      case RedisDataType.Hash =>
        StructType(Seq(
          StructField(options.keyColumn, StringType, nullable = false),
          StructField(options.fieldColumn, StringType, nullable = false),
          StructField(options.valueColumn, StringType, nullable = true)
        ))

      case RedisDataType.SetValue =>
        StructType(Seq(
          StructField(options.keyColumn, StringType, nullable = false),
          StructField(options.valueColumn, StringType, nullable = true)
        ))

      case RedisDataType.ListValue =>
        options.listReadMode match {
          case RedisListReadMode.Explode =>
            StructType(Seq(
              StructField(options.keyColumn, StringType, nullable = false),
              StructField("index", LongType, nullable = false),
              StructField(options.valueColumn, StringType, nullable = true)
            ))
          case RedisListReadMode.Array =>
            StructType(Seq(
              StructField(options.keyColumn, StringType, nullable = false),
              StructField("values", ArrayType(StringType, containsNull = true), nullable = true)
            ))
        }

      case RedisDataType.SortedSet =>
        StructType(Seq(
          StructField(options.keyColumn, StringType, nullable = false),
          StructField(options.memberColumn, StringType, nullable = true),
          StructField(options.scoreColumn, DoubleType, nullable = false)
        ))
    }
  }
}
