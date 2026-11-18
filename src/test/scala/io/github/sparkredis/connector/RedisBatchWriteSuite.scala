package io.github.sparkredis.connector

import java.util

import org.apache.spark.sql.connector.write.PhysicalWriteInfo
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

class RedisBatchWriteSuite extends AnyFunSuite {
  private val writeInfo: PhysicalWriteInfo = () => 1

  test("overwrite validates required columns before discovering keys to delete") {
    val options = optionsFor("zset").copy(keyColumn = "id", memberColumn = "name", scoreColumn = "points")
    val schema = new StructType().add("id", StringType)
    val write = new RedisBatchWrite(schema, options, truncateRequested = true)

    val error = intercept[IllegalArgumentException](write.createBatchWriterFactory(writeInfo))
    assert(error.getMessage == "Redis zset write is missing required columns: name, points")
  }

  test("hash writes require a value column only in field-value mode") {
    val options = optionsFor("hash")
    val wideSchema = new StructType().add("key", StringType).add("name", StringType)
    val wideWrite = new RedisBatchWrite(wideSchema, options, truncateRequested = false)
    assert(wideWrite.createBatchWriterFactory(writeInfo) == RedisDataWriterFactory(wideSchema, options))

    val fieldSchema = new StructType().add("key", StringType).add("field", StringType)
    val fieldWrite = new RedisBatchWrite(fieldSchema, options, truncateRequested = false)
    val error = intercept[IllegalArgumentException](fieldWrite.createBatchWriterFactory(writeInfo))
    assert(error.getMessage == "Redis hash write is missing required columns: value")
  }

  private def optionsFor(redisType: String): RedisOptions = {
    RedisOptions.from(new CaseInsensitiveStringMap(util.Collections.singletonMap("type", redisType)))
  }
}
