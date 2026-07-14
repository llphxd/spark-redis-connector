package io.github.sparkredis.connector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType

class RedisScanBuilder(schema: StructType, options: RedisOptions)
    extends ScanBuilder
    with SupportsPushDownRequiredColumns {
  private var requiredSchema: StructType = schema

  // Whether hash keys are read in field/value "explode" mode. This MUST be decided from the full
  // table schema, not from the (possibly pruned) required schema. Otherwise a query that prunes the
  // field column (e.g. `SELECT key, value` or `SELECT count(*)`) would silently flip to wide-hash
  // mode and return wrong data.
  private val hashFieldMode: Boolean = schema.fieldNames.contains(options.fieldColumn)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def build(): Scan = new RedisScan(requiredSchema, options, hashFieldMode)
}

class RedisScan(schema: StructType, options: RedisOptions, hashFieldMode: Boolean) extends Scan with Batch {
  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val keys = RedisKeyScanner.loadKeys(options)
    if (keys.isEmpty) {
      Array.empty
    } else {
      keys.grouped(math.max(1, options.keysPerPartition)).map(group => RedisInputPartition(group)).toArray
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    RedisPartitionReaderFactory(schema, options, hashFieldMode)
  }
}

final case class RedisInputPartition(keys: Seq[String]) extends InputPartition

final case class RedisPartitionReaderFactory(schema: StructType, options: RedisOptions, hashFieldMode: Boolean)
    extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new RedisPartitionReader(schema, options, hashFieldMode, partition.asInstanceOf[RedisInputPartition].keys)
  }
}
