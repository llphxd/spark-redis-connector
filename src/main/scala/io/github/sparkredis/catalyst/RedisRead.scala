package io.github.sparkredis.catalyst

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType

class RedisScanBuilder(schema: StructType, options: RedisOptions)
    extends ScanBuilder
    with SupportsPushDownRequiredColumns {
  private var requiredSchema: StructType = schema

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def build(): Scan = new RedisScan(requiredSchema, options)
}

class RedisScan(schema: StructType, options: RedisOptions) extends Scan with Batch {
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
    RedisPartitionReaderFactory(schema, options)
  }
}

final case class RedisInputPartition(keys: Seq[String]) extends InputPartition

final case class RedisPartitionReaderFactory(schema: StructType, options: RedisOptions)
    extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new RedisPartitionReader(schema, options, partition.asInstanceOf[RedisInputPartition].keys)
  }
}
