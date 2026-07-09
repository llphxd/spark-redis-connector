package io.github.sparkredis.catalyst

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

object RedisRowCodec {
  def row(values: Seq[Any], schema: StructType): InternalRow = {
    new GenericInternalRow(values.zip(schema.fields).map {
      case (null, _) => null
      case (value: UTF8String, _) => value
      case (value: Seq[_], field) => toCatalystArray(value, field.dataType)
      case (value: String, field) => toCatalyst(value, field.dataType)
      case (value, _) => value
    }.toArray)
  }

  def fromCatalyst(row: InternalRow, ordinal: Int, dataType: DataType): String = {
    if (row.isNullAt(ordinal)) {
      null
    } else {
      dataType match {
        case StringType => row.getUTF8String(ordinal).toString
        case IntegerType => row.getInt(ordinal).toString
        case LongType => row.getLong(ordinal).toString
        case DoubleType => row.getDouble(ordinal).toString
        case FloatType => row.getFloat(ordinal).toString
        case BooleanType => row.getBoolean(ordinal).toString
        case ShortType => row.getShort(ordinal).toString
        case ByteType => row.getByte(ordinal).toString
        case _ => row.get(ordinal, dataType).toString
      }
    }
  }

  def stringAt(row: InternalRow, schema: StructType, column: String): String = {
    val index = schema.fieldIndex(column)
    fromCatalyst(row, index, schema(index).dataType)
  }

  def optionalStringAt(row: InternalRow, schema: StructType, column: String): Option[String] = {
    schema.fields.zipWithIndex.find(_._1.name == column).flatMap { case (field, index) =>
      Option(fromCatalyst(row, index, field.dataType))
    }
  }

  private def toCatalyst(value: String, dataType: DataType): Any = {
    if (value == null) {
      null
    } else {
      dataType match {
        case StringType => UTF8String.fromString(value)
        case IntegerType => value.toInt
        case LongType => value.toLong
        case DoubleType => value.toDouble
        case FloatType => value.toFloat
        case BooleanType => value.toBoolean
        case ShortType => value.toShort
        case ByteType => value.toByte
        case _ => UTF8String.fromString(value)
      }
    }
  }

  private def toCatalystArray(value: Seq[_], dataType: DataType): Any = {
    dataType match {
      case ArrayType(StringType, _) =>
        new GenericArrayData(value.map {
          case null => null
          case str: String => UTF8String.fromString(str)
          case other => UTF8String.fromString(other.toString)
        }.toArray)
      case _ => value
    }
  }

  def hashRow(key: String, values: java.util.Map[String, String], schema: StructType, keyColumn: String): InternalRow = {
    val scalaValues = values.asScala
    row(schema.fields.map { field =>
      if (field.name == keyColumn) key else scalaValues.getOrElse(field.name, null)
    }, schema)
  }
}
