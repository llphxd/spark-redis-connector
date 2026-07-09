package io.github.sparkredis.connector

sealed trait RedisDataType extends Product with Serializable {
  def name: String
}

object RedisDataType {
  case object StringValue extends RedisDataType {
    override val name: String = "string"
  }

  case object Hash extends RedisDataType {
    override val name: String = "hash"
  }

  case object SetValue extends RedisDataType {
    override val name: String = "set"
  }

  case object ListValue extends RedisDataType {
    override val name: String = "list"
  }

  case object SortedSet extends RedisDataType {
    override val name: String = "zset"
  }

  val all: Seq[RedisDataType] = Seq(StringValue, Hash, SetValue, ListValue, SortedSet)

  def parse(value: String): RedisDataType = {
    all.find(_.name.equalsIgnoreCase(value)).getOrElse {
      throw new IllegalArgumentException(
        s"Unsupported Redis type '$value'. Supported values: ${all.map(_.name).mkString(", ")}")
    }
  }
}
