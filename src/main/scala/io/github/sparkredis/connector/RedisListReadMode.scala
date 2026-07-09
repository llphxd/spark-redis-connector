package io.github.sparkredis.connector

sealed trait RedisListReadMode extends Product with Serializable {
  def name: String
}

object RedisListReadMode {
  case object Explode extends RedisListReadMode {
    override val name: String = "explode"
  }

  case object Array extends RedisListReadMode {
    override val name: String = "array"
  }

  val all: Seq[RedisListReadMode] = Seq(Explode, Array)

  def parse(value: String): RedisListReadMode = {
    all.find(_.name.equalsIgnoreCase(value)).getOrElse {
      throw new IllegalArgumentException(
        s"Unsupported list read mode '$value'. Supported values: ${all.map(_.name).mkString(", ")}")
    }
  }
}
