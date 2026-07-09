package io.github.sparkredis.connector

sealed trait RedisListWriteCommand extends Product with Serializable {
  def name: String
}

object RedisListWriteCommand {
  case object LPush extends RedisListWriteCommand {
    override val name: String = "lpush"
  }

  case object RPush extends RedisListWriteCommand {
    override val name: String = "rpush"
  }

  val all: Seq[RedisListWriteCommand] = Seq(LPush, RPush)

  def parse(value: String): RedisListWriteCommand = {
    all.find(_.name.equalsIgnoreCase(value)).getOrElse {
      throw new IllegalArgumentException(
        s"Unsupported list write command '$value'. Supported values: ${all.map(_.name).mkString(", ")}")
    }
  }
}
