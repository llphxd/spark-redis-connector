package io.github.sparkredis.connector

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.net.Socket
import java.nio.charset.StandardCharsets.UTF_8
import java.util

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisDataException

class RedisDataWriterSuite extends AnyFunSuite {
  test("write fails when a full pipeline contains a Redis error") {
    val socket = new RedisTestSocket("+OK\r\n-NOPERM command denied\r\n")
    withWriter(pipelineSize = 2, socket) { writer =>
      writer.write(stringRow("allowed"))
      val error = intercept[JedisDataException](writer.write(stringRow("denied")))
      assert(error.getMessage.startsWith("NOPERM"))
    }
  }

  test("commit fails when the remaining pipeline contains a Redis error") {
    val socket = new RedisTestSocket("-WRONGTYPE incompatible Redis type\r\n")
    withWriter(pipelineSize = 2, socket) { writer =>
      writer.write(stringRow("key"))
      val error = intercept[JedisDataException](writer.commit())
      assert(error.getMessage.startsWith("WRONGTYPE"))
    }
  }

  private val stringOptions = RedisOptions.from(
    new CaseInsensitiveStringMap(util.Collections.singletonMap("type", "string")))
  private val stringSchema = RedisSchemas.defaultSchema(stringOptions)

  private def stringRow(key: String) = RedisRowCodec.row(Seq(key, "value"), stringSchema)

  private def withWriter(pipelineSize: Int, socket: RedisTestSocket)(test: RedisDataWriter => Unit): Unit = {
    val writer = new RedisDataWriter(
      stringSchema, stringOptions.copy(writePipelineSize = pipelineSize), new Jedis(() => socket))
    try test(writer)
    finally {
      writer.abort()
      writer.close()
    }
  }
}

private class RedisTestSocket(replies: String) extends Socket {
  private val input = new ByteArrayInputStream(replies.getBytes(UTF_8))
  private val output = new ByteArrayOutputStream()
  private var closed = false

  override def isBound: Boolean = true
  override def isConnected: Boolean = true
  override def isClosed: Boolean = closed
  override def getSoTimeout: Int = 0
  override def getInputStream: ByteArrayInputStream = input
  override def getOutputStream: ByteArrayOutputStream = output
  override def close(): Unit = { closed = true }
}
