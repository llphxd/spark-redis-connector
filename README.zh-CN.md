# spark-redis-connector

<p align="center">
  <strong>Language / 语言</strong><br/>
  <a href="README.md"><strong>📖 English README</strong></a> · 中文
</p>

`spark-redis-connector` 是一个基于 Spark DataSource V2 的 Redis Spark SQL 连接器。

项目只聚焦 Spark SQL 和 DataFrame API，不包含旧版 Spark Redis connector 中的 RDD、DStream、Spark Streaming 或 DataSource V1 API。

## 功能特性

- 支持 Spark SQL `USING redis` 和 DataFrame `.format("redis")`。
- 支持 Redis string、hash、set、list、sorted set 的批读写。
- 以 Redis 原生数据类型映射 Spark 表结构，不引入旧 RDD API。
- 读路径使用 `SCAN` 做模式匹配的 key 发现。
- 发布包含 Jedis 等运行时依赖的 shaded `all` jar，便于本地 Spark 测试。

Redis Streams 会单独设计，因为批读取 Streams 和 Structured Streaming 的 offset、commit 语义不同。

## 兼容性

| 组件 | 支持版本 |
| --- | --- |
| Spark | `3.3.x`, `3.4.x`, `3.5.x` |
| Scala binary version | `2.12` |
| Java | `8+` |
| Redis client | Jedis `5.1.5` |
| Artifact | `spark-redis-connector_2.12` |

Spark `3.2.x` 及以下、Spark `4.x` 及以上未在当前 release 中声明支持。它们可能可以运行，但需要你基于自己的 Spark 和 Redis 运行环境自行验证。

## 快速开始

从 GitHub Releases 下载最新 release jar：

```bash
curl -L -o spark-redis-connector_2.12-0.1.1-all.jar \
  https://github.com/llphxd/spark-redis-connector/releases/download/v0.1.1/spark-redis-connector_2.12-0.1.1-all.jar
```

推荐使用 `all` jar，因为它已经包含 Jedis 和其他运行时依赖。

### Spark SQL CLI

```bash
spark-sql \
  --jars spark-redis-connector_2.12-0.1.1-all.jar
```

然后执行：

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_users (
  key STRING,
  name STRING,
  age INT
)
USING redis
OPTIONS (
  type='hash',
  host='localhost',
  port='6379',
  keys.pattern='user:*',
  key.prefix='user:'
);

SELECT * FROM redis_users;
```

如果 Redis 中的 key 是 `user:1001`，配置 `key.prefix='user:'` 后，Spark 查询结果中的 `key` 会是 `1001`。

### Spark Shell

```bash
spark-shell \
  --jars spark-redis-connector_2.12-0.1.1-all.jar
```

```scala
val users = spark.read
  .format("redis")
  .option("type", "hash")
  .option("host", "localhost")
  .option("port", "6379")
  .option("keys.pattern", "user:*")
  .option("key.prefix", "user:")
  .schema("key STRING, name STRING, age INT")
  .load()

users.show()
```

## 示例

使用 Spark SQL 读取 Redis hash：

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_users (
  key STRING,
  name STRING,
  age INT
)
USING redis
OPTIONS (
  type='hash',
  keys.pattern='user:*',
  key.prefix='user:'
);

SELECT key, name, age FROM redis_users;
```

使用 DataFrame API 读取相同数据：

```scala
val users = spark.read
  .format("redis")
  .option("type", "hash")
  .option("keys.pattern", "user:*")
  .option("key.prefix", "user:")
  .schema("key STRING, name STRING, age INT")
  .load()
```

写入 Redis hash：

```scala
users.write
  .format("redis")
  .option("type", "hash")
  .option("key.column", "key")
  .option("key.prefix", "user:")
  .mode("append")
  .save()
```

当 `keys` 或 `keys.pattern` 标识目标 keyspace 时，支持 truncate 风格的 overwrite。

## 类型文档

连接器支持 Redis 五种核心数据类型的批读写。每种类型的 schema、SQL 示例、DataFrame 示例和类型相关参数见详细文档。

| Redis 类型 | 默认读 schema | 写命令 | 文档 |
| --- | --- | --- | --- |
| `string` | `key STRING`, `value STRING` | `SET` / `SETEX` | [String](docs/types/string.md) |
| `hash` | `key STRING`, `field STRING`, `value STRING` | `HSET` | [Hash](docs/types/hash.md) |
| `list` | `key STRING`, `index LONG`, `value STRING` | `RPUSH` / `LPUSH` | [List](docs/types/list.md) |
| `set` | `key STRING`, `value STRING` | `SADD` | [Set](docs/types/set.md) |
| `zset` | `key STRING`, `member STRING`, `score DOUBLE` | `ZADD` | [Sorted Set](docs/types/zset.md) |

## Schema 使用说明

当 SQL 或 DataFrame 代码没有声明 schema 时，Spark 会使用每种 Redis 数据类型的默认 schema。需要 Spark 类型不只是 `STRING`、希望把 Redis hash 读成宽表、或者写入路径需要明确 `key`、`field`、`value`、`member`、`score` 等列名时，应使用自定义 schema。

## 核心参数

| 参数 | 适用于 | 是否必填 | 说明 | 默认值 |
| --- | --- | --- | --- | --- |
| `type` | 读、写 | 否 | Redis 数据类型：`string`、`hash`、`set`、`list`、`zset` | `hash` |
| `host` | 读、写 | 否 | Redis host | `localhost` |
| `port` | 读、写 | 否 | Redis port | `6379` |
| `user` | 读、写 | 否 | Redis ACL user | 未设置 |
| `password` | 读、写 | 否 | Redis password | 未设置 |
| `database` | 读、写 | 否 | Redis logical database | `0` |
| `timeout` | 读、写 | 否 | Redis connection 和 socket timeout，单位毫秒 | `2000` |
| `connection.pool.max.total` | 读、写 | 否 | 每个 executor 连接池的最大连接数 | `8` |
| `connection.pool.max.idle` | 读、写 | 否 | 每个 executor 连接池的最大空闲连接数 | `8` |
| `connection.pool.min.idle` | 读、写 | 否 | 每个 executor 连接池的最小空闲连接数 | `0` |
| `ssl.enabled` | 读、写 | 否 | 是否启用 SSL/TLS | `false` |
| `ssl.truststore.path` | 读、写 | 否 | SSL/TLS truststore 路径 | 未设置 |
| `ssl.truststore.password` | 读、写 | 否 | SSL/TLS truststore password | 未设置 |
| `ssl.truststore.type` | 读、写 | 否 | SSL/TLS truststore type | `JKS` |
| `keys.pattern` | 读、Overwrite | 条件必填 | 读和 overwrite 删除时用于 `SCAN` 的 Redis key pattern | 未设置 |
| `keys` | 读、Overwrite | 条件必填 | 显式 Redis key 列表，逗号分隔 | 未设置 |
| `key.column` | 读、写 | 否 | Spark 中表示 Redis key 的列名 | `key` |
| `key.prefix` | 读、写 | 否 | 读时从 Spark 返回的 key 中移除该前缀，写时给 Redis key 加上该前缀 | 空 |
| `value.column` | 读、写 | 否 | string、set、list 和 hash field/value 模式使用的 value 列 | `value` |
| `field.column` | 读、写 | 否 | hash field/value 模式使用的 field 列 | `field` |
| `member.column` | 读、写 | 否 | zset member 列 | `member` |
| `score.column` | 读、写 | 否 | zset score 列 | `score` |
| `list.read.mode` | 读 | 否 | list 读取模式：`explode` 返回 `key,index,value`；`array` 返回 `key,values` | `explode` |
| `list.write.command` | 写 | 否 | list 写入命令：`lpush` 或 `rpush` | `rpush` |
| `scan.count` | 读、Overwrite | 否 | Redis `SCAN COUNT` hint | `1000` |
| `keys.per.partition` | 读、Overwrite | 否 | 每个 Spark 输入分区或删除批次的最大 key 数 | `1000` |
| `write.pipeline.size` | 写 | 否 | Redis pipeline 累积多少条命令后 sync | `1000` |
| `ttl` | 写 | 否 | 写入 key 的过期时间，单位秒，`0` 表示不过期 | `0` |

读路径需要配置 `keys.pattern` 或 `keys`。Overwrite 写入需要配置 `keys.pattern` 或 `keys`，用于识别写入前要删除的 keyspace。

## 构建

如果使用 GitHub Releases 中的 release jar，不需要从源码构建。

```bash
mvn test
```

构建 connector jar：

```bash
mvn package
```

构建后会生成两个 jar：

- `target/spark-redis-connector_2.12-0.1.1.jar`：仅包含 connector classes。
- `target/spark-redis-connector_2.12-0.1.1-all.jar`：包含 connector 和 Jedis 等运行时依赖。

本地 Spark SQL 测试推荐使用 `all` jar：

```bash
spark-shell \
  --jars target/spark-redis-connector_2.12-0.1.1-all.jar
```

## 设计说明

当前实现从 standalone Redis 开始。批读使用 Redis `SCAN` 做基于 pattern 的 key 发现，并通过 `keys.per.partition` 把发现的 key 拆分到 Spark input partitions。读路径支持 Spark column pruning；显式 schema 的 hash 读取会对选中的字段使用 `HMGET`。

连接由 executor 端 Jedis pool 管理。写入使用 Redis pipeline 缓冲，并在累计 `write.pipeline.size` 条 Redis 命令后 sync。Spark 仍按 input partitions/tasks 拆分工作，该参数只控制每个 writer task 内部的 Redis 命令 flush。

后续重要工程方向：

- 增加 Redis Cluster slot-aware partitioning。
- 在 Redis 命令可以支持的范围内增加 projection 和 filter pushdown。
- 增加 standalone、ACL、TLS、cluster 的 Testcontainers 集成测试。
- 决定 Redis Streams 先支持 batch-only，还是放在单独模块中支持 structured streaming。

