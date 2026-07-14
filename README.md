# spark-redis-connector

<p align="center">
  <strong>Language / 语言</strong><br/>
  English · <a href="README.zh-CN.md"><strong>📖 中文文档</strong></a>
</p>

`spark-redis-connector` is a Spark SQL connector for Redis built around Spark DataSource V2.

The project intentionally focuses on Spark SQL and DataFrame APIs only. It does not carry the legacy RDD, DStream, Spark Streaming, or DataSource V1 APIs from older Spark Redis connectors.

## Features

- Spark SQL `USING redis` and DataFrame `.format("redis")` support.
- Batch read/write support for Redis string, hash, set, list, and sorted set.
- Redis native data types are exposed as table layouts instead of legacy RDD APIs.
- Redis key discovery uses `SCAN` for pattern-based reads.
- A shaded `all` jar is published for local Spark testing.

Redis Streams are intentionally left for a separate design because batch stream reads and structured streaming have different offset and commit semantics.

## Compatibility

| Component | Supported version |
| --- | --- |
| Spark | `3.3.x`, `3.4.x`, `3.5.x` |
| Scala binary version | `2.12` |
| Java | `8+` |
| Redis client | Jedis `5.1.5` |
| Artifact | `spark-redis-connector_2.12` |

Spark `3.2.x` and earlier, and Spark `4.x` and later, are not declared as supported by this release. They may work, but you should validate them with your own Spark and Redis runtime tests.

## Quick Start

Download a release jar from [GitHub Releases](https://github.com/llphxd/spark-redis-connector/releases). Each release provides two artifacts:

- `spark-redis-connector_2.12-<version>-all.jar` — connector plus shaded runtime dependencies (**recommended** for Spark `--jars`)
- `spark-redis-connector_2.12-<version>.jar` — connector classes only

Replace `<version>` with the release version (for example `0.1.3`) and `v<version>` with the git tag (for example `v0.1.3`):

```bash
curl -L -o spark-redis-connector_2.12-<version>-all.jar \
  https://github.com/llphxd/spark-redis-connector/releases/download/v<version>/spark-redis-connector_2.12-<version>-all.jar
```

Use the `all` jar when you want a single jar that already includes Jedis and other runtime dependencies.

### Spark SQL CLI

```bash
spark-sql \
  --jars spark-redis-connector_2.12-<version>-all.jar
```

Then run:

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

### Spark Shell

```bash
spark-shell \
  --jars spark-redis-connector_2.12-<version>-all.jar
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

## Examples

Read Redis hashes with Spark SQL:

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

Read the same data with the DataFrame API:

```scala
val users = spark.read
  .format("redis")
  .option("type", "hash")
  .option("keys.pattern", "user:*")
  .option("key.prefix", "user:")
  .schema("key STRING, name STRING, age INT")
  .load()
```

Write Redis hashes:

```scala
users.write
  .format("redis")
  .option("type", "hash")
  .option("key.column", "key")
  .mode("append")
  .save()
```

Overwrite is supported as truncate-style overwrite when `keys` or `keys.pattern` identifies the target keyspace.

## Type Documentation

The connector supports batch reads and writes for the five core Redis data types. See the detailed type guides for schemas, SQL examples, DataFrame examples, and type-specific options.

| Redis type | Default read schema | Write command | Guide |
| --- | --- | --- | --- |
| `string` | `key STRING`, `value STRING` | `SET` / `SETEX` | [String](docs/types/string.md) |
| `hash` | `key STRING`, `field STRING`, `value STRING` | `HSET` | [Hash](docs/types/hash.md) |
| `list` | `key STRING`, `index LONG`, `value STRING` | `RPUSH` / `LPUSH` | [List](docs/types/list.md) |
| `set` | `key STRING`, `value STRING` | `SADD` | [Set](docs/types/set.md) |
| `zset` | `key STRING`, `member STRING`, `score DOUBLE` | `ZADD` | [Sorted Set](docs/types/zset.md) |

## Schema Usage

Spark can infer a default schema for each Redis data type when you do not declare one in SQL or DataFrame code. Use a custom schema when you want Spark types other than `STRING`, when you want a wide table for Redis hashes, or when your write path needs explicit column names such as `key`, `field`, `value`, `member`, or `score`.

## Core Options

| Option | Applies to | Required | Description | Default |
| --- | --- | --- | --- | --- |
| `type` | Read, Write | No | Redis data type: `string`, `hash`, `set`, `list`, `zset` | `hash` |
| `host` | Read, Write | No | Redis host. Example: `host='localhost'` or `host='redis.example.com'` | `localhost` |
| `port` | Read, Write | No | Redis port. Example: `port='6379'` | `6379` |
| `user` | Read, Write | No | Redis ACL user. Example: `user='default'` | unset |
| `password` | Read, Write | No | Redis password. Example: `password='secret'` | unset |
| `database` | Read, Write | No | Redis logical database. Example: `database='1'` | `0` |
| `timeout` | Read, Write | No | Redis connection and socket timeout in milliseconds. Example: `timeout='3000'` | `2000` |
| `connection.pool.max.total` | Read, Write | No | Maximum Redis connections in each executor-side pool. Example: `connection.pool.max.total='16'` | `8` |
| `connection.pool.max.idle` | Read, Write | No | Maximum idle Redis connections in each executor-side pool. Example: `connection.pool.max.idle='8'` | `8` |
| `connection.pool.min.idle` | Read, Write | No | Minimum idle Redis connections in each executor-side pool. Example: `connection.pool.min.idle='1'` | `0` |
| `ssl.enabled` | Read, Write | No | Enable SSL/TLS. Example: `ssl.enabled='true'` | `false` |
| `ssl.truststore.path` | Read, Write | No | Truststore path used for SSL/TLS. Example: `ssl.truststore.path='/path/to/redis.jks'` | unset |
| `ssl.truststore.password` | Read, Write | No | Truststore password used for SSL/TLS. Example: `ssl.truststore.password='changeit'` | unset |
| `ssl.truststore.type` | Read, Write | No | Truststore type. Example: `ssl.truststore.type='PKCS12'` | `JKS` |
| `keys.pattern` | Read, Overwrite | Conditional | Redis key pattern used for `SCAN` during reads and truncate-style overwrite. Example: `keys.pattern='user:*'` | unset |
| `keys` | Read, Overwrite | Conditional | Comma-separated explicit keys used for reads and truncate-style overwrite. Example: `keys='user:1,user:2'` | unset |
| `key.column` | Read, Write | No | Spark column that contains or receives the Redis key. Example: `key.column='id'` writes row `id=1001` as Redis key `1001` unless `key.prefix` is set | `key` |
| `key.prefix` | Read, Write | No | Prefix stripped from returned Spark keys during reads and prepended to Redis keys during writes. Example: reading Redis key `user:1001` returns Spark key `1001`; writing Spark key `1001` writes Redis key `user:1001` | empty |
| `value.column` | Read, Write | No | Value column for string, set, list, and hash field/value mode. Example: `value.column='payload'` | `value` |
| `field.column` | Read, Write | No | Hash field column in field/value mode. Example: `field.column='field_name'` | `field` |
| `member.column` | Read, Write | No | Sorted set member column. Example: `member.column='item'` | `member` |
| `score.column` | Read, Write | No | Sorted set score column. Example: `score.column='rank_score'` | `score` |
| `list.read.mode` | Read | No | List read mode: `explode` returns `key,index,value`; `array` returns `key,values`. Example: `list.read.mode='array'` | `explode` |
| `list.write.command` | Write | No | List write command: `lpush` or `rpush`. Example: `list.write.command='lpush'` | `rpush` |
| `scan.count` | Read, Overwrite | No | Redis `SCAN COUNT` hint used during key discovery. Example: `scan.count='5000'` | `1000` |
| `keys.per.partition` | Read, Overwrite | No | Maximum discovered keys assigned to each Spark input partition or delete batch. Example: `keys.per.partition='2000'` | `1000` |
| `write.pipeline.size` | Write | No | Maximum queued Redis commands before syncing the Redis pipeline. Example: `write.pipeline.size='1000'` | `1000` |
| `ttl` | Write | No | Expiration in seconds for written keys. `0` means no expiration. Example: `ttl='3600'` | `0` |

For reads, either `keys.pattern` or `keys` is required. For overwrite writes, `keys.pattern` or `keys` is required to identify the keyspace to delete before writing new rows.

## Build

You do not need to build from source if you use the release jar from GitHub Releases.

```bash
mvn test
```

Build the connector jars:

```bash
mvn package
```

This creates two jars under `target/` (the version comes from `pom.xml`):

- `spark-redis-connector_2.12-<version>.jar`: connector classes only.
- `spark-redis-connector_2.12-<version>-all.jar`: connector plus runtime dependencies such as Jedis.

For local Spark SQL testing, use the `all` jar:

```bash
spark-shell \
  --jars target/spark-redis-connector_2.12-<version>-all.jar
```

## Design Notes

This repository starts with a small standalone Redis implementation. Batch reads use Redis `SCAN` for pattern-based key discovery and split discovered keys into Spark input partitions with `keys.per.partition`. The scan path supports Spark column pruning, and explicit-schema hash reads use `HMGET` for the selected fields.

Connections are backed by executor-side Jedis pools. Writes are buffered through Redis pipelines and are synced when `write.pipeline.size` Redis commands are queued. Spark still splits work by input partitions/tasks; this option controls Redis command flushing inside each writer task.

The next important engineering steps are:

- Add Redis Cluster slot-aware partitioning.
- Add projection and filter pushdown where Redis commands can honor them.
- Add Testcontainers integration tests for standalone, ACL, TLS, and cluster.
- Decide whether Redis Streams should be batch-only first or include structured streaming in a separate module.
