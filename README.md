# spark-redis-connector

`spark-redis-connector` is a Spark SQL connector for Redis built around Spark DataSource V2.

The project intentionally focuses on Spark SQL and DataFrame APIs only. It does not carry the legacy RDD, DStream, Spark Streaming, or DataSource V1 APIs from older Spark Redis connectors.

## Features

- Spark SQL `USING redis` and DataFrame `.format("redis")` support.
- Batch read/write support for Redis string, hash, set, list, and sorted set.
- Redis native data types are exposed as table layouts instead of legacy RDD APIs.
- Redis key discovery uses `SCAN` for pattern-based reads.
- A shaded `all` jar is published for local Spark testing.

Redis Streams are intentionally left for a separate design because batch stream reads and structured streaming have different offset and commit semantics.

## Quick Start

Download the latest release jar from GitHub Releases:

```bash
curl -L -o spark-redis-connector_2.12-0.1.0-all.jar \
  https://github.com/llphxd/spark-redis-connector/releases/download/v0.1.0/spark-redis-connector_2.12-0.1.0-all.jar
```

Use the `all` jar when you want a single jar that already includes Jedis and other runtime dependencies.

### Spark SQL CLI

```bash
spark-sql \
  --jars spark-redis-connector_2.12-0.1.0-all.jar
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
  keys.pattern='user:*'
);

SELECT * FROM redis_users;
```

### Spark Shell

```bash
spark-shell \
  --jars spark-redis-connector_2.12-0.1.0-all.jar
```

```scala
val users = spark.read
  .format("redis")
  .option("type", "hash")
  .option("host", "localhost")
  .option("port", "6379")
  .option("keys.pattern", "user:*")
  .schema("key STRING, name STRING, age INT")
  .load()

users.show()
```

## SQL Examples

Read Redis strings:

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_config
USING redis
OPTIONS (
  type='string',
  keys.pattern='config:*'
);

SELECT key, value FROM redis_config;
```

Read Redis hashes as one row per hash field:

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_hash_fields
USING redis
OPTIONS (
  type='hash',
  keys.pattern='user:*'
);

SELECT key, field, value FROM redis_hash_fields;
```

Read Redis hashes with a custom schema:

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
  key.column='key'
);

SELECT key, name, age FROM redis_users;
```

Write Redis hashes:

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_user_out (
  id STRING,
  name STRING,
  age INT
)
USING redis
OPTIONS (
  type='hash',
  key.column='id',
  key.prefix='user:',
  ttl='3600'
);

INSERT INTO redis_user_out
SELECT id, name, age FROM source_users;
```

Hash writes support two layouts:

- Field/value layout: schema contains `key`, `field`, and `value`; each row is written as `HSET key field value`.
- Flattened layout: schema contains `key` plus arbitrary columns; each non-key column name becomes a Redis hash field.

Read sorted sets:

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_scores
USING redis
OPTIONS (
  type='zset',
  keys.pattern='ranking:*'
);

SELECT key, member, score FROM redis_scores;
```

Read Redis lists as arrays:

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_lists
USING redis
OPTIONS (
  type='list',
  keys.pattern='list:*',
  list.read.mode='array'
);

SELECT key, values FROM redis_lists;
```

## DataFrame Examples

```scala
val users = spark.read
  .format("redis")
  .option("type", "hash")
  .option("keys.pattern", "user:*")
  .schema("key STRING, name STRING, age INT")
  .load()
```

```scala
users.write
  .format("redis")
  .option("type", "hash")
  .option("key.column", "key")
  .mode("append")
  .save()
```

Overwrite is supported as truncate-style overwrite when `keys` or `keys.pattern` identifies the target keyspace:

```scala
users.write
  .format("redis")
  .option("type", "hash")
  .option("key.column", "key")
  .option("keys.pattern", "user:*")
  .mode("overwrite")
  .save()
```

## Schema Usage

Spark can infer a default schema for each Redis data type when you do not declare one in SQL or DataFrame code. Use the default schema when you want to inspect Redis data in its natural connector layout.

For `string`, `set`, and `zset`, the default schemas are usually enough because these types have a fixed row shape:

- `string`: `key STRING, value STRING`
- `set`: `key STRING, value STRING`
- `zset`: `key STRING, member STRING, score DOUBLE`

For `list`, the default read schema depends on `list.read.mode`:

- `explode`: `key STRING, index LONG, value STRING`, one Spark row per Redis list element.
- `array`: `key STRING, values ARRAY<STRING>`, one Spark row per Redis list key.

For `hash`, schema choice changes the table shape:

- Without a custom schema, hashes are read as `key STRING, field STRING, value STRING`. This is best for discovering unknown or inconsistent hash fields.
- With a custom schema such as `key STRING, name STRING, age INT`, each Redis hash key becomes one Spark row, and each non-key column name is read from the matching Redis hash field with `HMGET`.
- For writes, a schema containing `field` and `value` uses field/value layout (`HSET key field value`). A schema without `field` uses flattened layout, where every non-key column is written as a Redis hash field.

Use a custom schema when you want Spark types other than `STRING`, when you want a wide table for Redis hashes, or when your write path needs explicit column names such as `key`, `field`, `value`, `member`, or `score`.

## Core Options

| Option | Applies to | Required | Description | Default |
| --- | --- | --- | --- | --- |
| `type` | Read, Write | No | Redis data type: `string`, `hash`, `set`, `list`, `zset` | `hash` |
| `host` | Read, Write | No | Redis host. Example: `host='localhost'` or `host='redis.example.com'` | `localhost` |
| `port` | Read, Write | No | Redis port. Example: `port='6379'` | `6379` |
| `user` | Read, Write | No | Redis ACL user. Example: `user='default'` | unset |
| `password` / `auth` | Read, Write | No | Redis password. Example: `password='secret'` | unset |
| `database` / `dbNum` | Read, Write | No | Redis logical database. Example: `database='1'` | `0` |
| `timeout` | Read, Write | No | Redis connection timeout in milliseconds. Example: `timeout='5000'` | `2000` |
| `keys.pattern` | Read, Overwrite | Conditional | Redis key pattern used for `SCAN` during reads and truncate-style overwrite. Example: `keys.pattern='user:*'` | unset |
| `keys` | Read, Overwrite | Conditional | Comma-separated explicit keys used for reads and truncate-style overwrite. Example: `keys='user:1,user:2'` | unset |
| `key.column` | Read, Write | No | Spark column that contains or receives the Redis key. Example: `key.column='id'` writes row `id=1001` as Redis key `1001` unless `key.prefix` is set | `key` |
| `key.prefix` | Write | No | Prefix prepended to written Redis keys. Example: `key.prefix='user:'` and `id=1001` writes key `user:1001` | empty |
| `value.column` | Read, Write | No | Value column for string, set, list, and hash field/value mode. Example: `value.column='payload'` | `value` |
| `field.column` | Read, Write | No | Hash field column in field/value mode. Example: `field.column='field_name'` | `field` |
| `member.column` | Read, Write | No | Sorted set member column. Example: `member.column='item'` | `member` |
| `score.column` | Read, Write | No | Sorted set score column. Example: `score.column='rank_score'` | `score` |
| `list.read.mode` | Read | No | List read mode: `explode` returns `key,index,value`; `array` returns `key,values`. Example: `list.read.mode='array'` | `explode` |
| `list.write.command` | Write | No | List write command: `lpush` or `rpush`. Example: `list.write.command='lpush'` | `rpush` |
| `scan.count` | Read, Overwrite | No | Redis `SCAN COUNT` hint used during key discovery. Example: `scan.count='5000'` | `1000` |
| `keys.per.partition` | Read, Overwrite | No | Maximum discovered keys assigned to each Spark input partition or delete batch. Example: `keys.per.partition='2000'` | `1000` |
| `ttl` | Write | No | Expiration in seconds for written keys. `0` means no expiration. Example: `ttl='3600'` | `0` |

For reads, either `keys.pattern` or `keys` is required. For overwrite writes, `keys.pattern` or `keys` is required to identify the keyspace to delete before writing new rows.

## Type Layouts

| Redis type | Default columns | Write command |
| --- | --- | --- |
| `string` | `key STRING`, `value STRING` | `SET key value` |
| `list` read explode | `key STRING`, `index LONG`, `value STRING` | - |
| `list` read array | `key STRING`, `values ARRAY<STRING>` | - |
| `list` write | `key STRING`, `value STRING` | `RPUSH key value` by default, or `LPUSH key value` with `list.write.command='lpush'` |
| `set` | `key STRING`, `value STRING` | `SADD key value` |
| `hash` | `key STRING`, `field STRING`, `value STRING` | `HSET key field value` |
| `hash` flattened | `key STRING`, plus user columns | `HSET key col1 value1 ...` |
| `zset` | `key STRING`, `member STRING`, `score DOUBLE` | `ZADD key score member` |

The common Redis value columns are represented as Spark `STRING`. Sorted set score is represented as Spark `DOUBLE`.

## Build

You do not need to build from source if you use the release jar from GitHub Releases.

```bash
mvn test
```

Build the connector jars:

```bash
mvn package
```

This creates two jars:

- `target/spark-redis-connector_2.12-0.1.0.jar`: connector classes only.
- `target/spark-redis-connector_2.12-0.1.0-all.jar`: connector plus runtime dependencies such as Jedis.

For local Spark SQL testing, use the `all` jar:

```bash
spark-shell \
  --jars target/spark-redis-connector_2.12-0.1.0-all.jar
```

## Design Notes

This repository starts with a small standalone Redis implementation. Batch reads use Redis `SCAN` for pattern-based key discovery and split discovered keys into Spark input partitions with `keys.per.partition`. The scan path supports Spark column pruning, and explicit-schema hash reads use `HMGET` for the selected fields.

The next important engineering steps are:

- Add Redis Cluster slot-aware partitioning.
- Add projection and filter pushdown where Redis commands can honor them.
- Add Testcontainers integration tests for standalone, ACL, TLS, and cluster.
- Decide whether Redis Streams should be batch-only first or include structured streaming in a separate module.
