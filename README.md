# spark-redis-connector

`spark-redis-connector` is a Spark SQL connector for Redis built around Spark DataSource V2.

The project intentionally focuses on Spark SQL and DataFrame APIs only. It does not carry the legacy RDD, DStream, Spark Streaming, or DataSource V1 APIs from older Spark Redis connectors.

## Goals

- Provide a modern `USING redis` SQL experience similar to Spark JDBC.
- Treat Redis native data types as first-class table mappings.
- Keep the public option model small and explicit.
- Build the connector around DataSource V2 read/write contracts.
- Make standalone Redis work first, then add cluster-aware partition planning.

## Current Scope

The initial connector skeleton supports batch read/write plans for:

- Redis String
- Redis Hash
- Redis Set
- Redis List
- Redis Sorted Set

Redis Streams are intentionally left for a separate design because batch stream reads and structured streaming have different offset and commit semantics.

## SQL Examples

Read Redis strings:

```sql
CREATE TEMPORARY VIEW redis_config
USING redis
OPTIONS (
  host 'localhost',
  port '6379',
  type 'string',
  keys.pattern 'config:*'
);

SELECT key, value FROM redis_config;
```

Read Redis hashes as one row per hash field:

```sql
CREATE TEMPORARY VIEW redis_hash_fields
USING redis
OPTIONS (
  host 'localhost',
  port '6379',
  type 'hash',
  keys.pattern 'user:*'
);

SELECT key, field, value FROM redis_hash_fields;
```

Read Redis hashes with an explicit schema:

```sql
CREATE TEMPORARY VIEW redis_users (
  key STRING,
  name STRING,
  age INT
)
USING redis
OPTIONS (
  host 'localhost',
  port '6379',
  type 'hash',
  keys.pattern 'user:*',
  key.column 'key'
);

SELECT key, name, age FROM redis_users;
```

Write Redis hashes:

```sql
CREATE TEMPORARY VIEW redis_user_out (
  id STRING,
  name STRING,
  age INT
)
USING redis
OPTIONS (
  host 'localhost',
  port '6379',
  type 'hash',
  key.column 'id',
  key.prefix 'user:',
  ttl '3600'
);

INSERT INTO redis_user_out
SELECT id, name, age FROM source_users;
```

Hash writes support two layouts:

- Field/value layout: schema contains `key`, `field`, and `value`; each row is written as `HSET key field value`.
- Flattened layout: schema contains `key` plus arbitrary columns; each non-key column name becomes a Redis hash field.

Read sorted sets:

```sql
CREATE TEMPORARY VIEW redis_scores
USING redis
OPTIONS (
  type 'zset',
  keys.pattern 'ranking:*'
);

SELECT key, member, score FROM redis_scores;
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
| `host` | Read, Write | No | Redis host | `localhost` |
| `port` | Read, Write | No | Redis port | `6379` |
| `user` | Read, Write | No | Redis ACL user | unset |
| `password` / `auth` | Read, Write | No | Redis password | unset |
| `database` / `dbNum` | Read, Write | No | Redis logical database | `0` |
| `timeout` | Read, Write | No | Redis connection timeout in milliseconds | `2000` |
| `keys.pattern` | Read, Overwrite | Conditional | Redis key pattern used for `SCAN` during reads and truncate-style overwrite | unset |
| `keys` | Read, Overwrite | Conditional | Comma-separated explicit keys used for reads and truncate-style overwrite | unset |
| `key.column` | Read, Write | No | Spark column that contains the Redis key or receives the Redis key | `key` |
| `key.prefix` | Write | No | Prefix prepended to written Redis keys | empty |
| `value.column` | Read, Write | No | Value column for string, set, list, and hash field/value mode | `value` |
| `field.column` | Read, Write | No | Hash field column in field/value mode | `field` |
| `member.column` | Read, Write | No | Sorted set member column | `member` |
| `score.column` | Read, Write | No | Sorted set score column | `score` |
| `list.read.mode` | Read | No | List read mode: `explode` or `array` | `explode` |
| `list.write.command` | Write | No | List write command: `lpush` or `rpush` | `rpush` |
| `scan.count` | Read, Overwrite | No | Redis `SCAN COUNT` hint used during key discovery | `1000` |
| `keys.per.partition` | Read, Overwrite | No | Maximum discovered keys assigned to each Spark input partition or delete batch | `1000` |
| `ttl` | Write | No | Expiration in seconds for written keys. `0` means no expiration | `0` |

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

Read Redis lists as arrays:

```sql
CREATE TEMPORARY VIEW redis_lists
USING redis
OPTIONS (
  type 'list',
  keys.pattern 'list:*',
  list.read.mode 'array'
);

SELECT key, values FROM redis_lists;
```

## Build

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
