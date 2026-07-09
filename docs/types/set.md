# Redis Set

Use `type='set'` to read and write Redis sets.

## Layout

| Column | Spark type | Description |
| --- | --- | --- |
| `key` | `STRING` | Redis key |
| `value` | `STRING` | Set member |

The read command is `SMEMBERS`. The write command is `SADD key value`.

## SQL Read

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_tags
USING redis
OPTIONS (
  type='set',
  keys.pattern='tags:*'
);

SELECT key, value FROM redis_tags;
```

## SQL Write

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_tags_out (
  key STRING,
  value STRING
)
USING redis
OPTIONS (
  type='set',
  ttl='3600'
);

INSERT INTO redis_tags_out
SELECT key, value FROM source_tags;
```

## DataFrame Read

```scala
val tags = spark.read
  .format("redis")
  .option("type", "set")
  .option("keys.pattern", "tags:*")
  .load()
```

## DataFrame Write

```scala
tags.write
  .format("redis")
  .option("type", "set")
  .mode("append")
  .save()
```

## Type-Specific Options

This section lists options most relevant to this Redis type. See the README Core Options section for the full option list.

| Option | Description |
| --- | --- |
| `key.prefix` | Prefix stripped from returned Spark keys during reads and prepended to Redis keys during writes. |
| `value.column` | Column used as the set member. Default: `value`. |
| `ttl` | Expiration in seconds for written keys. Default: `0`, meaning no expiration. |

