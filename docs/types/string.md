# Redis String

Use `type='string'` to read and write Redis string values.

## Layout

| Column | Spark type | Description |
| --- | --- | --- |
| `key` | `STRING` | Redis key |
| `value` | `STRING` | Redis string value |

The write command is `SET key value`. If `ttl` is greater than `0`, writes use `SETEX key ttl value`.

## SQL Read

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_config
USING redis
OPTIONS (
  type='string',
  keys.pattern='config:*'
);

SELECT key, value FROM redis_config;
```

## SQL Write

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_config_out (
  key STRING,
  value STRING
)
USING redis
OPTIONS (
  type='string',
  ttl='3600'
);

INSERT INTO redis_config_out
SELECT key, value FROM source_config;
```

## DataFrame Read

```scala
val config = spark.read
  .format("redis")
  .option("type", "string")
  .option("keys.pattern", "config:*")
  .load()
```

## DataFrame Write

```scala
config.write
  .format("redis")
  .option("type", "string")
  .option("ttl", "3600")
  .mode("append")
  .save()
```

## Type-Specific Options

This section lists options most relevant to this Redis type. See the README Core Options section for the full option list.

| Option | Description |
| --- | --- |
| `key.prefix` | Prefix stripped from returned Spark keys during reads and prepended to Redis keys during writes. |
| `value.column` | Column used as the Redis string value. Default: `value`. |
| `ttl` | Expiration in seconds for written keys. Default: `0`, meaning no expiration. |

