# Redis Hash

Use `type='hash'` to read and write Redis hashes.

## Layouts

Hash supports two table layouts.

Field/value layout:

| Column | Spark type | Description |
| --- | --- | --- |
| `key` | `STRING` | Redis key |
| `field` | `STRING` | Redis hash field |
| `value` | `STRING` | Redis hash value |

Wide layout:

| Column | Spark type | Description |
| --- | --- | --- |
| `key` | `STRING` | Redis key |
| any non-key column | any Spark type supported by the connector codec | Redis hash field with the same name |

Without a custom schema, hashes are read as field/value rows. With a custom schema such as `key STRING, name STRING, age INT`, each Redis hash key becomes one Spark row and non-key columns are fetched with `HMGET`.

## SQL Read: Field/Value

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_hash_fields
USING redis
OPTIONS (
  type='hash',
  keys.pattern='user:*',
  key.prefix='user:'
);

SELECT key, field, value FROM redis_hash_fields;
```

## SQL Read: Wide Schema

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

## SQL Write

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

## DataFrame Read

```scala
val users = spark.read
  .format("redis")
  .option("type", "hash")
  .option("keys.pattern", "user:*")
  .schema("key STRING, name STRING, age INT")
  .load()
```

## DataFrame Write

```scala
users.write
  .format("redis")
  .option("type", "hash")
  .option("key.column", "key")
  .mode("append")
  .save()
```

## Type-Specific Options

This section lists options most relevant to this Redis type. See the README Core Options section for the full option list.

| Option | Description |
| --- | --- |
| `key.prefix` | Prefix stripped from returned Spark keys during reads and prepended to Redis keys during writes. |
| `field.column` | Column used as the hash field in field/value layout. Default: `field`. |
| `value.column` | Column used as the hash value in field/value layout. Default: `value`. |
| `ttl` | Expiration in seconds for written keys. Default: `0`, meaning no expiration. |

