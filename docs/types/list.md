# Redis List

Use `type='list'` to read and write Redis lists.

## Read Layouts

Explode mode is the default:

| Column | Spark type | Description |
| --- | --- | --- |
| `key` | `STRING` | Redis key |
| `index` | `LONG` | Zero-based list index |
| `value` | `STRING` | List element |

Array mode returns one Spark row per Redis list key:

| Column | Spark type | Description |
| --- | --- | --- |
| `key` | `STRING` | Redis key |
| `values` | `ARRAY<STRING>` | All list elements |

## SQL Read: Explode

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_list_items
USING redis
OPTIONS (
  type='list',
  keys.pattern='queue:*'
);

SELECT key, index, value FROM redis_list_items;
```

## SQL Read: Array

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_lists
USING redis
OPTIONS (
  type='list',
  keys.pattern='queue:*',
  list.read.mode='array'
);

SELECT key, values FROM redis_lists;
```

## SQL Write

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_list_out (
  key STRING,
  value STRING
)
USING redis
OPTIONS (
  type='list',
  list.write.command='rpush'
);

INSERT INTO redis_list_out
SELECT key, value FROM source_events;
```

## DataFrame Read

```scala
val items = spark.read
  .format("redis")
  .option("type", "list")
  .option("keys.pattern", "queue:*")
  .load()
```

## DataFrame Write

```scala
items.select("key", "value")
  .write
  .format("redis")
  .option("type", "list")
  .option("list.write.command", "rpush")
  .mode("append")
  .save()
```

## Options

| Option | Description |
| --- | --- |
| `list.read.mode` | `explode` or `array`. Default: `explode`. |
| `list.write.command` | `rpush` or `lpush`. Default: `rpush`. |
| `value.column` | Column used as the list element on write. Default: `value`. |
| `ttl` | Expiration in seconds for written keys. Default: `0`, meaning no expiration. |

