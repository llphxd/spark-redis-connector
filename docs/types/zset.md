# Redis Sorted Set

Use `type='zset'` to read and write Redis sorted sets.

## Layout

| Column | Spark type | Description |
| --- | --- | --- |
| `key` | `STRING` | Redis key |
| `member` | `STRING` | Sorted set member |
| `score` | `DOUBLE` | Sorted set score |

The read command is `ZRANGE key 0 -1 WITHSCORES`. The write command is `ZADD key score member`.

## SQL Read

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_scores
USING redis
OPTIONS (
  type='zset',
  keys.pattern='ranking:*'
);

SELECT key, member, score FROM redis_scores;
```

## SQL Write

```sql
CREATE OR REPLACE TEMPORARY VIEW redis_scores_out (
  key STRING,
  member STRING,
  score DOUBLE
)
USING redis
OPTIONS (
  type='zset'
);

INSERT INTO redis_scores_out
SELECT key, member, score FROM source_scores;
```

## DataFrame Read

```scala
val scores = spark.read
  .format("redis")
  .option("type", "zset")
  .option("keys.pattern", "ranking:*")
  .load()
```

## DataFrame Write

```scala
scores.write
  .format("redis")
  .option("type", "zset")
  .mode("append")
  .save()
```

## Type-Specific Options

This section lists options most relevant to this Redis type. See the README Core Options section for the full option list.

| Option | Description |
| --- | --- |
| `key.prefix` | Prefix stripped from returned Spark keys during reads and prepended to Redis keys during writes. |
| `member.column` | Column used as the sorted set member. Default: `member`. |
| `score.column` | Column used as the sorted set score. Default: `score`. |
| `ttl` | Expiration in seconds for written keys. Default: `0`, meaning no expiration. |

