# Silver layer

## bluesky_dedup table

```sql
CREATE TABLE bluesky.bluesky_dedup
(
	`data` JSON(	SKIP `commit.record.reply.root.record`, SKIP `commit.record.value.value`),
	`kind` LowCardinality(String),
	`scrape_ts` DateTime64(6),
	`bluesky_ts` DateTime64(6),
	`dedup_hash` String
)
ENGINE = ReplacingMergeTree
PARTITION BY toStartOfInterval(bluesky_ts, toIntervalMinute(20))
ORDER BY dedup_hash
TTL toStartOfMinute(bluesky_ts) + toIntervalMinute(1440) SETTINGS ttl_only_drop_parts=1
```

## Dead-letter queue table

```sql
CREATE TABLE bluesky.bluesky_dlq
(
	`data` JSON(	SKIP `commit.record.reply.root.record`, 	SKIP `commit.record.value.value`),
	`kind` LowCardinality(String),
	`scrape_ts` DateTime64(6),
	`bluesky_ts` DateTime64(6),
	`dedup_hash` String
)
ENGINE = MergeTree
ORDER BY (kind, scrape_ts)
```

# Transfer from Bronze to Silver

## Materialized view for bluesky_dedup table

```sql
CREATE MATERIALIZED VIEW bluesky.bluesky_dedup_mv TO bluesky.bluesky_dedup
(
	`data` JSON,
	`kind` LowCardinality(String),
	`scrape_ts` DateTime64(6),
	`bluesky_ts` DateTime64(6),
	`dedup_hash` String
)
AS SELECT
	data,
	kind,
	scrape_ts,
	bluesky_ts,
	dedup_hash
FROM bluesky.bluesky_raw
WHERE abs(timeDiff(scrape_ts, bluesky_ts)) < 1200
```


## Materialized view for dead-letter queue table

```sql
CREATE MATERIALIZED VIEW bluesky.bluesky_dlq_mv TO bluesky.bluesky_dlq
(
	`data` JSON,
	`kind` LowCardinality(String),
	`scrape_ts` DateTime64(6),
	`bluesky_ts` DateTime64(6),
	`dedup_hash` String
)
AS SELECT
	data,
	kind,
	scrape_ts,
	bluesky_ts,
	dedup_hash
FROM bluesky.bluesky_raw
WHERE abs(timeDiff(scrape_ts, bluesky_ts)) >= 1200
```



