# Bronze layer

## bluesky_raw table

```sql
CREATE TABLE bluesky.bluesky_raw
(
	`data` JSON(	SKIP `commit.record.reply.root.record`, SKIP `commit.record.value.value`),
	`_file` LowCardinality(String),
	`kind` LowCardinality(String) MATERIALIZED getSubcolumn(data, 'kind'),
	`scrape_ts` DateTime64(6) MATERIALIZED fromUnixTimestamp64Micro(CAST(getSubcolumn(data, 'time_us'), 'UInt64')),
	`bluesky_ts` DateTime64(6) MATERIALIZED multiIf(getSubcolumn(data, 'kind') = 'commit', parseDateTime64BestEffortOrZero(CAST(getSubcolumn(data, 'commit.record.createdAt'), 'String')), getSubcolumn(data, 'kind') = 'identity', parseDateTime64BestEffortOrZero(CAST(getSubcolumn(data, 'identity.time'), 'String')), getSubcolumn(data, 'kind') = 'account', parseDateTime64BestEffortOrZero(CAST(getSubcolumn(data, 'account.time'), 'String')), toDateTime64(0, 6)),
	`dedup_hash` String MATERIALIZED cityHash64(arrayFilter(p -> ((p.1) != 'time_us'), JSONExtractKeysAndValues(CAST(data, 'String'), 'String')))
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (kind, bluesky_ts)
ORDER BY (kind, bluesky_ts, dedup_hash)
```

##  S3Queue table

```sql
CREATE TABLE bluesky.bluesky_queue
(
	`data` Nullable(String)
)
ENGINE = S3Queue('https://storage.googleapis.com/pme-internal/bluesky/*.gz', '<HMAC_KEY>', '<HMAC_SECRET>', 'CSVWithNames')
SETTINGS mode = 'ordered', s3queue_buckets = 30, s3queue_processing_threads_num = 10;
```


## Materialized view for S3Queue table
```sql
CREATE MATERIALIZED VIEW bluesky.bluesky_mv TO bluesky.bluesky_raw
(
	`data` Nullable(String)
)
AS SELECT
	data,
	_file
FROM bluesky.bluesky_queue
WHERE isValidJSON(data) = 1
```



