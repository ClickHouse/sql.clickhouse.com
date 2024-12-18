# Gold layer

## bluesky table

```sql
CREATE TABLE bluesky.bluesky
(
	`data` JSON(	SKIP `commit.record.reply.root.record`, 	SKIP `commit.record.value.value`),
	`kind` LowCardinality(String),
	`bluesky_ts` DateTime64(6),
	`_rmt_partition_id` LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toStartOfInterval(bluesky_ts, toIntervalMonth(1))
ORDER BY (kind, bluesky_ts)
```

# Transfer from Silver to Gold


## latest_partition table

```sql
CREATE TABLE bluesky.latest_partition
(
	`partition_id` SimpleAggregateFunction(max, UInt32)
)
ENGINE = AggregatingMergeTree
ORDER BY tuple()
```

## Materialized view for latest_partition table

```sql
CREATE MATERIALIZED VIEW bluesky.latest_partition_mv TO bluesky.latest_partition
(
	`partition_id` UInt32
)
AS SELECT max(CAST(_rmt_partition_id, 'UInt32')) AS partition_id
FROM bluesky.bluesky
```


## Refreshable materialized view for bluesky table

```sql
CREATE MATERIALIZED VIEW bluesky.blue_sky_dedupe_rmv
REFRESH EVERY 20 MINUTE APPEND TO bluesky.bluesky
(
	`data` JSON(	SKIP `commit.record.reply.root.record`, 	SKIP `commit.record.value.value`),
	`kind` LowCardinality(String),
	`bluesky_ts` DateTime64(6),
	`_rmt_partition_id` LowCardinality(String)
)
AS WITH
	(
          --step 1
    	  SELECT toUnixTimestamp(subtractMinutes(CAST(_partition_id, 'DateTime'), 40))
    	  FROM bluesky.bluesky_dedup
    	  GROUP BY _partition_id
    	  ORDER BY _partition_id DESC
    	  LIMIT 1
	) AS current_partition,
	(
          --step 2
    	  SELECT toUnixTimestamp(addMinutes(CAST(max(partition_id), 'DateTime'), 20))
    	  FROM bluesky.latest_partition
	) AS next_to_process
SELECT
	data,
	kind,
	bluesky_ts,
	_partition_id AS _rmt_partition_id
FROM bluesky.bluesky_dedup
FINAL
--step 3 & 4
WHERE _partition_id = CAST(if(next_to_process = 1200, current_partition, if(current_partition >= next_to_process, next_to_process, 0)), 'String')
SETTINGS do_not_merge_across_partitions_select_final = 1
```


# Dictionaries and materialized views for common queries

## Dictionary for mapping did-ids to handles

### Materialized view and target table for handle_per_user dictionary

This avoids loading the dictionary content from the potentially very large main gold table

```sql
CREATE TABLE bluesky.handle_per_user
(
    did String,
    handle String
)
ENGINE = ReplacingMergeTree
ORDER BY (did);
```

```sql
CREATE MATERIALIZED VIEW bluesky.handle_per_user_mv TO bluesky.handle_per_user
AS SELECT
    data.identity.did AS did,
    any(data.identity.handle) AS handle
FROM bluesky.bluesky
WHERE (kind = 'identity')
GROUP BY did;
```

optionally backfill with this query:
```sql
INSERT INTO bluesky.handle_per_user
SELECT
    data.identity.did AS did,
    any(data.identity.handle) AS handle
FROM bluesky.bluesky
WHERE (kind = 'identity')
GROUP BY did;
```

### handle_per_user dictionary

```sql
CREATE DICTIONARY bluesky.handle_per_user_dict
(
    did String,
    handle String
)
PRIMARY KEY (did)
SOURCE(CLICKHOUSE(QUERY $query$
    SELECT did, handle
    FROM bluesky.handle_per_user FINAL
$query$))
LIFETIME(MIN 300 MAX 360)
LAYOUT(complex_key_hashed());

SYSTEM RELOAD DICTIONARY bluesky.handle_per_user_dict;
```

## Dictionary for mapping did-ids to displayNames

### Materialized view and target table for displayName_per_user_dict dictionary

This avoids loading the dictionary content from the potentially very large main gold table

```sql
CREATE TABLE bluesky.displayName_per_user
(
    did String,
    displayName String
)
ENGINE = ReplacingMergeTree
ORDER BY (did);

```

```sql
CREATE MATERIALIZED VIEW bluesky.displayName_per_user_mv TO bluesky.displayName_per_user
AS SELECT
    data.did as did,
    argMax(data.commit.record.displayName, bluesky_ts) as displayName
FROM bluesky
WHERE (kind = 'commit') AND (data.commit.collection = 'app.bsky.actor.profile') AND notEmpty(data.commit.record.displayName) AND (data.commit.operation = 'update' OR data.commit.operation = 'create')
GROUP BY data.did;
```

optionally backfill with this query:
```sql
INSERT INTO bluesky.displayName_per_user
SELECT
    data.did as did,
    argMax(data.commit.record.displayName, bluesky_ts) as displayName
FROM bluesky
WHERE (kind = 'commit') AND (data.commit.collection = 'app.bsky.actor.profile') AND notEmpty(data.commit.record.displayName) AND (data.commit.operation = 'update' OR data.commit.operation = 'create')
GROUP BY data.did;
```

### displayName_per_user_dict dictionary

```sql
CREATE DICTIONARY bluesky.displayName_per_user_dict
(
    did String,
    displayName String
)
PRIMARY KEY (did)
SOURCE(CLICKHOUSE(QUERY $query$
    SELECT did, displayName
    FROM bluesky.displayName_per_user FINAL
$query$))
LIFETIME(MIN 300 MAX 360)
LAYOUT(complex_key_hashed());


SYSTEM RELOAD DICTIONARY bluesky.displayName_per_user_dict;
```

## Dictionary monitoring

```sql
SELECT
    name,
    status,
    element_count as count,
    formatReadableSize(bytes_allocated) AS memory_allocated,
    formatReadableTimeDelta(loading_duration) AS loading_duration,
    last_successful_update_time
FROM system.dictionaries
WHERE name in ['handle_per_user_dict', 'displayName_per_user_dict']
ORDER BY name;
```

## Materialized view for efficiently fetching texts for cids

The main gold table's sorting key is not optimal for cid lookups, therefore
we create a sub-copy of the table with an optimal soring key.


```sql
CREATE TABLE bluesky.cid_to_text
(
    cid String,
    did String,
    text String,
    about_clickhouse Boolean -- little efficient helper for querying posts about ClickHouse
)
ENGINE = MergeTree
ORDER BY (cid);
```

```sql

CREATE MATERIALIZED VIEW cid_to_text_mv TO bluesky.cid_to_text
AS SELECT
    data.commit.cid AS cid,
    data.did AS did,
    data.commit.record.text AS text,
    text::String ILIKE '% clickhouse %' AS about_clickhouse
FROM bluesky.bluesky
WHERE (kind = 'commit') AND (data.commit.collection = 'app.bsky.feed.post') AND notEmpty(text::String)
```

optionally backfill with this query:

```sql
INSERT INTO bluesky.cid_to_text
SELECT
    data.commit.cid AS cid,
    data.did AS did,
    data.commit.record.text AS text,
    text::String ILIKE '% clickhouse %' AS about_clickhouse
FROM bluesky.bluesky
WHERE (kind = 'commit') AND (data.commit.collection = 'app.bsky.feed.post') AND notEmpty(text::String)
```

## Materialized views for common queries

### When do people use BlueSky?

```sql
CREATE TABLE bluesky.events_per_hour_of_day
(
    event LowCardinality(String),
    hour_of_day UInt8,
    count SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (event, hour_of_day);
```

```sql
CREATE MATERIALIZED VIEW bluesky.events_per_hour_of_day_mv TO bluesky.events_per_hour_of_day
AS SELECT
    extract(data.commit.collection, '\\.([^.]+)$') AS event,
    toHour(bluesky_ts) as hour_of_day,
    count() AS count
FROM bluesky.bluesky
WHERE (kind = 'commit')
GROUP BY event, hour_of_day;
```

optionally backfill with this query:

```sql
-- optionally backfill
INSERT INTO bluesky.events_per_hour_of_day
SELECT
    extract(data.commit.collection, '\\.([^.]+)$') AS event,
    toHour(bluesky_ts) as hour_of_day,
    count() AS count
FROM bluesky.bluesky
WHERE (kind = 'commit')
GROUP BY event, hour_of_day;
```

main query
```sql
SELECT event, hour_of_day, sum(count) as count
FROM bluesky.events_per_hour_of_day
WHERE event in ['post', 'repost', 'like']
GROUP BY event, hour_of_day
ORDER BY hour_of_day;
```

### Top event types

```sql
CREATE TABLE bluesky.top_post_types
(
	`collection` LowCardinality(String),
	`posts` SimpleAggregateFunction(sum, UInt64),
	`users` AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
ORDER BY collection;
```

```sql
CREATE MATERIALIZED VIEW top_post_types_mv TO top_post_types
AS 
SELECT data.commit.collection AS collection, count() AS posts,
	uniqState(CAST(data.did, 'String')) AS users
FROM bluesky.bluesky
WHERE kind = 'commit'
GROUP BY ALL;
```



main query
```sql
SELECT collection,
       sum(posts) AS posts,
       uniqMerge(users) AS users
FROM bluesky.top_post_types
GROUP BY collection
ORDER BY posts DESC;
```

### Most liked posts

```sql
CREATE TABLE bluesky.likes_per_post
(
    cid String,
    likes SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (cid);
```

```sql
CREATE MATERIALIZED VIEW bluesky.likes_per_post_mv TO bluesky.likes_per_post
AS SELECT
    data.commit.record.subject.cid AS cid,
    count() AS likes
FROM bluesky.bluesky
WHERE data.commit.collection = 'app.bsky.feed.like'
GROUP BY cid;
```

optionally backfill with this query:

```sql
INSERT INTO bluesky.likes_per_post
SELECT
    data.commit.record.subject.cid AS cid,
    count() AS likes
FROM bluesky.bluesky
WHERE data.commit.collection = 'app.bsky.feed.like'
GROUP BY cid;
```

main query
```sql
WITH top_liked_cids AS
(
    SELECT
        cid,
        SUM(likes) AS likes
    FROM bluesky.likes_per_post
    GROUP BY cid
    ORDER BY likes DESC
    LIMIT 10
)
SELECT
    t1.likes,
    t2.text
FROM top_liked_cids AS t1
LEFT JOIN
(
    -- by exploiting its sorting key (cid),
    -- we manually pre-filter the right join partner
    SELECT *
    FROM bluesky.cid_to_text
    WHERE cid IN (SELECT cid FROM top_liked_cids)
) AS t2 ON t1.cid = t2.cid;
```

### Most liked posts about ClickHouse

```sql
CREATE TABLE bluesky.likes_per_post_about_clickhouse
(
    cid String,
    likes SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (cid);
```

```sql
CREATE MATERIALIZED VIEW bluesky.likes_per_post_about_clickhouse_mv TO bluesky.likes_per_post_about_clickhouse
AS SELECT
    data.commit.record.subject.cid AS cid,
    count() AS likes
FROM bluesky.bluesky AS bs INNER JOIN bluesky.cid_to_text ctt ON bs.data.commit.record.subject.cid = ctt.cid
WHERE bs.data.commit.collection = 'app.bsky.feed.like'
AND ctt.about_clickhouse
GROUP BY cid;
```

optionally backfill with this query:

```sql
INSERT INTO bluesky.likes_per_post_about_clickhouse
SELECT
    data.commit.record.subject.cid AS cid,
    count() AS likes
FROM bluesky.bluesky AS bs INNER JOIN bluesky.cid_to_text ctt ON bs.data.commit.record.subject.cid = ctt.cid
WHERE bs.data.commit.collection = 'app.bsky.feed.like'
AND ctt.about_clickhouse
GROUP BY cid;
```

main query
```sql
WITH top_liked_cids AS
(
    SELECT
        cid,
        SUM(likes) AS likes
    FROM bluesky.likes_per_post_about_clickhouse
    GROUP BY cid
    ORDER BY likes DESC
    LIMIT 3
)
SELECT
    t1.likes,
    t2.text
FROM top_liked_cids AS t1
LEFT JOIN
(
    -- by exploiting its sorting key (cid),
    -- we manually pre-filter the right join partner
    SELECT *
    FROM bluesky.cid_to_text
    WHERE cid IN (SELECT cid FROM top_liked_cids)
) AS t2 ON t1.cid = t2.cid;
```



### Most reposted posts

```sql
CREATE TABLE bluesky.reposts_per_post
(
    cid String,
    reposts SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (cid);
```

```sql

CREATE MATERIALIZED VIEW bluesky.reposts_per_post_mv TO bluesky.reposts_per_post
AS SELECT
    data.commit.record.subject.cid AS cid,
    count() AS reposts
FROM bluesky.bluesky
WHERE data.commit.collection = 'app.bsky.feed.repost'
GROUP BY cid;
```

optionally backfill with this query:

```sql
-- optionally backfill
INSERT INTO bluesky.reposts_per_post
SELECT
    data.commit.record.subject.cid AS cid,
    count() AS reposts
FROM bluesky.bluesky
WHERE data.commit.collection = 'app.bsky.feed.repost'
GROUP BY cid;
```

main query
```sql
WITH top_liked_cids AS
(
    SELECT
        cid,
        sum(reposts) AS reposts
    FROM bluesky.reposts_per_post
    GROUP BY cid
    ORDER BY reposts DESC
    LIMIT 10
)
SELECT
    t1.reposts,
    t2.text
FROM top_liked_cids AS t1
LEFT JOIN
(
    -- by exploiting its sorting key (cid),
    -- we manually pre-filter the right join partner
    SELECT *
    FROM bluesky.cid_to_text
    WHERE cid IN (SELECT cid FROM top_liked_cids)
) AS t2 ON t1.cid = t2.cid;
```





### Most used languages

```sql
CREATE TABLE bluesky.posts_per_language
(
    language LowCardinality(String),
    posts SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (language);
```

```sql
CREATE MATERIALIZED VIEW bluesky.posts_per_language_mv TO bluesky.posts_per_language
AS SELECT
    arrayJoin(CAST(data.commit.record.langs, 'Array(String)')) AS language,
    count() AS posts
FROM bluesky
WHERE data.commit.collection = 'app.bsky.feed.post'
GROUP BY language;
```

optionally backfill with this query:

```sql
INSERT INTO bluesky.posts_per_language
SELECT
    arrayJoin(CAST(data.commit.record.langs, 'Array(String)')) AS language,
    count() AS posts
FROM bluesky
WHERE data.commit.collection = 'app.bsky.feed.post'
GROUP BY language;
```

main query
```sql
SELECT
    language,
    sum(posts) as posts
FROM bluesky.posts_per_language
GROUP BY language
ORDER BY posts DESC
LIMIT 10;
```





### Most liked users

```sql
CREATE TABLE bluesky.likes_per_user
(
    did String,
    likes SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (did);
```

```sql
CREATE MATERIALIZED VIEW bluesky.likes_per_user_mv TO bluesky.likes_per_user
AS SELECT
    extract(data.commit.record.subject.uri, 'did:[^/]+') AS did,
    count() AS likes
FROM bluesky.bluesky
WHERE data.commit.collection = 'app.bsky.feed.like'
GROUP BY did;
```

optionally backfill with this query:

```sql
INSERT INTO bluesky.likes_per_user
SELECT
    extract(data.commit.record.subject.uri, 'did:[^/]+') AS did,
    count() AS likes
FROM bluesky.bluesky
WHERE data.commit.collection = 'app.bsky.feed.like'
GROUP BY did;
```

main query
```sql
SELECT
    dictGetOrDefault('bluesky.handle_per_user_dict', 'handle', did, did) as user,
    sum(likes) as likes
FROM bluesky.likes_per_user
GROUP BY did
ORDER BY likes DESC
LIMIT 10
```

main query - if we pretent to have a handle for each user
```sql
SELECT
    handle,
    sum(likes) AS likes
FROM bluesky.likes_per_user AS lpu
INNER JOIN bluesky.handle_per_user AS hpu ON lpu.did = hpu.did
GROUP BY ALL
ORDER BY likes DESC
LIMIT 10
```







### Most reposted users

```sql
CREATE TABLE bluesky.reposts_per_user
(
    did String,
    reposts SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (did);
```

```sql
CREATE MATERIALIZED VIEW bluesky.reposts_per_user_mv TO bluesky.reposts_per_user
AS SELECT
    extract(data.commit.record.subject.uri, 'did:[^/]+') AS did,
    count() AS reposts
FROM bluesky.bluesky
WHERE data.commit.collection = 'app.bsky.feed.repost'
GROUP BY did;
```

optionally backfill with this query:

```sql
INSERT INTO bluesky.reposts_per_user
SELECT
    extract(data.commit.record.subject.uri, 'did:[^/]+') AS did,
    count() AS reposts
FROM bluesky.bluesky
WHERE data.commit.collection = 'app.bsky.feed.repost'
GROUP BY did;
```

main query
```sql
SELECT
    dictGetOrDefault('bluesky.handle_per_user_dict', 'handle', did, did) as user,
    sum(reposts) as reposts
FROM bluesky.reposts_per_user
GROUP BY did
ORDER BY reposts DESC
LIMIT 10;
```

main query - if we pretent to have a handle for each user
```sql
SELECT
    handle,
    sum(reposts) AS reposts
FROM bluesky.reposts_per_user AS rpu
INNER JOIN bluesky.handle_per_user AS hpu ON rpu.did = hpu.did
GROUP BY ALL
ORDER BY reposts DESC
LIMIT 10
```





