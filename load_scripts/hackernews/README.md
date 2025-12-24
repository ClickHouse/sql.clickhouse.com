# Hacker News Ingestion Scripts

Fast incremental ingestion of Hacker News items into ClickHouse.

## Python Script (Recommended)

High-performance async HTTP implementation that can process 1000+ requests per second.

### Requirements

```bash
pip install -r requirements.txt
```

### Usage

```bash
# Incremental: from max(id) in ClickHouse -> HN maxitem
python ingest.py

# Start from specific ID
python ingest.py 46352456

# Dry run: estimate items to download without downloading
python ingest.py --dry-run

# Dry run from specific ID
python ingest.py --dry-run 46352456

# With custom configuration
WORKERS=1000 BLOCK_SIZE=20000 python ingest.py
```

### Environment Variables

- `CLICKHOUSE_HOST` - ClickHouse host (default: localhost)
- `CLICKHOUSE_PORT` - ClickHouse native port (default: 9000)
- `CLICKHOUSE_USER` - ClickHouse user (default: default)
- `CLICKHOUSE_PASSWORD` - ClickHouse password (default: empty)
- `CLICKHOUSE_DATABASE` - Database name (default: default)
- `TABLE_NAME` - Table name (default: hackernews)
- `WORKERS` - Concurrent HTTP connections (default: 500)
- `BLOCK_SIZE` - Items per block (default: 10000)
- `BATCH_LINES` - Insert batch size (default: 500)

## Docker

### Build

```bash
docker build -t hn-ingest .
```

### Run

```bash
# Incremental from max(id)
docker run --rm \
  -e CLICKHOUSE_HOST=your-clickhouse-host \
  -e CLICKHOUSE_PORT=9000 \
  hn-ingest

# Start from specific ID
docker run --rm \
  -e CLICKHOUSE_HOST=your-clickhouse-host \
  -e CLICKHOUSE_PORT=9000 \
  hn-ingest 46352456
```

## Performance

The Python async implementation is significantly faster than sequential approaches:

- **~1000+ requests/second** with default settings
- **~10,000 items in ~10-15 seconds**
- Configurable concurrency via `WORKERS` environment variable

## Table Schema

The script expects a ClickHouse table with the following schema:

```sql
CREATE TABLE hackernews (
    id UInt32,
    deleted UInt8,
    type String,
    by String,
    time UInt32,
    text String,
    dead UInt8,
    parent UInt32,
    poll UInt32,
    kids Array(UInt32),
    url String,
    score UInt32,
    title String,
    parts Array(UInt32),
    descendants UInt32
) ENGINE = MergeTree()
ORDER BY id;
```
