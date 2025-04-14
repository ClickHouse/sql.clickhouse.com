#!/bin/bash

# This script is used to ingest daily downloads from RubyGems into ClickHouse.
ingest_daily_downloads() {
    echo "Ingesting daily downloads..."
    # Step 1: Query ClickHouse for the max date in the table
    MAX_DATE=$(clickhouse client --host ${CLICKHOUSE_HOST} --secure --password ${CLICKHOUSE_PASSWORD} --query "SELECT max(date) FROM ${DAILY_DOWNLOAD_TABLE_NAME}" --format=TSV)

    # Check if we got a valid date
    if [[ -z "$MAX_DATE" ]]; then
    echo "Error: Unable to retrieve max date."
    exit 1
    fi

    echo "Max date from ClickHouse: $MAX_DATE"

    # Step 2: Download and insert the file into ClickHouse
    echo "Downloading new data..."
    # Download files from github using this pattern https://raw.githubusercontent.com/segiddins/gem-daily-downloads/refs/heads/main/dates/YYYY/MM/YYYY-MM-DD.csv
    while true; do
        # Increment date by 1 day
        MAX_DATE=$(date -d "$MAX_DATE +1 day" +%Y-%m-%d)

        # Extract YYYY, MM from max_date
        YYYY=$(date -d "$MAX_DATE" +%Y)
        MM=$(date -d "$MAX_DATE" +%m)

        # Construct the URL
        url="https://raw.githubusercontent.com/segiddins/gem-daily-downloads/refs/heads/main/dates/$YYYY/$MM/$MAX_DATE.csv"

        # Check if the file exists on the server
        wget --spider "$url" 
        if [ $? -ne 0 ]; then
            echo "No more files available at $url. Stopping."
            break
        fi

        # Insert the file into ClickHouse
        echo "Trying to insert: $url"
        clickhouse local --query "SELECT replaceOne(_file, '.csv','')::Date as date, * FROM url('$url') FORMAT Native" \
        | clickhouse client --host ${CLICKHOUSE_HOST} --secure --password ${CLICKHOUSE_PASSWORD} --query "INSERT INTO ${DAILY_DOWNLOAD_TABLE_NAME} FORMAT Native"

        if [ $? -ne 0 ]; then
            echo "Something went wrong with ingesting the file. Stopping."
            break
        fi
    done

    echo "Daily downloads ingestion complete."
}

# Cross-platform date offset handling
get_date_parts() {
  if date -v -2d +%Y >/dev/null 2>&1; then
    # macOS/BSD date
    current_date=$(date +%Y-%m-%d)
    previous_date=$(date -v -2d +%Y-%m-%d)
  else
    # GNU/Linux date
    current_date=$(date +%Y-%m-%d)
    previous_date=$(date -d "2 days ago" +%Y-%m-%d)
  fi

  year=$(date -j -f "%Y-%m-%d" "$current_date" +%Y 2>/dev/null || date -d "$current_date" +%Y)
  month=$(date -j -f "%Y-%m-%d" "$current_date" +%m 2>/dev/null || date -d "$current_date" +%m)
  day=$(date -j -f "%Y-%m-%d" "$current_date" +%d 2>/dev/null || date -d "$current_date" +%d)

  prev_year=$(date -j -f "%Y-%m-%d" "$previous_date" +%Y 2>/dev/null || date -d "$previous_date" +%Y)
  prev_month=$(date -j -f "%Y-%m-%d" "$previous_date" +%m 2>/dev/null || date -d "$previous_date" +%m)
  prev_day=$(date -j -f "%Y-%m-%d" "$previous_date" +%d 2>/dev/null || date -d "$previous_date" +%d)

  date_path="$year/$month/$day"
  table_name="${year}_${month}_${day}"
  previous_table_name="${prev_year}_${prev_month}_${prev_day}"
}

ingest_downloads () {
    echo "Ingesting downloads..."
    # Step 1: Get the date parts
    get_date_parts

    # Step 2: Set the table name
    s3_path="${S3_PATH}/${date_path}/*.json.gz"
    
    echo "Creating s3 queue for ${date_path}"
    clickhouse client --secure --host "${CLICKHOUSE_HOST}" --password "${CLICKHOUSE_PASSWORD}" --query="
    CREATE TABLE IF NOT EXISTS rubygems.downloads_queue_${table_name} (
    \`timestamp\` DateTime,
    \`request_path\` String,
    \`request_query\` String,
    \`user_agent\` Tuple(
        agent_name String,
        agent_version String,
        bundler String,
        ci String,
        command String,
        jruby String,
        options String,
        platform Tuple(
        cpu String,
        os String,
        version String),
        ruby String,
        rubygems String,
        truffleruby String),
    \`tls_cipher\` String,
    \`time_elapsed\` Int64,
    \`client_continent\` String,
    \`client_country\` String,
    \`client_region\` String,
    \`client_city\` String,
    \`client_latitude\` String,
    \`client_longitude\` String,
    \`client_timezone\` String,
    \`client_connection\` String,
    \`request\` String,
    \`request_host\` String,
    \`request_bytes\` Int64,
    \`http2\` Bool,
    \`tls\` Bool,
    \`tls_version\` String,
    \`response_status\` Int64,
    \`response_text\` String,
    \`response_bytes\` Int64,
    \`response_cache\` String,
    \`cache_state\` String,
    \`cache_lastuse\` Float64,
    \`cache_hits\` Int64,
    \`server_region\` String,
    \`server_datacenter\` String,
    \`gem\` String,
    \`version\` String,
    \`platform\` String
    )
    ENGINE=S3Queue('${s3_path}', '${S3_KEY}', '${S3_SECRET}', 'JSONEachRow', 'gzip')
    SETTINGS mode = 'unordered', s3queue_polling_min_timeout_ms=1800000, s3queue_polling_max_timeout_ms=2400000,s3queue_tracked_files_limit=2000;"

    echo "Creating mv for ${date_path}"
    clickhouse client --secure --host "${CLICKHOUSE_HOST}" --password "${CLICKHOUSE_PASSWORD}" --query="CREATE MATERIALIZED VIEW IF NOT EXISTS rubygems.downloads_${table_name}_mv TO rubygems.downloads AS SELECT * FROM rubygems.downloads_queue_${table_name};"

    echo "Dropping ${previous_table_name} queue"
    clickhouse client --secure --host "${CLICKHOUSE_HOST}" --password "${CLICKHOUSE_PASSWORD}" --query="DROP TABLE IF EXISTS rubygems.downloads_queue_${previous_table_name};"

    echo "Dropping ${previous_table_name} mv"
    clickhouse client --secure --host "${CLICKHOUSE_HOST}" --password "${CLICKHOUSE_PASSWORD}" --query="DROP TABLE IF EXISTS rubygems.downloads_${previous_table_name}_mv;"
    echo "Download ingestion complete."
}

ingest_daily_downloads
ingest_downloads

