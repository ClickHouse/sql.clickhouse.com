#!/bin/bash

# Step 1: Query ClickHouse for the max date in the table
MAX_DATE=$(clickhouse client --host ${CLICKHOUSE_HOST} --secure --password ${CLICKHOUSE_PASSWORD} --query "SELECT max(date) FROM  ${TABLE_NAME}" --format=TSV)

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
    | clickhouse client --host ${CLICKHOUSE_HOST} --secure --password ${CLICKHOUSE_PASSWORD} --query "INSERT INTO ${TABLE_NAME} FORMAT Native"

    if [ $? -ne 0 ]; then
        echo "Something went wrong with ingesting the file. Stopping."
        break
    fi
done

echo "Data ingestion complete."
