#!/bin/bash

# Step 1: Query ClickHouse for the max date in the table
MAX_DATE=$(clickhouse client --host ${CLICKHOUSE_HOST} --secure --password ${CLICKHOUSE_PASSWORD} --query "SELECT max(FlightDate) FROM ${TABLE_NAME}" --format=TSV)

# Check if we got a valid date
if [[ -z "$MAX_DATE" ]]; then
  echo "Error: Unable to retrieve max date."
  exit 1
fi

echo "Max date from ClickHouse: $MAX_DATE"

# Step 2:  Insert the file into ClickHouse from S3
echo "Downloading new data..."
# Download files from S3 using this pattern https://clickhouse-public-datasets.s3.amazonaws.com/ontime/original/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${YYYY}_${MM}.zip
while true; do
    # Increment date by 1 day
    MAX_DATE=$(date -d "$MAX_DATE +1 month" +%Y-%m-%d)

    # Extract YYYY, MM from max_date
    YYYY=$(date -d "$MAX_DATE" +%Y)
    MM=$(date -d "$MAX_DATE" +%-m)

    # Construct the URL
    url="https://clickhouse-public-datasets.s3.amazonaws.com/ontime/original/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${YYYY}_${MM}.zip"

    # Check if the file exists on the server
    wget --spider "$url" 
    if [ $? -ne 0 ]; then
        echo "No more files available at $url. Stopping."
        break
    fi
    # Insert the file into ClickHouse
    echo "Trying to insert: $url"
    wget -q -O tmp.zip $url
    find . -name '*.zip' -exec bash -c 'echo {}; unzip -cq {} "*.csv" | sed "s/\.00//g" | clickhouse client --host ${CLICKHOUSE_HOST} --secure --password ${CLICKHOUSE_PASSWORD} --input_format_csv_empty_as_default 1 --query="INSERT INTO ${TABLE_NAME} FORMAT CSVWithNames"' \;
    # find . -name 'tmp.zip' -exec bash -c "echo {}; unzip -cq {} '*.csv' | sed 's/\.00//g' | clickhouse client --host ${CLICKHOUSE_HOST} --secure --password ${CLICKHOUSE_PASSWORD} --input_format_csv_empty_as_default 1 --query='INSERT INTO ${TABLE_NAME} FORMAT CSVWithNames'"

    if [ $? -ne 0 ]; then
        echo "Something went wrong with ingesting the file. Stopping."
        break
    fi
    rm tmp.zip 
done

echo "Data ingestion complete."
