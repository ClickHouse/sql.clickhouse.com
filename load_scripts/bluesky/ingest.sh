#!/bin/bash 

# This script pushes data to a Google Cloud Storage bucket. ClickPipes is used to load the data into ClickHouse.

COUNT=0
OUTPUT_FILE="output.json"
WS_URL="wss://jetstream1.us-east.bsky.network"

has_more=true

while $has_more; do
    # List files in the bucket, filter for .csv.gz files, sort by name, and get the most recent file
    most_recent_file=$(gsutil ls "${BUCKET_PATH}" | grep -E '[0-9]+\.csv\.gz$' | sort | tail -n 1)

    if [[ -z "$most_recent_file" ]]; then
        echo "No matching files found."
        exit 1
    fi

    # Extract the timestamp 
    cursor=$(basename "$most_recent_file" | sed -E 's/([0-9]+)\.csv\.gz/\1/')

    echo "Extracted timestamp: $cursor"

    # Connect to WebSocket and process messages
    websocat -Un -B 196605 --max-messages-rev $MAX_MESSAGES "$WS_URL/subscribe?wantedCollections=app.*&cursor=$cursor" > "$OUTPUT_FILE"
    COUNT=$(wc -l < "$OUTPUT_FILE")
    echo "Received $COUNT messages"
    has_more=false
    if [ $COUNT -eq $MAX_MESSAGES ]; then
        echo "Processing $COUNT messages"
        # Extract the last timestamp
        last_value=$(tail -1 "$OUTPUT_FILE" | jq -r .time_us)

        # Validate the extracted value
        if [ -z "$last_value" ]; then
            echo "Error: last_value is empty. Skipping this chunk."
            rm "$OUTPUT_FILE"
        fi

        # Rename chunk file to use the timestamp
        mv "$OUTPUT_FILE" "${last_value}.json"

        if clickhouse local --query "SELECT line as data FROM file('${last_value}.json', 'LineAsString') FORMAT CSVWithNames" > "${last_value}.csv"; then
            gzip "${last_value}.csv"
            if gsutil cp "${last_value}.csv.gz" ${BUCKET_PATH}; then
                rm "${last_value}.csv.gz" "${last_value}.json"
                echo "Processed $COUNT messages"
                has_more=true
            else
                echo "Error: gsutil upload failed for ${last_value}.csv.gz"
            fi
        else
            echo "Error: ClickHouse query failed for ${last_value}.json"
        fi
    fi
done


