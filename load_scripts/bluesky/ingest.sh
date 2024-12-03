#!/bin/bash

# List files in the bucket, filter for .csv.gz files, sort by name (assuming timestamps are comparable), and get the most recent file
MOST_RECENT_FILE=$(gsutil ls "${BUCKET_PATH}" | grep -E '[0-9]+\.csv\.gz$' | sort | tail -n 1)

if [[ -z "$MOST_RECENT_FILE" ]]; then
  echo "No matching files found."
  exit 1
fi

echo "Extracted timestamp: $TIMESTAMP"

# Extract the timestamp (assuming the filename format is <timestamp>.csv.gz)
cursor=$(basename "$MOST_RECENT_FILE" | sed -E 's/([0-9]+)\.csv\.gz/\1/')

# Function to process each chunk
process_chunk() {
    local chunk_file="$1"
    local last_value

    # Extract the last timestamp
    last_value=$(tail -1 "$chunk_file" | jq -r .time_us)

    # Validate the extracted value
    if [ -z "$last_value" ]; then
        echo "Error: last_value is empty. Skipping this chunk."
        rm "$chunk_file"
        return 1
    fi

    # Rename chunk file to use the timestamp
    mv "$chunk_file" "${last_value}.json"

    # Process the chunk if it has the required number of lines
    if [ $(wc -l < "${last_value}.json") -eq 1000000 ]; then
        if clickhouse-local --query "SELECT line as data FROM file('${last_value}.json', 'LineAsString') FORMAT CSVWithNames" > "${last_value}.csv"; then
            gzip "${last_value}.csv"
            if gsutil cp "${last_value}.csv.gz" ${BUCKET_PATH}; then
                rm "${last_value}.csv.gz" "${last_value}.json"
                return 0
            else
                echo "Error: gsutil upload failed for ${last_value}.csv.gz"
            fi
        else
            echo "Error: ClickHouse query failed for ${last_value}.json"
        fi
    else
        echo "Error: Invalid number of lines in ${last_value}.json. Removing file."
    fi
    # Clean up in case of errors
    rm "${last_value}.json"
    return 1
}

export -f process_chunk

websocat --exit-on-eof --ping-interval 1 --ping-timeout 5 \
        "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.*&cursor=$cursor" \
        | pv -l | split -l 1000000 --filter='chunk_file=$(mktemp); cat > "$chunk_file"; process_chunk "$chunk_file"'


