#!/bin/bash

# Function to shift the logs
shift_logs() {
    # Define the query command with the host and password
    CLIENT_QUERY_CMD="clickhouse client --host ${CLICKHOUSE_HOST} --secure --password ${CLICKHOUSE_PASSWORD} --query"
    # Execute the query and store the result
    MAX_TS=$($CLIENT_QUERY_CMD "SELECT greatest(MAX(Timestamp), now()) FROM otel.otel_logs")
    random_string=$(LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 16)

    FILENAME=$(clickhouse local --query "SELECT now()::UInt64")
    # Define the query command with the host and password

    $CLIENT_QUERY_CMD "CREATE TABLE IF NOT EXISTS otel.otel_logs_temp as otel.otel_logs"

    echo -n "generating logs..." 
    # Execute the main logs query
    $CLIENT_QUERY_CMD "INSERT INTO otel.otel_logs_temp
    WITH
        logs AS (
            SELECT
                Timestamp,
                toDate(Timestamp) AS TimestampDate,
                toDateTime(Timestamp) AS TimestampTime,
                TraceId,
                SpanId,
                TraceFlags,
                SeverityText,
                SeverityNumber,
                ServiceName,
                Body,
                ResourceSchemaUrl,
                ResourceAttributes,
                ScopeSchemaUrl,
                ScopeName,
                ScopeVersion,
                ScopeAttributes,
                LogAttributes
            FROM otel.otel_logs_parquet_investigation
        ),
        (
            SELECT min(Timestamp)
            FROM logs
        ) AS min,
        (
            SELECT toUInt64(dateDiff('microsecond', min, max(Timestamp)))
            FROM logs
        ) AS range
    SELECT
        addMicroseconds('${MAX_TS}', dateDiff('microsecond', min, Timestamp)) AS Timestamp,
        toDate(Timestamp) AS TimestampDate,
        toDateTime(Timestamp) AS TimestampTime,
        lower(hex(sipHash128(TraceId, '${random_string}'))) AS TraceId,
        lower(hex(sipHash64(SpanId, '${random_string}'))) SpanId,
        TraceFlags,
        SeverityText,
        SeverityNumber,
        ServiceName,
        Body,
        ResourceSchemaUrl,
        ResourceAttributes,
        ScopeSchemaUrl,
        ScopeName,
        ScopeVersion,
        ScopeAttributes,
        LogAttributes,
        true as HasTraces
    FROM logs SETTINGS schema_inference_make_columns_nullable = 0"
    echo "done" 
}

# Function to copy the logs
copy_logs() {
    # Define the query command with the host and password
    CLIENT_QUERY_CMD="clickhouse client --host ${CLICKHOUSE_HOST} --secure --password ${CLICKHOUSE_PASSWORD} --query"
    # Execute the query and store the result
    COUNT_LOG_TEMP=$($CLIENT_QUERY_CMD "SELECT count() FROM otel.otel_logs_temp")
    # Execute only if the count is correct
    if [ "$COUNT_LOG_TEMP" -eq 6436054 ]; then
      echo -n "copying logs..." 
      $CLIENT_QUERY_CMD "INSERT INTO otel.otel_logs SELECT * FROM otel.otel_logs_temp"
      echo "done" 
    else
      echo -n "Wrong count ($COUNT_LOG_TEMP) of logs , not copying..." 
    fi
}

# Function to shift the traces
copy_traces() {
    # Define the query command with the host and password
    CLIENT_QUERY_CMD="clickhouse client --host ${CLICKHOUSE_HOST} --secure --password ${CLICKHOUSE_PASSWORD} --query"
    # Execute the query and store the result
    COUNT_TRACES_TEMP=$($CLIENT_QUERY_CMD "SELECT count() FROM otel.otel_traces_temp")
    # Execute only if the count is correct
    if [ "$COUNT_TRACES_TEMP" -eq 222117940 ]; then
      echo -n "copying traces..." 
      $CLIENT_QUERY_CMD "INSERT INTO otel.otel_traces SELECT * FROM otel.otel_traces_temp"
      echo "done" 
    else
      echo -n "Wrong count ($COUNT_TRACES_TEMP) of traces , not copying..." 
    fi
}

# Function to clean table
clean_tmp() {
    # Define the query command with the host and password
    CLIENT_QUERY_CMD="clickhouse client --host ${CLICKHOUSE_HOST} --secure --password ${CLICKHOUSE_PASSWORD} --query"
    echo -n "clean up temp tables..." 
    $CLIENT_QUERY_CMD "TRUNCATE TABLE otel.otel_traces_temp"
    $CLIENT_QUERY_CMD "TRUNCATE TABLE otel.otel_logs_temp"
    echo "done" 
}

# Function to shift the traces
shift_traces() {
    # Define the query command with the host and password
    CLIENT_QUERY_CMD="clickhouse client --host ${CLICKHOUSE_HOST} --secure --password ${CLICKHOUSE_PASSWORD} --query"
    # Execute the query and store the result
    MAX_TS=$($CLIENT_QUERY_CMD "SELECT greatest(MAX(Timestamp), now()) FROM otel.otel_traces")
    $CLIENT_QUERY_CMD "CREATE TABLE IF NOT EXISTS otel.otel_traces_temp as otel.otel_traces"
    echo -n "generating traces..." 
    # Execute the main traces query
    $CLIENT_QUERY_CMD "INSERT INTO otel.otel_traces_temp
    WITH
        traces AS (
            SELECT * FROM otel.otel_traces_parquet_investigation
        ),
        (
            SELECT min(Timestamp)
            FROM traces
        ) AS min,
        (
            SELECT toUInt64(dateDiff('microsecond', min, max(Timestamp)))
            FROM traces
        ) AS range
    SELECT
        addMicroseconds('${MAX_TS}', dateDiff('microsecond', min, Timestamp)) AS Timestamp,
        lower(hex(sipHash128(TraceId, '${random_string}'))) AS TraceId,
        lower(hex(sipHash64(SpanId, '${random_string}'))) SpanId,
        lower(hex(sipHash64(ParentSpanId, '${random_string}'))) ParentSpanId,
        TraceState,
        SpanName,
        SpanKind,
        ServiceName,
        ResourceAttributes,
        ScopeName,
        ScopeVersion,
        SpanAttributes,
        Duration,
        StatusCode,
        StatusMessage,
        Events.Timestamp,
        Events.Name,
        Events.Attributes,
        Links.TraceId,
        Links.SpanId,
        Links.TraceState,
        Links.Attributes
    FROM traces"

    echo "done" 
}


start_time=$(date +%s)
echo -n "Script started at $start_time" 

# Execute the main query
shift_logs
shift_traces
copy_logs
copy_traces
clean_tmp

end_time=$(date +%s)
elapsed_time=$((end_time - start_time))

echo "Script execution time: $elapsed_time seconds" 

# Increment the counter
counter=$((counter + 1))
echo "Generated file" 
