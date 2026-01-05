#!/bin/bash

# Configurable variables
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-otel_v2_temp}"
CLICKHOUSE_HOST="sql-clickhouse.clickhouse.com"
HOURS="${HOURS:-29}"
MAX_RETRIES=5
SLEEP_TIME=1  

for ((i=1; i<=MAX_RETRIES; i++)); do

  START_TIME=$(clickhouse client \
    --secure --host="$CLICKHOUSE_HOST" \
    --user="$CLICKHOUSE_USER" \
    --password="$CLICKHOUSE_PASSWORD" \
    --query="SELECT subtractHours(now(), $HOURS) AS start FORMAT TabSeparated" 2>/dev/null)

  if [[ $? -eq 0 && -n "$START_TIME" ]]; then
    echo "Start time: $START_TIME"
    break
  else
    echo "Attempt $i failed. Retrying in $SLEEP_TIME seconds..."
    sleep $SLEEP_TIME
    SLEEP_TIME=$((SLEEP_TIME * 2))  # Exponential backoff
  fi

  # If this was the last attempt and still failed
  if [[ $i -eq $MAX_RETRIES ]]; then
    echo "ERROR: Failed to retrieve start time after $MAX_RETRIES attempts."
    exit 1
  fi
done

# Create database if it doesn't exist
clickhouse client \
  --secure --host="$CLICKHOUSE_HOST" \
  --user="$CLICKHOUSE_USER" \
  --password="$CLICKHOUSE_PASSWORD" \
  --query="CREATE DATABASE IF NOT EXISTS $CLICKHOUSE_DATABASE"

# Array of table names
tables=(
  "otel_logs"
  "otel_traces"
  "otel_metrics_gauge"
  "otel_metrics_histogram"
  "otel_metrics_sum"
  "otel_metrics_summary"
  "otel_metrics_exponential_histogram"
  "hyperdx_sessions"
)

# Create each table from otel_v2_source
for table in "${tables[@]}"; do
  # creating table in case it doesn't exist
  echo "Creating ${CLICKHOUSE_DATABASE}.${table}"
  clickhouse client \
    --secure --host="$CLICKHOUSE_HOST" \
    --user="$CLICKHOUSE_USER" \
    --password="$CLICKHOUSE_PASSWORD" \
    --query="CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}.${table} AS otel_v2_source.${table}"
  echo "Creating ${CLICKHOUSE_DATABASE}.${table}_temp"
  clickhouse client \
    --secure --host="$CLICKHOUSE_HOST" \
    --user="$CLICKHOUSE_USER" \
    --password="$CLICKHOUSE_PASSWORD" \
    --query="CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}.${table}_temp AS otel_v2_source.${table}"
done

echo "Database '$CLICKHOUSE_DATABASE' and tables created"

echo "Checking target tables are empty..."
# Truncate tables
for table in "${tables[@]}"; do
  echo "Truncating table ${CLICKHOUSE_DATABASE}.${table}_temp"
  clickhouse client \
    --secure --host="$CLICKHOUSE_HOST" \
    --user="$CLICKHOUSE_USER" \
    --password="$CLICKHOUSE_PASSWORD" \
    --query="TRUNCATE ${CLICKHOUSE_DATABASE}.${table}_temp"
done


# Insert adjusted data into otel_logs_temp
echo -n "Inserting adjusted logs into ${CLICKHOUSE_DATABASE}.otel_logs_temp..."
clickhouse client \
  --secure --host="$CLICKHOUSE_HOST" \
  --user="$CLICKHOUSE_USER" \
  --password="$CLICKHOUSE_PASSWORD" \
  --query="
    INSERT INTO ${CLICKHOUSE_DATABASE}.otel_logs_temp
    WITH (
        SELECT
            min(Timestamp),
            min(TimestampTime),
            '${START_TIME}'::DateTime AS start
        FROM otel_v2_source.otel_logs
    ) AS times
    SELECT
        (times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.1, Timestamp)) AS Timestamp,
        CAST((times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.2, TimestampTime)), 'DateTime') AS TimestampTime,
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
    FROM otel_v2_source.otel_logs"

echo "OK"

# Insert adjusted data into otel_traces_temp
echo -n "Inserting adjusted traces into ${CLICKHOUSE_DATABASE}.otel_traces_temp..."
clickhouse client \
  --secure --host="$CLICKHOUSE_HOST" \
  --user="$CLICKHOUSE_USER" \
  --password="$CLICKHOUSE_PASSWORD" \
  --query="
    INSERT INTO ${CLICKHOUSE_DATABASE}.otel_traces_temp WITH (
        SELECT
            min(Timestamp),
            '${START_TIME}'::DateTime AS start
        FROM otel_v2_source.otel_traces
    ) AS times
    SELECT
        (times.2) + toIntervalNanosecond(dateDiff('nanosecond', times.1, Timestamp)) AS Timestamp,
        TraceId,
        SpanId,
        ParentSpanId,
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
        arrayMap(t -> ((times.2) + toIntervalNanosecond(dateDiff('nanosecond', times.1, t))), Events.Timestamp) AS \`Events.Timestamp\`,
        Events.Name,
        Events.Attributes,
        Links.TraceId,
        Links.SpanId,
        Links.TraceState,
        Links.Attributes
    FROM otel_v2_source.otel_traces"

echo "OK"

# Insert adjusted data into otel_metrics_gauge
echo -n "Inserting adjusted metrics into ${CLICKHOUSE_DATABASE}.otel_metrics_gauge_temp..."
clickhouse client \
  --secure --host="$CLICKHOUSE_HOST" \
  --user="$CLICKHOUSE_USER" \
  --password="$CLICKHOUSE_PASSWORD" \
  --query="
    INSERT INTO ${CLICKHOUSE_DATABASE}.otel_metrics_gauge_temp
    WITH (
            SELECT
                min(StartTimeUnix),
                min(TimeUnix),
                '${START_TIME}'::DateTime AS start
            FROM otel_v2_source.otel_metrics_gauge WHERE StartTimeUnix > '1970-01-01'
        ) AS times
    SELECT ResourceAttributes, ResourceSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes, ScopeDroppedAttrCount, ScopeSchemaUrl, ServiceName, MetricName, MetricDescription, MetricUnit, Attributes,
    if (StartTimeUnix = '1970-01-01 00:00:00.000000000', StartTimeUnix, (times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.1, StartTimeUnix))) AS StartTimeUnix,
    if (TimeUnix ='1970-01-01 00:00:00.000000000', TimeUnix, (times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.2, TimeUnix))) AS TimeUnix,
    Value,
    Flags,
    Exemplars.FilteredAttributes,
    Exemplars.TimeUnix,
    Exemplars.Value,
    Exemplars.SpanId,
    Exemplars.TraceId
    FROM otel_v2_source.otel_metrics_gauge"

echo "OK"

# Insert adjusted data into otel_metrics_histogram
echo -n "Inserting adjusted metrics into ${CLICKHOUSE_DATABASE}.otel_metrics_histogram_temp..."
clickhouse client \
  --secure --host="$CLICKHOUSE_HOST" \
  --user="$CLICKHOUSE_USER" \
  --password="$CLICKHOUSE_PASSWORD" \
  --query="
    INSERT INTO ${CLICKHOUSE_DATABASE}.otel_metrics_histogram_temp WITH (
        SELECT
            min(StartTimeUnix),
            min(TimeUnix),
            '${START_TIME}'::DateTime AS start
        FROM otel_v2_source.otel_metrics_histogram
        WHERE StartTimeUnix > '1970-01-01'
    ) AS times
    SELECT
        ResourceAttributes,
        ResourceSchemaUrl,
        ScopeName,
        ScopeVersion,
        ScopeAttributes,
        ScopeDroppedAttrCount,
        ScopeSchemaUrl,
        ServiceName,
        MetricName,
        MetricDescription,
        MetricUnit,
        Attributes,
        if(StartTimeUnix = '1970-01-01 00:00:00.000000000', StartTimeUnix, (times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.1, StartTimeUnix))) AS StartTimeUnix,
        if(TimeUnix = '1970-01-01 00:00:00.000000000', TimeUnix, (times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.2, TimeUnix))) AS TimeUnix,
        Count,
        Sum,
        BucketCounts,
        ExplicitBounds,
        Exemplars.FilteredAttributes,
        arrayMap(t -> ((times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.2, t))), Exemplars.TimeUnix) AS \`Exemplars.TimeUnix\`,
        Exemplars.Value,
        Exemplars.SpanId,
        Exemplars.TraceId,
        Flags,
        Min,
        Max,
        AggregationTemporality
    FROM otel_v2_source.otel_metrics_histogram"

echo "OK"

# Insert adjusted data into otel_metrics_sum
echo -n "Inserting adjusted metrics into ${CLICKHOUSE_DATABASE}.otel_metrics_sum_temp..."
clickhouse client \
  --secure --host="$CLICKHOUSE_HOST" \
  --user="$CLICKHOUSE_USER" \
  --password="$CLICKHOUSE_PASSWORD" \
  --query="
    INSERT INTO ${CLICKHOUSE_DATABASE}.otel_metrics_sum_temp WITH (
        SELECT
            min(StartTimeUnix),
            min(TimeUnix),
            '${START_TIME}'::DateTime AS start
        FROM otel_v2_source.otel_metrics_sum
        WHERE StartTimeUnix > '1970-01-01'
    ) AS times
    SELECT
        ResourceAttributes,
        ResourceSchemaUrl,
        ScopeName,
        ScopeVersion,
        ScopeAttributes,
        ScopeDroppedAttrCount,
        ScopeSchemaUrl,
        ServiceName,
        MetricName,
        MetricDescription,
        MetricUnit,
        Attributes,
        if(StartTimeUnix = '1970-01-01 00:00:00.000000000', StartTimeUnix, (times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.1, StartTimeUnix))) AS StartTimeUnix,
        if(TimeUnix = '1970-01-01 00:00:00.000000000', TimeUnix, (times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.2, TimeUnix))) AS TimeUnix,
        Value,
        Flags,
        Exemplars.FilteredAttributes,
        arrayMap(t -> ((times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.2, t))), Exemplars.TimeUnix) AS \`Exemplars.TimeUnix\`,
        Exemplars.Value,
        Exemplars.SpanId,
        Exemplars.TraceId,
        AggregationTemporality,
        IsMonotonic
    FROM otel_v2_source.otel_metrics_sum"

echo "OK"

# Insert adjusted data into otel_metrics_summary
echo -n "Inserting adjusted metrics into ${CLICKHOUSE_DATABASE}.otel_metrics_summary_temp..."
clickhouse client \
  --secure --host="$CLICKHOUSE_HOST" \
  --user="$CLICKHOUSE_USER" \
  --password="$CLICKHOUSE_PASSWORD" \
  --query="
    INSERT INTO ${CLICKHOUSE_DATABASE}.otel_metrics_summary_temp WITH (
        SELECT
            min(StartTimeUnix),
            min(TimeUnix),
            '${START_TIME}'::DateTime AS start
        FROM otel_v2_source.otel_metrics_summary
        WHERE StartTimeUnix > '1970-01-01'
    ) AS times
    SELECT
        ResourceAttributes,
        ResourceSchemaUrl,
        ScopeName,
        ScopeVersion,
        ScopeAttributes,
        ScopeDroppedAttrCount,
        ScopeSchemaUrl,
        ServiceName,
        MetricName,
        MetricDescription,
        MetricUnit,
        Attributes,
        if(StartTimeUnix = '1970-01-01 00:00:00.000000000', StartTimeUnix, (times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.1, StartTimeUnix))) AS StartTimeUnix,
        if(TimeUnix = '1970-01-01 00:00:00.000000000', TimeUnix, (times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.2, TimeUnix))) AS TimeUnix,
        Count,
        Sum,
        ValueAtQuantiles.Quantile,
        ValueAtQuantiles.Value,
        Flags
    FROM otel_v2_source.otel_metrics_summary"

echo "OK"

# Insert adjusted data into otel_metrics_exponential_histogram
echo -n "Inserting adjusted metrics into ${CLICKHOUSE_DATABASE}.otel_metrics_exponential_histogram_temp..."
clickhouse client \
  --secure --host="$CLICKHOUSE_HOST" \
  --user="$CLICKHOUSE_USER" \
  --password="$CLICKHOUSE_PASSWORD" \
  --query="
    INSERT INTO ${CLICKHOUSE_DATABASE}.otel_metrics_exponential_histogram_temp WITH (
        SELECT
            min(StartTimeUnix),
            min(TimeUnix),
            '${START_TIME}'::DateTime AS start
        FROM otel_v2_source.otel_metrics_exponential_histogram
        WHERE StartTimeUnix > '1970-01-01'
    ) AS times
    SELECT
        ResourceAttributes,
        ResourceSchemaUrl,
        ScopeName,
        ScopeVersion,
        ScopeAttributes,
        ScopeDroppedAttrCount,
        ScopeSchemaUrl,
        ServiceName,
        MetricName,
        MetricDescription,
        MetricUnit,
        Attributes,
        if(StartTimeUnix = '1970-01-01 00:00:00.000000000', StartTimeUnix, (times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.1, StartTimeUnix))) AS StartTimeUnix,
        if(TimeUnix = '1970-01-01 00:00:00.000000000', TimeUnix, (times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.2, TimeUnix))) AS TimeUnix,
        Count,
        Sum,
        Scale,
        ZeroCount,
        PositiveOffset,
        PositiveBucketCounts,
        NegativeOffset,
        NegativeBucketCounts,
        Exemplars.FilteredAttributes,
        arrayMap(t -> ((times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.2, t))), Exemplars.TimeUnix) AS \`Exemplars.TimeUnix\`,
        Exemplars.Value,
        Exemplars.SpanId,
        Exemplars.TraceId,
        Flags,
        Min,
        Max,
        AggregationTemporality
    FROM otel_v2_source.otel_metrics_exponential_histogram"

echo "OK"

# Insert adjusted data into hyperdx_sessions
echo -n "Inserting adjusted metrics into ${CLICKHOUSE_DATABASE}.hyperdx_sessions_temp..."
clickhouse client \
  --secure --host="$CLICKHOUSE_HOST" \
  --user="$CLICKHOUSE_USER" \
  --password="$CLICKHOUSE_PASSWORD" \
  --query="
    INSERT INTO ${CLICKHOUSE_DATABASE}.hyperdx_sessions_temp
    WITH (
            SELECT
                min(Timestamp),
                min(TimestampTime),
                '${START_TIME}'::DateTime AS start
            FROM otel_v2_source.hyperdx_sessions 
        ) AS times
    SELECT
        (times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.1, Timestamp)) AS Timestamp,
        CAST((times.3) + toIntervalNanosecond(dateDiff('nanosecond', times.2, TimestampTime)), 'DateTime') AS TimestampTime,
        TraceId,
        SpanId,
        TraceFlags,
        SeverityText,
        SeverityNumber,
        ServiceName,
        replaceRegexpOne(Body, '\"timestamp\":' || JSONExtractUInt(Body, 'timestamp'), '\"timestamp\":' || toUnixTimestamp64Milli((times.3) + toIntervalMillisecond(dateDiff('millisecond', times.1, fromUnixTimestamp64Milli(JSONExtractUInt(Body, 'timestamp')))))) as Body,
        ResourceSchemaUrl,
        ResourceAttributes,
        ScopeSchemaUrl,
        ScopeName,
        ScopeVersion,
        ScopeAttributes,
        LogAttributes
    FROM otel_v2_source.hyperdx_sessions"

echo "OK"

# Refill MVs
# Insert aggregated data into otel_traces_1m
echo -n "Refilling MV ${CLICKHOUSE_DATABASE}.otel_traces_1m..."
clickhouse client \
  --secure --host="$CLICKHOUSE_HOST" \
  --user="$CLICKHOUSE_USER" \
  --password="$CLICKHOUSE_PASSWORD" \
  --query="
    INSERT INTO ${CLICKHOUSE_DATABASE}.otel_traces_1m_temp SELECT
        toStartOfMinute(Timestamp) AS Timestamp,
        ServiceName,
        StatusCode,
        count() AS count,
        avgState(Duration) AS avg__Duration,
        maxSimpleState(Duration) AS max__Duration
    FROM ${CLICKHOUSE_DATABASE}.otel_traces_temp
    GROUP BY
        Timestamp,
        ServiceName,
        StatusCode"

echo "OK"


# Insert aggregated data into otel_traces_1m_v2
echo -n "Refilling MV ${CLICKHOUSE_DATABASE}.otel_traces_1m_v2..."
clickhouse client \
  --secure --host="$CLICKHOUSE_HOST" \
  --user="$CLICKHOUSE_USER" \
  --password="$CLICKHOUSE_PASSWORD" \
  --query="
    INSERT INTO ${CLICKHOUSE_DATABASE}.otel_traces_1m_v2_temp SELECT
        toStartOfMinute(Timestamp) AS Timestamp,
        ServiceName,
        SpanName,
        StatusCode,
        count() AS count,
        avgState(Duration) AS avg__Duration,
        maxSimpleState(Duration) AS max__Duration,
        quantileTDigestState(0.5)(Duration) AS quantile__Duration
    FROM ${CLICKHOUSE_DATABASE}.otel_traces_temp
    GROUP BY
        Timestamp,
        ServiceName,
        StatusCode,
        SpanName"

echo "OK"


# exchange tables
for table in "${tables[@]}"; do
  echo "Exchanging tables ${CLICKHOUSE_DATABASE}.${table} AND ${CLICKHOUSE_DATABASE}.${table}_temp"
  clickhouse client \
    --secure --host="$CLICKHOUSE_HOST" \
    --user="$CLICKHOUSE_USER" \
    --password="$CLICKHOUSE_PASSWORD" \
    --query="EXCHANGE TABLES ${CLICKHOUSE_DATABASE}.${table} AND ${CLICKHOUSE_DATABASE}.${table}_temp"
done

echo "Exchanging tables ${CLICKHOUSE_DATABASE}.otel_traces_1m AND ${CLICKHOUSE_DATABASE}.otel_traces_1m_temp"
clickhouse client \
  --secure --host="$CLICKHOUSE_HOST" \
  --user="$CLICKHOUSE_USER" \
  --password="$CLICKHOUSE_PASSWORD" \
  --query="EXCHANGE TABLES ${CLICKHOUSE_DATABASE}.otel_traces_1m AND ${CLICKHOUSE_DATABASE}.otel_traces_1m_temp"

clickhouse client \
  --secure --host="$CLICKHOUSE_HOST" \
  --user="$CLICKHOUSE_USER" \
  --password="$CLICKHOUSE_PASSWORD" \
  --query="EXCHANGE TABLES ${CLICKHOUSE_DATABASE}.otel_traces_1m_v2 AND ${CLICKHOUSE_DATABASE}.otel_traces_1m_v2_temp"
echo "OK"


# truncate temp tables
for table in "${tables[@]}"; do
  echo "Truncating table ${CLICKHOUSE_DATABASE}.${table}_temp"
  clickhouse client \
    --secure --host="$CLICKHOUSE_HOST" \
    --user="$CLICKHOUSE_USER" \
    --password="$CLICKHOUSE_PASSWORD" \
    --query="TRUNCATE ${CLICKHOUSE_DATABASE}.${table}_temp"
done

# truncate temp tables
echo "Truncating table ${CLICKHOUSE_DATABASE}.otel_traces_1m_temp"
clickhouse client \
  --secure --host="$CLICKHOUSE_HOST" \
  --user="$CLICKHOUSE_USER" \
  --password="$CLICKHOUSE_PASSWORD" \
  --query="TRUNCATE ${CLICKHOUSE_DATABASE}.otel_traces_1m_temp"

echo "Truncating table ${CLICKHOUSE_DATABASE}.otel_traces_1m_v2_temp"
clickhouse client \
  --secure --host="$CLICKHOUSE_HOST" \
  --user="$CLICKHOUSE_USER" \
  --password="$CLICKHOUSE_PASSWORD" \
  --query="TRUNCATE ${CLICKHOUSE_DATABASE}.otel_traces_1m_v2_temp"
echo "Done"
