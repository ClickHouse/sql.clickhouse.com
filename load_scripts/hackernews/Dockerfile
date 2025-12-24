FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ingestion script
COPY ingest.py /app/ingest.py
RUN chmod +x /app/ingest.py

# Set default environment variables
ENV CLICKHOUSE_HOST=localhost \
    CLICKHOUSE_PORT=9000 \
    CLICKHOUSE_USER=default \
    CLICKHOUSE_PASSWORD="" \
    CLICKHOUSE_DATABASE=default \
    TABLE_NAME=hackernews \
    WORKERS=500 \
    BLOCK_SIZE=10000 \
    BATCH_LINES=500

ENTRYPOINT ["python", "/app/ingest.py"]
