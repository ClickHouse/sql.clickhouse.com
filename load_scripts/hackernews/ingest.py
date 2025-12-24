#!/usr/bin/env python3
"""
Usage:
    ./ingest.py              # incremental: from max(id) in CH -> HN maxitem
    ./ingest.py 38718864     # start from specific ID -> HN maxitem
    ./ingest.py --dry-run    # estimate items to download without downloading
"""

import asyncio
import json
import logging
import os
import sys
from typing import List, Optional

import aiohttp
from clickhouse_driver import Client

# Configuration
HN_BASE_URL = "https://hacker-news.firebaseio.com/v0"
MAX_CONNECTIONS = int(os.getenv("WORKERS", "500"))
BATCH_SIZE = int(os.getenv("BATCH_LINES", "500"))
BLOCK_SIZE = int(os.getenv("BLOCK_SIZE", "10000"))

# ClickHouse connection (native protocol, not HTTP)
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))  # Native protocol port, not 8123
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
CH_SECURE = os.getenv("CLICKHOUSE_SECURE", False)
TABLE_NAME = os.getenv("TABLE_NAME", "hackernews")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def fetch_item(session: aiohttp.ClientSession, item_id: int) -> Optional[dict]:
    """Fetch a single HN item by ID."""
    url = f"{HN_BASE_URL}/item/{item_id}.json"
    try:
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                return await response.json()
            return None
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.debug(f"Failed to fetch item {item_id}: {e}")
        return None


async def fetch_maxitem(session: aiohttp.ClientSession) -> int:
    """Fetch the current max item ID from HN."""
    url = f"{HN_BASE_URL}/maxitem.json"
    async with session.get(url) as response:
        return await response.json()


async def download_batch(session: aiohttp.ClientSession, start_id: int, end_id: int) -> List[dict]:
    """Download a batch of items in parallel."""
    tasks = [fetch_item(session, item_id) for item_id in range(start_id, end_id + 1)]
    results = await asyncio.gather(*tasks)
    # Filter out None values and non-valid items
    return [item for item in results if item and isinstance(item, dict)]


def transform_item(item: dict) -> tuple:
    """Transform HN item to ClickHouse row format."""
    return (
        item.get("id", 0),
        1 if item.get("deleted", False) else 0,
        item.get("type", "story"),
        item.get("by", ""),
        item.get("time", 0),
        item.get("text", ""),
        1 if item.get("dead", False) else 0,
        item.get("parent", 0),
        item.get("poll", 0),
        item.get("kids", []),
        item.get("url", ""),
        item.get("score", 0),
        item.get("title", ""),
        item.get("parts", []),
        item.get("descendants", 0)
    )


def insert_to_clickhouse(client: Client, items: List[dict]) -> int:
    """Insert items into ClickHouse."""
    if not items:
        return 0
    
    # Filter valid item types
    valid_types = {"story", "comment", "poll", "pollopt", "job"}
    filtered_items = [item for item in items if item.get("type") in valid_types]
    
    if not filtered_items:
        return 0
    
    rows = [transform_item(item) for item in filtered_items]
    
    query = f"""
        INSERT INTO {TABLE_NAME} 
        (id, deleted, type, by, time, text, dead, parent, poll, kids, url, score, title, parts, descendants)
        VALUES
    """
    
    client.execute(query, rows)
    return len(rows)


async def main():
    # Check for dry-run mode
    dry_run = "--dry-run" in sys.argv
    
    # Determine starting ID
    client = Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
        secure=CH_SECURE
    )
    
    # Parse arguments (skip --dry-run flag)
    args = [arg for arg in sys.argv[1:] if arg != "--dry-run"]
    
    if len(args) > 0:
        start_id = int(args[0])
        logger.info(f"Starting from ID (parameter): {start_id}")
    else:
        result = client.execute(f"SELECT max(id) FROM {TABLE_NAME}")
        last_id = result[0][0] if result and result[0][0] else 0
        start_id = last_id + 1
        logger.info(f"Last downloaded ID: {last_id}")
        logger.info(f"Starting from ID: {start_id}")
    
    # Fetch maxitem
    connector = aiohttp.TCPConnector(limit=MAX_CONNECTIONS)
    async with aiohttp.ClientSession(connector=connector) as session:
        maxitem = await fetch_maxitem(session)
        logger.info(f"HN maxitem: {maxitem}")
        
        if start_id > maxitem:
            logger.info(f"Nothing to do: start ID {start_id} > maxitem {maxitem}")
            return
        
        # Calculate estimate
        items_to_download = maxitem - start_id + 1
        logger.info(f"Items to download: {items_to_download:,} (IDs {start_id} -> {maxitem})")
        
        if dry_run:
            logger.info("=" * 60)
            logger.info("DRY RUN MODE - No actual download will be performed")
            logger.info("=" * 60)
            logger.info(f"Estimated items to process: {items_to_download:,}")
            logger.info(f"Estimated blocks: {(items_to_download + BLOCK_SIZE - 1) // BLOCK_SIZE}")
            logger.info(f"Block size: {BLOCK_SIZE:,} items")
            logger.info(f"Concurrent workers: {MAX_CONNECTIONS}")
            logger.info(f"Estimated time: ~{items_to_download / 1000:.1f}-{items_to_download / 500:.1f} seconds")
            logger.info("=" * 60)
            return
        
        logger.info(f"Starting download...")
        
        total_inserted = 0
        current_id = start_id
        
        while current_id <= maxitem:
            batch_end = min(current_id + BLOCK_SIZE - 1, maxitem)
            logger.info(f"Processing block: {current_id}..{batch_end}")
            
            # Download batch
            items = await download_batch(session, current_id, batch_end)
            logger.info(f"Downloaded {len(items)} items")
            
            # Insert to ClickHouse
            inserted = insert_to_clickhouse(client, items)
            total_inserted += inserted
            logger.info(f"Inserted {inserted} items (total: {total_inserted})")
            
            current_id = batch_end + 1
        
        logger.info(f"Done! Total inserted: {total_inserted}")


if __name__ == "__main__":
    asyncio.run(main())
