import sqlite3
import boto3
import json
import logging
import os
import sys
import time

# --- Configuration ---
# You can change these values or set them as environment variables
DB_PATH = os.getenv("DB_PATH", "podcastindex_feeds.db")
# This is the new queue for unprocessed feeds
FEEDS_SQS_QUEUE_URL = os.getenv("FEEDS_SQS_QUEUE_URL", "https://sqs.us-west-1.amazonaws.com/450282239172/FeedsToProcessQueue")
AWS_REGION = os.getenv("AWS_REGION", "us-west-1")

# --- Constants ---
SQS_BATCH_SIZE = 10 # AWS SQS limit is 10 messages per batch

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

# --- AWS Clients ---
sqs_client = boto3.client("sqs", region_name=AWS_REGION)

def send_batch_to_sqs(sqs_batch):
    """Sends a batch of messages to the SQS queue."""
    if not sqs_batch:
        return 0

    try:
        response = sqs_client.send_message_batch(
            QueueUrl=FEEDS_SQS_QUEUE_URL, Entries=sqs_batch
        )
        # Log only failed messages for cleaner output
        if "Failed" in response and response["Failed"]:
            logging.error(f"Failed to send {len(response['Failed'])} feed jobs.")
            for failed_msg in response["Failed"]:
                logging.error(f"  - Failed Msg ID: {failed_msg['Id']}, Reason: {failed_msg['Message']}")
        return len(response.get("Successful", []))
    except Exception as e:
        logging.error(f"Error sending batch to SQS: {e}")
        return 0

def main():
    """
    Main function to read all feeds from the DB and enqueue them for processing.
    """
    if not FEEDS_SQS_QUEUE_URL:
        logging.error("Missing required environment variable: FEEDS_SQS_QUEUE_URL")
        sys.exit(1)

    if not os.path.exists(DB_PATH):
        logging.error(f"Database file not found at: {DB_PATH}")
        sys.exit(1)

    logging.info("--- Starting Feeder Script ---")
    logging.info(f"Database: {DB_PATH}")
    logging.info(f"Target SQS Queue: {FEEDS_SQS_QUEUE_URL}")

    db_conn = sqlite3.connect(DB_PATH)
    cursor = db_conn.cursor()

    # Get total count for progress logging
    cursor.execute("SELECT COUNT(*) FROM podcasts")
    total_feeds = cursor.fetchone()[0]
    logging.info(f"Found {total_feeds:,} podcast feeds to enqueue.")

    # Initialize counters and the SQS message batch list
    feeds_enqueued = 0
    sqs_batch = []
    log_interval = 100000  # Log progress every 100,000 feeds
    last_log_count = 0

    # Randomize the order of feeds to avoid hitting any single server too quickly
    # This is crucial for preventing rate-limiting when using multiple workers.
    logging.info("Querying and randomizing all feeds from the database...")
    cursor.execute("SELECT id, url, language FROM podcasts ORDER BY RANDOM()")
    
    logging.info("Query complete. Starting to enqueue jobs...")
    for podcast_id, rss_url, language in cursor:
        # Create a message for the SQS batch
        message = {
            "Id": str(podcast_id), # Use podcast ID as the unique message ID in the batch
            "MessageBody": json.dumps({
                "podcast_id": podcast_id,
                "rss_url": rss_url,
                "language": language or "unknown" # Ensure language is never null
            })
        }
        sqs_batch.append(message)

        # When the batch is full, send it
        if len(sqs_batch) == SQS_BATCH_SIZE:
            sent_count = send_batch_to_sqs(sqs_batch)
            feeds_enqueued += sent_count
            sqs_batch.clear()  # Clear the batch

            # Log progress at intervals
            if feeds_enqueued - last_log_count >= log_interval:
                logging.info(f"Progress: {feeds_enqueued:,} / {total_feeds:,} feeds enqueued.")
                last_log_count = feeds_enqueued

    # Send any remaining messages in the final batch
    if sqs_batch:
        sent_count = send_batch_to_sqs(sqs_batch)
        feeds_enqueued += sent_count

    db_conn.close()
    logging.info("--- Feeder Script Finished ---")
    logging.info(f"Total feed jobs enqueued: {feeds_enqueued:,}")


if __name__ == "__main__":
    main() 