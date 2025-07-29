import sqlite3
import feedparser
import boto3
import json
import logging
import os
import sys
import time

# --- Configuration ---
# You can change these values or set them as environment variables
DB_PATH = os.getenv("DB_PATH", "podcastindex.db")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "https://sqs.us-west-1.amazonaws.com/450282239172/PodcastIndexQueue") # Required: Set this in your environment
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME", "PodcastIndexJobs") # Required
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
dynamodb_client = boto3.client("dynamodb", region_name=AWS_REGION)

def send_batch_to_sqs(sqs_batch):
    """Sends a batch of messages to the SQS queue."""
    if not sqs_batch:
        return 0

    try:
        response = sqs_client.send_message_batch(
            QueueUrl=SQS_QUEUE_URL, Entries=sqs_batch
        )
        # Log successful and failed messages
        if "Successful" in response:
            logging.info(f"Successfully sent {len(response['Successful'])} messages.")
        if "Failed" in response and response["Failed"]:
            logging.error(f"Failed to send {len(response['Failed'])} messages.")
            for failed_msg in response["Failed"]:
                logging.error(f"  - Failed Msg ID: {failed_msg['Id']}, Reason: {failed_msg['Message']}")
        return len(response.get("Successful", []))
    except Exception as e:
        logging.error(f"Error sending batch to SQS: {e}")
        return 0

def main():
    """
    Main function to read the DB, parse feeds, and enqueue download jobs.
    """
    if not SQS_QUEUE_URL or not DYNAMODB_TABLE_NAME:
        logging.error("Missing required environment variables: SQS_QUEUE_URL and DYNAMODB_TABLE_NAME")
        sys.exit(1)

    if not os.path.exists(DB_PATH):
        logging.error(f"Database file not found at: {DB_PATH}")
        sys.exit(1)

    logging.info("--- Starting Producer Script ---")
    logging.info(f"Database: {DB_PATH}")
    logging.info(f"SQS Queue: {SQS_QUEUE_URL}")

    db_conn = sqlite3.connect(DB_PATH)
    cursor = db_conn.cursor()

    # Get total count for progress logging
    cursor.execute("SELECT COUNT(*) FROM podcasts")
    total_feeds = cursor.fetchone()[0]
    logging.info(f"Found {total_feeds:,} podcast feeds to process.")

    # Get total episodes from DB for stats
    cursor.execute("SELECT SUM(episodeCount) FROM podcasts")
    # Handle case where table is empty or column has NULLs
    total_episodes_in_db = cursor.fetchone()[0] or 0
    logging.info(f"DB reports a total of {total_episodes_in_db:,} episodes across all feeds.")

    # Initialize counters and the SQS message batch list
    feeds_processed = 0
    episodes_enqueued = 0
    total_episodes_found_in_feeds = 0
    sqs_batch = []
    
    cursor.execute("SELECT id, url FROM podcasts")
    for podcast_id, rss_url in cursor:
        feeds_processed += 1
        logging.info(f"Processing feed {feeds_processed}/{total_feeds}: {rss_url}")
        
        try:
            # feedparser handles fetching the URL content
            feed = feedparser.parse(rss_url)
            
            # Check for parsing errors or empty feeds
            if feed.bozo:
                logging.warning(f"Bozo feed (might be malformed): {rss_url} - {feed.bozo_exception}")
            
            if not feed.entries:
                logging.warning(f"No entries found in feed: {rss_url}")
                continue

            total_episodes_found_in_feeds += len(feed.entries)

            for entry in feed.entries:
                for enclosure in getattr(entry, 'enclosures', []):
                    # Ensure the enclosure is audio and has a valid URL
                    if 'audio' in enclosure.get('type', '') and enclosure.get('href'):
                        episode_url = enclosure.href
                        
                        # Create a message for the SQS batch
                        message = {
                            "Id": str(time.time_ns()), # A unique ID for the message in the batch
                            "MessageBody": json.dumps({
                                "episode_url": episode_url,
                                "podcast_rss_url": rss_url
                            })
                        }
                        sqs_batch.append(message)
                        
                        # When the batch is full, send it
                        if len(sqs_batch) == SQS_BATCH_SIZE:
                            sent_count = send_batch_to_sqs(sqs_batch)
                            episodes_enqueued += sent_count
                            sqs_batch.clear() # Clear the batch
                        break # Found an enclosure, process next episode entry
                        
        except Exception as e:
            logging.error(f"Failed to process feed {rss_url}: {e}")

    # Send any remaining messages in the final batch
    if sqs_batch:
        sent_count = send_batch_to_sqs(sqs_batch)
        episodes_enqueued += sent_count

    db_conn.close()
    logging.info("--- Producer Script Finished ---")
    logging.info(f"Total feeds processed: {feeds_processed:,}")
    logging.info(f"Total episodes (from DB): {total_episodes_in_db:,}")
    logging.info(f"Total episodes (found in feeds): {total_episodes_found_in_feeds:,}")
    logging.info(f"Total episode jobs enqueued: {episodes_enqueued:,}")


if __name__ == "__main__":
    main()