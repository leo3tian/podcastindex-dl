import feedparser
import boto3
import json
import logging
import os
import sys
import time
import requests
import re
import urllib3
import psycopg2

# Suppress only the InsecureRequestWarning from urllib3, as we are intentionally
# disabling SSL verification for some feeds.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration ---
# You can change these values or set them as environment variables
# This is the queue workers pull from
FEEDS_SQS_QUEUE_URL = os.getenv("FEEDS_SQS_QUEUE_URL", "https://sqs.us-west-1.amazonaws.com/450282239172/FeedsToProcessQueue")
# This is the final queue for downloaders
DOWNLOAD_SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "https://sqs.us-west-1.amazonaws.com/450282239172/PodcastIndexQueue")
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME", "PodcastIndexJobs")
AWS_REGION = os.getenv("AWS_REGION", "us-west-1")

# --- Tuning Levers ---
DB_UPDATE_BATCH_SIZE = int(os.getenv("DB_UPDATE_BATCH_SIZE", 100))
# The threshold at which we start staggering SQS messages
STAGGER_THRESHOLD = 20
# How many jobs to send in each staggered batch
STAGGER_BATCH_SIZE = 20
# How many seconds to delay each subsequent batch
STAGGER_DELAY_SECONDS = 10

# --- AWS / DB Config ---
PG_HOST = os.getenv("PG_HOST")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# --- Constants ---
SQS_BATCH_SIZE = 10  # For sending to the download queue
MAX_DYNAMODB_BATCH_GET = 100 # DynamoDB limit
# This will match common private IP ranges (10.x.x.x, 172.16-31.x.x, 192.168.x.x)
PRIVATE_IP_REGEX = re.compile(
    r"^(?:10|127)\.(?:[0-9]{1,3}\.){2}[0-9]{1,3}$|"
    r"^(?:172\.(?:1[6-9]|2[0-9]|3[0-1]))\.(?:[0-9]{1,3}\.){1}[0-9]{1,3}$|"
    r"^(?:192\.168)\.(?:[0-9]{1,3}\.){1}[0-9]{1,3}$"
)

# --- Timeouts ---
# Use a tuple for (connect_timeout, read_timeout)
# Connect: How long to wait for a connection to be established.
# Read: How long to wait for the server to send data once connected.
REQUEST_TIMEOUT = (5, 25)  # 5-second connect timeout, 25-second read timeout

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

# --- AWS / DB Clients ---
sqs_client = boto3.client("sqs", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
dynamodb_table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# --- Global State ---
# Workers will batch DB updates for efficiency. These lists hold pending updates.
COMPLETED_IDS_BATCH = []
FAILED_IDS_BATCH = []
# Global DB Connection - Workers can reuse a single connection for their lifetime.
pg_conn = None

def flush_db_update_batch(status, force_flush=False):
    """
    Writes a batch of completed or failed IDs to the database.
    A flush is triggered when the batch is full or on a forced flush (e.g., shutdown).
    """
    conn = get_postgres_conn()
    if not conn:
        logging.error("Cannot flush DB batch: No DB connection.")
        return

    id_list = []
    batch_size_to_check = DB_UPDATE_BATCH_SIZE
    
    if status == 'complete':
        if not COMPLETED_IDS_BATCH: return
        # Only flush if the batch is full or if we're forcing it on shutdown
        if len(COMPLETED_IDS_BATCH) >= batch_size_to_check or force_flush:
            id_list = list(COMPLETED_IDS_BATCH)
            COMPLETED_IDS_BATCH.clear()
        else:
            return
    elif status == 'failed':
        if not FAILED_IDS_BATCH: return
        if len(FAILED_IDS_BATCH) >= batch_size_to_check or force_flush:
            id_list = list(FAILED_IDS_BATCH)
            FAILED_IDS_BATCH.clear()
        else:
            return
    elif status == 'complete_empty':
        if not COMPLETED_IDS_BATCH: return
        if len(COMPLETED_IDS_BATCH) >= batch_size_to_check or force_flush:
            id_list = list(COMPLETED_IDS_BATCH)
            COMPLETED_IDS_BATCH.clear()
        else:
            return
    else:
        return

    logging.info(f"Flushing {len(id_list)} records to DB with status '{status}'...")
    try:
        with conn.cursor() as cursor:
            # Use a transaction to update the batch
            cursor.execute(
                f"UPDATE podcasts SET processing_status = %s, updated_at = CURRENT_TIMESTAMP WHERE id = ANY(%s)",
                (status, id_list)
            )
        conn.commit()
    except psycopg2.Error as e:
        logging.error(f"DB Error flushing batch for status '{status}': {e}. Re-queuing IDs.")
        conn.rollback() # Rollback the failed transaction
        conn.close() # Force reconnect
        # On failure, add the IDs back to the global list to be retried.
        if status == 'complete':
            COMPLETED_IDS_BATCH.extend(id_list)
        elif status == 'failed':
            FAILED_IDS_BATCH.extend(id_list)
        elif status == 'complete_empty':
            COMPLETED_IDS_BATCH.extend(id_list)

def get_postgres_conn():
    """Establishes or returns the global PostgreSQL connection."""
    global pg_conn
    if pg_conn and not pg_conn.closed:
        return pg_conn
    
    if not all([PG_HOST, PG_DATABASE, PG_USER, PG_PASSWORD]):
        logging.error("Missing required PostgreSQL environment variables.")
        return None
    try:
        pg_conn = psycopg2.connect(
            host=PG_HOST, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD
        )
        return pg_conn
    except psycopg2.OperationalError as e:
        logging.error(f"Could not connect to PostgreSQL database: {e}")
        return None

# --- New Status ---
# 'complete_empty' is for feeds that parse correctly but have no valid audio.
def mark_feed_as_status(podcast_id, status):
    """Generic function to add a podcast ID to the correct batch for a given status."""
    if status == 'complete':
        COMPLETED_IDS_BATCH.append(podcast_id)
    elif status == 'failed':
        FAILED_IDS_BATCH.append(podcast_id)
    elif status == 'complete_empty':
        # For now, we'll treat empty feeds as a success state.
        # We can create a separate list/logic later if needed.
        COMPLETED_IDS_BATCH.append(podcast_id)
    else:
        logging.warning(f"Unknown status '{status}' for podcast_id {podcast_id}")
        return
    
    # Flush the appropriate batch
    flush_db_update_batch(status)

def mark_feed_as_complete(podcast_id):
    """Adds a podcast ID to the batch to be marked as 'complete'."""
    mark_feed_as_status(podcast_id, 'complete')

def mark_feed_as_failed(podcast_id):
    """Adds a podcast ID to the batch to be marked as 'failed'."""
    mark_feed_as_status(podcast_id, 'failed')


def get_existing_episodes(episode_urls):
    """
    Checks DynamoDB for a list of episode URLs to see which ones already exist.
    Returns a set of existing URLs.
    """
    if not episode_urls:
        return set()

    existing_urls = set()
    # Handle DynamoDB's batch limit of 100
    for i in range(0, len(episode_urls), MAX_DYNAMODB_BATCH_GET):
        batch_urls = episode_urls[i:i + MAX_DYNAMODB_BATCH_GET]
        keys = [{"episode_url": url} for url in batch_urls]
        try:
            response = dynamodb.batch_get_item(
                RequestItems={DYNAMODB_TABLE_NAME: {"Keys": keys}}
            )
            for item in response.get("Responses", {}).get(DYNAMODB_TABLE_NAME, []):
                existing_urls.add(item["episode_url"])
        except Exception as e:
            logging.error(f"Error checking DynamoDB for existing episodes: {e}")
            # In case of error, conservatively assume all might exist to avoid re-queueing
            # A more robust solution could implement retries
            return set(batch_urls)
    return existing_urls


def send_batch_to_sqs(sqs_batch, delay_seconds=0):
    """Sends a batch of messages to the final download SQS queue, with an optional delay."""
    if not sqs_batch:
        return 0
    
    # Respect the SQS maximum delay of 900 seconds (15 minutes)
    effective_delay = min(delay_seconds, 900)
    if delay_seconds > 900:
        logging.warning(f"Calculated delay of {delay_seconds}s exceeds SQS limit. Capping at 900s.")

    # Add the delay to each message if specified
    for message in sqs_batch:
        message['DelaySeconds'] = effective_delay

    try:
        response = sqs_client.send_message_batch(
            QueueUrl=DOWNLOAD_SQS_QUEUE_URL, Entries=sqs_batch
        )
        if "Failed" in response and response["Failed"]:
            # Log the specific failure reasons from SQS for better debugging
            for failed_msg in response["Failed"]:
                logging.error(f"SQS send failed for Msg ID {failed_msg['Id']}: Code={failed_msg['Code']}, Message={failed_msg['Message']}")
        return len(response.get("Successful", []))
    except Exception as e:
        # Log the boto3/botocore exception, which is much more informative
        logging.error(f"Error sending batch to SQS (delay: {effective_delay}s): {e}")
        return 0


def process_feed_job(message):
    """
    Processes a single feed job from the SQS queue.
    """
    receipt_handle = message['ReceiptHandle']
    try:
        body = json.loads(message['Body'])
        podcast_id = body['podcast_id']
        rss_url = body['rss_url']
        language = body.get('language', 'unknown') # Safely get language
    except (KeyError, json.JSONDecodeError) as e:
        logging.error(f"Invalid message format, deleting from queue: {message['Body']} - {e}")
        sqs_client.delete_message(QueueUrl=FEEDS_SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        return

    logging.info(f"Processing feed for podcast_id {podcast_id}: {rss_url}")

    # --- Pre-flight checks ---
    # Check for private IPs before making a network request
    hostname = requests.utils.urlparse(rss_url).hostname
    if hostname and PRIVATE_IP_REGEX.match(hostname):
        logging.warning(f"Skipping private/internal IP address: {rss_url}")
        mark_feed_as_failed(podcast_id)
        sqs_client.delete_message(QueueUrl=FEEDS_SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        return

    try:
        # Use requests with timeout, then pass to feedparser
        try:
            logging.info(f"Fetching feed content for {rss_url}...")
            response = requests.get(
                rss_url,
                timeout=REQUEST_TIMEOUT,
                headers={'User-Agent': 'PodcastIndex-Downloader/1.0'},
                verify=False  # Ignore SSL certificate errors
            )
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            
            logging.info(f"Parsing feed content for {rss_url}...")
            feed = feedparser.parse(response.content)
        except requests.exceptions.RequestException as e:
            logging.warning(f"Failed to fetch feed {rss_url}: {e}")
            
            # Check for permanent failures vs. temporary ones.
            # Client errors (4xx) and DNS/Connection errors are permanent.
            is_permanent_failure = False
            if e.response and 400 <= e.response.status_code < 500:
                is_permanent_failure = True
            # NewConnectionError is a strong signal of a dead domain or firewall block.
            elif isinstance(e, requests.exceptions.ConnectionError):
                is_permanent_failure = True

            if is_permanent_failure:
                mark_feed_as_failed(podcast_id)
                sqs_client.delete_message(QueueUrl=FEEDS_SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            # Otherwise, it's a server (5xx) or temporary network error, so let it be retried.
            return

        if feed.bozo:
            # Setting feed.bozo to 0 suppresses the warning print by feedparser
            # We are logging it ourselves anyway.
            feed.bozo = 0
            logging.warning(f"Bozo feed (might be malformed): {rss_url} - {feed.bozo_exception}")

        all_episode_urls = []
        audio_extensions = {'.mp3', '.m4a', '.ogg', '.wav', '.aac', '.flac', '.opus'}

        for entry in feed.entries:
            for enclosure in getattr(entry, 'enclosures', []):
                href = enclosure.get('href')
                if not href:
                    continue

                # Check 1: Standard audio MIME type
                is_audio_mime = 'audio' in enclosure.get('type', '')

                # Check 2: File extension, if MIME type is missing or generic
                href_lower = href.lower()
                has_audio_extension = any(href_lower.endswith(ext) for ext in audio_extensions)

                if is_audio_mime or has_audio_extension:
                    all_episode_urls.append(href)
                    break  # Assume one audio enclosure per entry

        if not all_episode_urls:
            logging.warning(f"No audio enclosures found in feed: {rss_url}")
            # Use our new status for better data integrity
            mark_feed_as_status(podcast_id, 'complete_empty')
            sqs_client.delete_message(QueueUrl=FEEDS_SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            return

        existing_urls = get_existing_episodes(all_episode_urls)
        logging.info(f"Feed has {len(all_episode_urls)} episodes. Found {len(existing_urls)} existing in DynamoDB.")

        new_episodes_to_enqueue = [url for url in all_episode_urls if url not in existing_urls]

        if not new_episodes_to_enqueue:
            logging.info(f"No new episodes to enqueue for {rss_url}.")
            mark_feed_as_complete(podcast_id) # Mark as complete even if no new episodes
            sqs_client.delete_message(QueueUrl=FEEDS_SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            return

        # --- Enqueueing with All-or-Nothing Logic ---
        total_to_enqueue = len(new_episodes_to_enqueue)
        episodes_enqueued = 0
        all_batches_successful = True
        
        # --- Staggering Logic ---
        if total_to_enqueue > STAGGER_THRESHOLD:
            logging.warning(f"Large feed detected ({total_to_enqueue} new episodes). Staggering enqueue to SQS.")
            current_delay = 0
            # The outer loop determines which large chunk of jobs gets a specific delay
            for i in range(0, total_to_enqueue, STAGGER_BATCH_SIZE):
                staggered_chunk = new_episodes_to_enqueue[i:i + STAGGER_BATCH_SIZE]
                
                # The inner loop respects the SQS API limit of 10
                sqs_api_batch = []
                for episode_url in staggered_chunk:
                    sqs_api_batch.append({
                        "Id": str(time.time_ns()),
                        "MessageBody": json.dumps({
                            "episode_url": episode_url,
                            "podcast_rss_url": rss_url,
                            "language": language,
                            "podcast_id": podcast_id
                        })
                    })
                    if len(sqs_api_batch) == SQS_BATCH_SIZE:
                        sent_count = send_batch_to_sqs(sqs_api_batch, delay_seconds=current_delay)
                        episodes_enqueued += sent_count
                        if sent_count < len(sqs_api_batch):
                            all_batches_successful = False
                            break
                        sqs_api_batch.clear()
                
                if not all_batches_successful:
                    break
                
                if sqs_api_batch:
                    sent_count = send_batch_to_sqs(sqs_api_batch, delay_seconds=current_delay)
                    episodes_enqueued += sent_count
                    if sent_count < len(sqs_api_batch):
                        all_batches_successful = False
                
                if not all_batches_successful:
                    break
                
                current_delay += STAGGER_DELAY_SECONDS
        else:
            # --- Standard Logic for smaller feeds ---
            sqs_batch = []
            for episode_url in new_episodes_to_enqueue:
                message = {
                    "Id": str(time.time_ns()),
                    "MessageBody": json.dumps({
                        "episode_url": episode_url,
                        "podcast_rss_url": rss_url,
                        "language": language,
                        "podcast_id": podcast_id
                    })
                }
                sqs_batch.append(message)
                if len(sqs_batch) == SQS_BATCH_SIZE:
                    sent_count = send_batch_to_sqs(sqs_batch)
                    episodes_enqueued += sent_count
                    if sent_count < len(sqs_batch):
                        all_batches_successful = False
                        break
                    sqs_batch.clear()
            
            if all_batches_successful and sqs_batch:
                sent_count = send_batch_to_sqs(sqs_batch)
                episodes_enqueued += sent_count
                if sent_count < len(sqs_batch):
                    all_batches_successful = False

        if all_batches_successful:
            logging.info(f"Successfully enqueued all {episodes_enqueued} new episodes for {rss_url}.")
            mark_feed_as_complete(podcast_id)
            sqs_client.delete_message(QueueUrl=FEEDS_SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        else:
            logging.error(f"Failed to enqueue all episodes for {rss_url}. Enqueued {episodes_enqueued}/{total_to_enqueue}. Will be retried.")
            # DO NOT mark as complete. DO NOT delete SQS message. Let the job time out.

    except Exception as e:
        logging.error(f"Failed to process feed {rss_url}: {e}", exc_info=True)
        # For unexpected errors, mark as failed so we don't retry indefinitely
        mark_feed_as_failed(podcast_id)
        sqs_client.delete_message(QueueUrl=FEEDS_SQS_QUEUE_URL, ReceiptHandle=receipt_handle)


def main():
    """
    Main worker loop to continuously poll for and process feed jobs.
    """
    if not all([FEEDS_SQS_QUEUE_URL, DOWNLOAD_SQS_QUEUE_URL, DYNAMODB_TABLE_NAME, PG_HOST]):
        logging.error("Missing required environment variables.")
        sys.exit(1)

    logging.info("--- Starting Worker Script ---")
    logging.info(f"Polling SQS Queue: {FEEDS_SQS_QUEUE_URL}")
    logging.info(f"DB Update Batch Size: {DB_UPDATE_BATCH_SIZE}")

    try:
        while True:
            try:
                response = sqs_client.receive_message(
                    QueueUrl=FEEDS_SQS_QUEUE_URL,
                    MaxNumberOfMessages=10, # Fetch more messages to improve throughput
                    WaitTimeSeconds=20,
                    AttributeNames=['All']
                )
                if "Messages" in response:
                    for message in response["Messages"]:
                        process_feed_job(message)
                else:
                    logging.info("Queue is empty, waiting for new messages...")
                    # Flush any lingering items if the queue is empty
                    flush_db_update_batch('complete', force_flush=True)
                    flush_db_update_batch('failed', force_flush=True)
                    flush_db_update_batch('complete_empty', force_flush=True)

            except Exception as e:
                logging.error(f"An error occurred in the main SQS loop: {e}")
                time.sleep(10) # Wait before retrying
    finally:
        logging.info("--- Shutting down. Flushing final DB update batches... ---")
        flush_db_update_batch('complete', force_flush=True)
        flush_db_update_batch('failed', force_flush=True)
        flush_db_update_batch('complete_empty', force_flush=True)
        if pg_conn:
            pg_conn.close()
            logging.info("PostgreSQL connection closed.")

if __name__ == "__main__":
    main() 