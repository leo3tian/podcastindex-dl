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
REQUEST_TIMEOUT = (3, 18)  # 5-second connect timeout, 25-second read timeout

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

# --- AWS Clients ---
sqs_client = boto3.client("sqs", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
dynamodb_table = dynamodb.Table(DYNAMODB_TABLE_NAME)


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


def send_batch_to_sqs(sqs_batch):
    """Sends a batch of messages to the final download SQS queue."""
    if not sqs_batch:
        return 0

    try:
        response = sqs_client.send_message_batch(
            QueueUrl=DOWNLOAD_SQS_QUEUE_URL, Entries=sqs_batch
        )
        if "Successful" in response:
            logging.info(f"Successfully sent {len(response['Successful'])} download jobs.")
        if "Failed" in response and response["Failed"]:
            logging.error(f"Failed to send {len(response['Failed'])} download jobs.")
            for failed_msg in response["Failed"]:
                logging.error(f"  - Failed Msg ID: {failed_msg['Id']}, Reason: {failed_msg['Message']}")
        return len(response.get("Successful", []))
    except Exception as e:
        logging.error(f"Error sending batch to SQS: {e}")
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
            sqs_client.delete_message(QueueUrl=FEEDS_SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
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
            sqs_client.delete_message(QueueUrl=FEEDS_SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            return

        existing_urls = get_existing_episodes(all_episode_urls)
        logging.info(f"Feed has {len(all_episode_urls)} episodes. Found {len(existing_urls)} existing in DynamoDB.")

        new_episodes_to_enqueue = [url for url in all_episode_urls if url not in existing_urls]

        if not new_episodes_to_enqueue:
            logging.info(f"No new episodes to enqueue for {rss_url}.")
            sqs_client.delete_message(QueueUrl=FEEDS_SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            return

        sqs_batch = []
        episodes_enqueued = 0
        for episode_url in new_episodes_to_enqueue:
            message = {
                "Id": str(time.time_ns()),
                "MessageBody": json.dumps({
                    "episode_url": episode_url,
                    "podcast_rss_url": rss_url,
                    "language": language
                })
            }
            sqs_batch.append(message)
            if len(sqs_batch) == SQS_BATCH_SIZE:
                episodes_enqueued += send_batch_to_sqs(sqs_batch)
                sqs_batch.clear()

        if sqs_batch:
            episodes_enqueued += send_batch_to_sqs(sqs_batch)

        logging.info(f"Successfully enqueued {episodes_enqueued} new episodes for {rss_url}.")
        sqs_client.delete_message(QueueUrl=FEEDS_SQS_QUEUE_URL, ReceiptHandle=receipt_handle)

    except Exception as e:
        logging.error(f"Failed to process feed {rss_url}: {e}", exc_info=True)
        # Do not delete message from queue, let it be retried


def main():
    """
    Main worker loop to continuously poll for and process feed jobs.
    """
    if not FEEDS_SQS_QUEUE_URL or not DOWNLOAD_SQS_QUEUE_URL or not DYNAMODB_TABLE_NAME:
        logging.error("Missing required environment variables.")
        sys.exit(1)

    logging.info("--- Starting Worker Script ---")
    logging.info(f"Polling SQS Queue: {FEEDS_SQS_QUEUE_URL}")
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=FEEDS_SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20, # Use long polling
                AttributeNames=['All']
            )

            if "Messages" in response:
                for message in response["Messages"]:
                    process_feed_job(message)
            else:
                logging.info("Queue is empty, waiting for new messages...")
                # Optional: break here if you want the script to exit when the queue is empty
                # break

        except Exception as e:
            logging.error(f"An error occurred in the main loop: {e}")
            time.sleep(10) # Wait before retrying


if __name__ == "__main__":
    main() 