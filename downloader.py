import boto3
import requests
import os
import sys
import json
import logging
import time
import re
from urllib.parse import urlparse
import hashlib

# --- Configuration ---
DOWNLOAD_SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "https://sqs.us-west-1.amazonaws.com/450282239172/PodcastIndexQueue")
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME", "PodcastIndexJobs")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "podcast-index-dataset") # Required: Set this in your environment
AWS_REGION = os.getenv("AWS_REGION", "us-west-1")

# --- Timeouts ---
REQUEST_TIMEOUT = (5, 60)  # 5s connect, 60s read (generous for large audio files)

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,  # Set back to INFO to show progress updates
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

# --- AWS Clients ---
sqs_client = boto3.client("sqs", region_name=AWS_REGION)
s3_client = boto3.client("s3", region_name=AWS_REGION)
dynamodb_table = boto3.resource("dynamodb", region_name=AWS_REGION).Table(DYNAMODB_TABLE_NAME)

def sanitize_filename(url):
    """Creates a safe filename from a URL, preserving the extension."""
    parsed_url = urlparse(url)
    # Get path, remove leading slash, replace other slashes
    path = parsed_url.path.lstrip('/').replace('/', '_')
    # Basic sanitization
    safe_path = re.sub(r'[^a-zA-Z0-9._-]', '_', path)
    # Ensure it's not too long
    return safe_path[-128:]

def get_s3_key(podcast_id, language, episode_url):
    """
    Creates a unique, collision-proof S3 key.
    Path: raw_audio/{language}/{podcast_id}/{sha256_hash}.{extension}
    """
    # 1. Get the file extension
    path = urlparse(episode_url).path
    # Fallback to .mp3 if no extension is found
    # If there's no extension, or it's unreasonably long, fallback to mp3.
    if not ext or len(ext) > 5:
        ext = 'mp3'

    # 2. Create a SHA256 hash of the URL for a unique filename
    full_hash = hashlib.sha256(episode_url.encode('utf-8')).hexdigest()
    # Use a 16-character prefix as a safe and short unique identifier
    short_hash = full_hash[:16]

    # 3. Construct the final key
    return f"raw_audio/{language}/{podcast_id}/{short_hash}.{ext}"


def process_download_job(message):
    """
    Processes a single download job from the SQS queue.
    Downloads the file, uploads to S3, and updates DynamoDB.
    Returns True if a new episode was successfully downloaded, False otherwise.
    """
    receipt_handle = message['ReceiptHandle']
    try:
        body = json.loads(message['Body'])
        
        # --- Hardened Parsing Logic ---
        # The only required field is episode_url.
        episode_url = body.get('episode_url')
        if not episode_url:
            raise KeyError("'episode_url' is a required field.")
        
        podcast_id = body.get('podcast_id')
        if not podcast_id:
            raise KeyError("'podcast_id' is a required field for the S3 key.")

        # Language is optional and will default to 'unknown'.
        language = body.get('language', 'unknown')

    except (KeyError, json.JSONDecodeError) as e:
        logging.error(f"Invalid message format, deleting from queue: {message['Body']} - {e}")
        sqs_client.delete_message(QueueUrl=DOWNLOAD_SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        return False

    # 1. Download and Stream audio file directly to S3
    try:
        # Use a specific user-agent unless it's a hearthis.at URL to avoid throttling
        if 'hearthis.at' in episode_url:
            headers = {}
        else:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
            }

        with requests.get(episode_url, timeout=REQUEST_TIMEOUT, stream=True, headers=headers, allow_redirects=True) as response:
            response.raise_for_status() # Check for bad status codes (4xx or 5xx)

            # 2. Construct S3 path and upload via stream
            s3_key = get_s3_key(podcast_id, language, episode_url)

            try:
                # upload_fileobj streams the response body directly to S3, handling multipart uploads for large files automatically.
                # This keeps memory usage low and constant.
                logging.info(f"Starting S3 upload stream for {episode_url} to s3://{S3_BUCKET_NAME}/{s3_key}")
                s3_client.upload_fileobj(response.raw, S3_BUCKET_NAME, s3_key)
                logging.info(f"Finished S3 upload stream for {episode_url}")
            except Exception as e:
                logging.error(f"Failed during S3 upload stream for {episode_url}: {e}")
                return False # Let the job be retried

    except requests.exceptions.RequestException as e:
        logging.warning(f"Failed to start download stream for {episode_url}: {e}")
        # Do not delete message, let it be retried or sent to DLQ
        return False

    # 3. Perform conditional write to DynamoDB (The Guarantee)
    is_duplicate = False
    try:
        dynamodb_table.put_item(
            Item={'episode_url': episode_url},
            ConditionExpression='attribute_not_exists(episode_url)'
        )
    except dynamodb_table.meta.client.exceptions.ConditionalCheckFailedException:
        # This is not an error. It's the expected outcome for a duplicate job.
        logging.warning(f"Race condition detected: {episode_url} was already processed. Discarding duplicate job.")
        is_duplicate = True
        # The job was a duplicate, but we've confirmed the work is done, so we can delete the message.
    except Exception as e:
        logging.error(f"Failed to write to DynamoDB for {episode_url}: {e}")
        # This is a more serious error (e.g., throttling, permissions).
        # Let the message be retried.
        return False

    # 4. If all successful, delete the message from SQS
    try:
        sqs_client.delete_message(QueueUrl=DOWNLOAD_SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        logging.info(f"Successfully completed download for {episode_url}.")
    except Exception as e:
        logging.error(f"Failed to delete SQS message for {episode_url}: {e}")
        # This is not ideal, as the job might be re-processed, but the DynamoDB check will prevent re-download.
        return False

    # Return True only if it was a new download, not a duplicate.
    return not is_duplicate

def main():
    """
    Main downloader loop to continuously poll for and process download jobs.
    """
    if not S3_BUCKET_NAME:
        logging.error("Missing required environment variable: S3_BUCKET_NAME")
        sys.exit(1)

    logging.info("--- Starting Downloader Script ---")
    logging.info(f"Polling SQS Queue: {DOWNLOAD_SQS_QUEUE_URL}")

    downloads_completed = 0
    log_interval = 10

    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=DOWNLOAD_SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                AttributeNames=['All']
            )
            if "Messages" in response:
                for message in response["Messages"]:
                    if process_download_job(message):
                        downloads_completed += 1
                        if downloads_completed > 0 and downloads_completed % log_interval == 0:
                            logging.info(f"--- Progress: {downloads_completed} new episodes downloaded ---")
            else:
                logging.info("Download queue is empty, waiting for new messages...")
        except Exception as e:
            logging.error(f"An error occurred in the main loop: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
