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
import multiprocessing

# --- Configuration ---
DOWNLOAD_SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "https://sqs.us-west-1.amazonaws.com/450282239172/PodcastIndexQueue")
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME", "PodcastIndexJobs")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "podcast-index-dataset") # Required: Set this in your environment
AWS_REGION = os.getenv("AWS_REGION", "us-west-1")

# --- Timeouts ---
REQUEST_TIMEOUT = (5, 60)  # 5s connect, 60s read (generous for large audio files)
MAX_DOWNLOAD_TIME = 45  # 45 seconds total for a download job to complete

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
    # 1. Get the file extension from the URL path
    path = urlparse(episode_url).path

    # Use rsplit to robustly get the extension. It handles cases with no dot.
    parts = path.rsplit('.', 1)

    ext = ''
    if len(parts) == 2:
        # If a dot was found, the extension is the second part
        ext = parts[1]

    # If the extension is missing, invalid, or just part of a long path segment,
    # default to 'mp3'. A simple length check is a decent heuristic.
    # Also check for slashes to avoid using a path segment as an extension.
    if not ext or len(ext) > 5 or '/' in ext:
        ext = 'mp3'

    # 2. Create a SHA256 hash of the URL for a unique filename
    full_hash = hashlib.sha256(episode_url.encode('utf-8')).hexdigest()
    # Use a 16-character prefix as a safe and short unique identifier
    short_hash = full_hash[:16]

    # 3. Construct the final key
    return f"raw_audio/{language}/{podcast_id}/{short_hash}.{ext}"


def _execute_download_job(episode_url, podcast_id, language, receipt_handle, aws_region, download_sqs_url, dynamodb_table_name, s3_bucket_name, request_timeout):
    """
    This function runs in a separate process. It contains the core logic for
    downloading a file, uploading it to S3, and updating DynamoDB.
    It communicates its outcome via sys.exit() codes.
    - 0: Success
    - 1: Duplicate job, successfully handled
    - 2: Recoverable error, job should be retried (message not deleted)
    """
    # Re-initialize AWS clients in the new process
    sqs_client = boto3.client("sqs", region_name=aws_region)
    s3_client = boto3.client("s3", region_name=aws_region)
    dynamodb_table = boto3.resource("dynamodb", region_name=aws_region).Table(dynamodb_table_name)
    
    try:
        # 1. Download and Stream audio file directly to S3
        try:
            # Use a specific user-agent unless it's a hearthis.at URL to avoid throttling
            if 'hearthis.at' in episode_url:
                headers = {}
            else:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
                }

            with requests.get(episode_url, timeout=request_timeout, stream=True, headers=headers, allow_redirects=True) as response:
                response.raise_for_status()

                # 2. Construct S3 path and upload via stream
                s3_key = get_s3_key(podcast_id, language, episode_url)

                try:
                    logging.info(f"Starting S3 upload stream for {episode_url} to s3://{s3_bucket_name}/{s3_key}")
                    s3_client.upload_fileobj(response.raw, s3_bucket_name, s3_key)
                    logging.info(f"Finished S3 upload stream for {episode_url}")
                except Exception as e:
                    logging.error(f"Failed during S3 upload stream for {episode_url}: {e}")
                    sys.exit(2)

        except requests.exceptions.RequestException as e:
            logging.warning(f"Failed to start download stream for {episode_url}: {e}")
            sys.exit(2)

        # 3. Perform conditional write to DynamoDB
        try:
            dynamodb_table.put_item(
                Item={'episode_url': episode_url},
                ConditionExpression='attribute_not_exists(episode_url)'
            )
        except dynamodb_table.meta.client.exceptions.ConditionalCheckFailedException:
            logging.warning(f"Race condition: {episode_url} already processed. Deleting duplicate job.")
            try:
                sqs_client.delete_message(QueueUrl=download_sqs_url, ReceiptHandle=receipt_handle)
                sys.exit(1)
            except Exception as e:
                logging.error(f"Failed to delete duplicate SQS message for {episode_url}: {e}")
                sys.exit(2)
                
        except Exception as e:
            logging.error(f"Failed to write to DynamoDB for {episode_url}: {e}")
            sys.exit(2)

        # 4. If all successful, delete the message from SQS
        try:
            sqs_client.delete_message(QueueUrl=download_sqs_url, ReceiptHandle=receipt_handle)
            logging.info(f"Successfully completed download for {episode_url}.")
        except Exception as e:
            logging.error(f"Failed to delete SQS message for {episode_url}: {e}")
            sys.exit(2)
        
        sys.exit(0)

    except Exception as e:
        logging.error(f"An unexpected error occurred in the child process for {episode_url}: {e}", exc_info=True)
        sys.exit(2)


def process_download_job(message):
    """
    Processes a single download job from the SQS queue.
    This function now acts as a manager, spawning a separate process to do
    the actual work and enforcing a 2-minute timeout.
    Returns True if a new episode was successfully downloaded, False otherwise.
    """
    receipt_handle = message['ReceiptHandle']
    try:
        body = json.loads(message['Body'])
        
        episode_url = body.get('episode_url')
        if not episode_url:
            raise KeyError("'episode_url' is a required field.")
        
        podcast_id = body.get('podcast_id')
        if not podcast_id:
            raise KeyError("'podcast_id' is a required field for the S3 key.")

        language = body.get('language', 'unknown')

    except (KeyError, json.JSONDecodeError) as e:
        logging.error(f"Invalid message format, deleting from queue: {message['Body']} - {e}")
        sqs_client.delete_message(QueueUrl=DOWNLOAD_SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        return False

    # Spawn a new process to handle the download
    process_args = (
        episode_url, podcast_id, language, receipt_handle,
        AWS_REGION, DOWNLOAD_SQS_QUEUE_URL, DYNAMODB_TABLE_NAME, S3_BUCKET_NAME, REQUEST_TIMEOUT
    )
    process = multiprocessing.Process(target=_execute_download_job, args=process_args)
    process.start()
    process.join(timeout=45)

    if process.is_alive():
        logging.warning(f"DOWNLOAD TIMEOUT for {episode_url}. Terminating process.")
        process.terminate()
        process.join()
        return False

    if process.exitcode == 0:
        return True
    elif process.exitcode == 1:
        return False
    else:
        logging.error(f"Download worker for {episode_url} failed with exit code {process.exitcode}. Will be retried.")
        return False

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
