import boto3
import requests
import os
import sys
import json
import logging
import time
import re
from urllib.parse import urlparse

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

def process_download_job(message):
    """
    Processes a single download job from the SQS queue.
    Downloads the file, uploads to S3, and updates DynamoDB.
    Returns True if a new episode was successfully downloaded, False otherwise.
    """
    receipt_handle = message['ReceiptHandle']
    try:
        body = json.loads(message['Body'])
        episode_url = body['episode_url']
        language = body.get('language', 'unknown')
    except (KeyError, json.JSONDecodeError) as e:
        logging.error(f"Invalid message format, deleting from queue: {message['Body']} - {e}")
        sqs_client.delete_message(QueueUrl=DOWNLOAD_SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        return False

    logging.info(f"Processing download for: {episode_url}")

    # 1. Download the audio file
    try:
        response = requests.get(episode_url, timeout=REQUEST_TIMEOUT, stream=True)
        response.raise_for_status()
        audio_content = response.content
    except requests.exceptions.RequestException as e:
        logging.warning(f"Failed to download {episode_url}: {e}")
        # Do not delete message, let it be retried or sent to DLQ
        return False

    # 2. Construct S3 path and upload
    filename = sanitize_filename(episode_url)
    s3_key = f"raw_audio/{language}/{filename}"
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=audio_content
        )
        logging.info(f"Successfully uploaded to S3: s3://{S3_BUCKET_NAME}/{s3_key}")
    except Exception as e:
        logging.error(f"Failed to upload {s3_key} to S3: {e}")
        # Do not delete message, let it be retried or sent to DLQ
        return False

    # 3. Perform conditional write to DynamoDB (The Guarantee)
    is_duplicate = False
    try:
        dynamodb_table.put_item(
            Item={'episode_url': episode_url},
            ConditionExpression='attribute_not_exists(episode_url)'
        )
        logging.info(f"Successfully marked {episode_url} as processed in DynamoDB.")
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
        logging.info(f"Successfully deleted message for {episode_url}.")
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
    log_interval = 100

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
