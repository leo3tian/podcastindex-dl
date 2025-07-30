import psycopg2
import boto3
import json
import logging
import os
import sys
import time

# --- Configuration ---
# --- Tuning Levers ---
# BATCH_SIZE: How many pending feeds to grab from the DB in each cycle.
# SLEEP_SECONDS: How long to wait before fetching the next batch.
FEEDER_BATCH_SIZE = int(os.getenv("FEEDER_BATCH_SIZE", 1000))
FEEDER_SLEEP_SECONDS = int(os.getenv("FEEDER_SLEEP_SECONDS", 10))
# How old an 'in_progress' job can be before we consider it stale.
STALE_JOB_TIMEOUT_MINUTES = int(os.getenv("STALE_JOB_TIMEOUT_MINUTES", 60))

# --- AWS / DB Config ---
FEEDS_SQS_QUEUE_URL = os.getenv("FEEDS_SQS_QUEUE_URL", "https://sqs.us-west-1.amazonaws.com/450282239172/FeedsToProcessQueue")
AWS_REGION = os.getenv("AWS_REGION", "us-west-1")
PG_HOST = os.getenv("PG_HOST")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# --- Constants ---
SQS_BATCH_SIZE = 10  # SQS send_message_batch limit is 10

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

# --- AWS / DB Clients ---
sqs_client = boto3.client("sqs", region_name=AWS_REGION)

def get_postgres_conn():
    """Establishes a reusable connection to the PostgreSQL database."""
    if not all([PG_HOST, PG_DATABASE, PG_USER, PG_PASSWORD]):
        logging.error("Missing required PostgreSQL environment variables.")
        sys.exit(1)
    try:
        conn = psycopg2.connect(
            host=PG_HOST, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD
        )
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Could not connect to PostgreSQL database: {e}")
        return None

def send_batch_to_sqs(sqs_batch):
    """Sends a batch of messages to the SQS queue."""
    if not sqs_batch:
        return 0
    try:
        response = sqs_client.send_message_batch(
            QueueUrl=FEEDS_SQS_QUEUE_URL, Entries=sqs_batch
        )
        if "Failed" in response and response["Failed"]:
            logging.error(f"Failed to send {len(response['Failed'])} feed jobs.")
        return len(response.get("Successful", []))
    except Exception as e:
        logging.error(f"Error sending batch to SQS: {e}")
        return 0

def reset_stale_jobs(conn):
    """
    Finds jobs that have been 'in_progress' for too long and resets them to 'pending'.
    This prevents jobs from getting stuck if a worker crashes.
    """
    if not conn:
        return 0
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE podcasts
                SET processing_status = 'pending', updated_at = CURRENT_TIMESTAMP
                WHERE processing_status = 'in_progress'
                AND updated_at < NOW() - INTERVAL '%s minutes';
            """, (STALE_JOB_TIMEOUT_MINUTES,))
            
            stale_count = cursor.rowcount
            conn.commit()
            if stale_count > 0:
                logging.warning(f"Reset {stale_count} stale 'in_progress' jobs back to 'pending'.")
            return stale_count
    except psycopg2.Error as e:
        logging.error(f"DB Error resetting stale jobs: {e}")
        # Close connection on error to force reconnect next time
        conn.close() 
        return 0

def main():
    """
    Main feeder loop. Continuously pulls pending jobs from Postgres,
    marks them as in_progress, enqueues them to SQS, and cleans up stale jobs.
    """
    logging.info("--- Starting Feeder Manager Script ---")
    logging.info(f"Batch Size: {FEEDER_BATCH_SIZE}, Sleep Time: {FEEDER_SLEEP_SECONDS}s, Stale Timeout: {STALE_JOB_TIMEOUT_MINUTES}min")
    
    pg_conn = get_postgres_conn()
    if not pg_conn:
        sys.exit(1)

    loop_count = 0
    while True:
        try:
            # --- Janitor Duty: Reset stale jobs every 5 loops ---
            loop_count += 1
            if loop_count % 5 == 0:
                reset_stale_jobs(pg_conn)

            with pg_conn.cursor() as pg_cursor:
                # This transaction is critical. It finds pending rows, locks them so no other
                # feeder can touch them, and returns their data in a single, atomic operation.
                # 'FOR UPDATE SKIP LOCKED' is designed for exactly this kind of concurrent queue processing.
                pg_cursor.execute("""
                    UPDATE podcasts
                    SET processing_status = 'in_progress', updated_at = CURRENT_TIMESTAMP
                    WHERE id IN (
                        SELECT id
                        FROM podcasts
                        WHERE processing_status = 'pending'
                        ORDER BY id
                        FOR UPDATE SKIP LOCKED
                        LIMIT %s
                    )
                    RETURNING id, url, language;
                """, (FEEDER_BATCH_SIZE,))
                
                jobs_to_enqueue = pg_cursor.fetchall()
                pg_conn.commit()

            if not jobs_to_enqueue:
                logging.info("No pending feeds found. Waiting...")
                time.sleep(FEEDER_SLEEP_SECONDS * 2) # Sleep longer if there's no work
                continue

            logging.info(f"Found and locked {len(jobs_to_enqueue)} new jobs. Enqueueing to SQS...")
            
            sqs_batch = []
            for job in jobs_to_enqueue:
                podcast_id, rss_url, language = job
                message = {
                    "Id": str(podcast_id),
                    "MessageBody": json.dumps({
                        "podcast_id": podcast_id,
                        "rss_url": rss_url,
                        "language": (language and language.strip()) or "unknown" # Sanitize language
                    })
                }
                sqs_batch.append(message)
                if len(sqs_batch) == SQS_BATCH_SIZE:
                    send_batch_to_sqs(sqs_batch)
                    sqs_batch.clear()
            
            if sqs_batch: # Send any remaining messages
                send_batch_to_sqs(sqs_batch)

            logging.info(f"Successfully enqueued {len(jobs_to_enqueue)} jobs. Sleeping for {FEEDER_SLEEP_SECONDS}s.")
            time.sleep(FEEDER_SLEEP_SECONDS)

        except psycopg2.Error as e:
            logging.error(f"Database error: {e}. Attempting to reconnect...")
            pg_conn.close()
            time.sleep(15)
            pg_conn = get_postgres_conn()
            if not pg_conn:
                logging.error("Failed to reconnect, shutting down.")
                break
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            time.sleep(15)

    if pg_conn:
        pg_conn.close()
    logging.info("--- Feeder Manager Script Shut Down ---")

if __name__ == "__main__":
    main() 