import boto3
import random
import os
import logging
import binascii
from dotenv import load_dotenv
import shutil

load_dotenv()

# --- Configuration ---
S3_BUCKET_NAME = "podcast-index-dataset"
AWS_REGION = "us-west-1" 
S3_PREFIX = "raw_audio/en/"
SAMPLE_SIZE = 20 
URL_EXPIRATION_SECONDS = 3600

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def download_samples(s3_client, bucket_name, keys, download_dir):
    """Downloads a list of S3 keys to a local directory."""
    if os.path.exists(download_dir):
        logging.warning(f"Removing existing '{download_dir}/' directory.")
        shutil.rmtree(download_dir)
        
    os.makedirs(download_dir)
    
    downloaded_paths = []
    logging.info(f"Downloading {len(keys)} samples to '{download_dir}/'...")
    for key in keys:
        try:
            local_filename = os.path.basename(key)
            local_filepath = os.path.join(download_dir, local_filename)
            s3_client.download_file(bucket_name, key, local_filepath)
            downloaded_paths.append(local_filepath)
        except Exception as e:
            logging.error(f"Failed to download {key}: {e}")
            
    return downloaded_paths

def get_random_samples_efficiently(s3_client, bucket_name, prefix, sample_size):
    """
    Gets a random sample of keys by jumping to random prefixes in the key space
    and sampling from the returned page of results. This is much more efficient
    and provides a better random distribution than the previous methods.
    """
    samples = set()
    attempts = 0
    max_attempts = sample_size * 5  # Avoid an infinite loop

    logging.info("Starting efficient random sampling...")
    while len(samples) < sample_size and attempts < max_attempts:
        # Generate a random 2-byte hex string for a random starting point
        random_hex = binascii.hexlify(os.urandom(2)).decode('utf-8')
        start_after = f"{prefix}{random_hex}"
        
        try:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                StartAfter=start_after,
                MaxKeys=1000  # Fetch a full page for better random sampling
            )
            if 'Contents' in response and response['Contents']:
                # Pick a random key from the returned page
                random_key = random.choice(response['Contents'])['Key']
                if random_key not in samples:
                    samples.add(random_key)
                    logging.info(f"Found sample {len(samples)}/{sample_size}: {random_key}")

        except Exception as e:
            logging.error(f"Error listing objects with StartAfter={start_after}: {e}")
        
        attempts += 1
    
    if len(samples) < sample_size:
        logging.warning(f"Could only find {len(samples)} unique samples after {max_attempts} attempts.")

    return list(samples)

def main():
    """
    Connects to S3, efficiently takes a random sample of audio files,
    downloads them locally, and waits for user input to clean them up.
    """
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    
    sample_keys = get_random_samples_efficiently(s3_client, S3_BUCKET_NAME, S3_PREFIX, SAMPLE_SIZE)

    if not sample_keys:
        logging.error("Could not retrieve any samples.")
        return
        
    local_samples_dir = "samples"
    downloaded_files = download_samples(s3_client, S3_BUCKET_NAME, sample_keys, local_samples_dir)

    if not downloaded_files:
        logging.error("Failed to download any samples.")
        return

    print("\n" + "="*50)
    print(f"Successfully downloaded {len(downloaded_files)} files to the '{local_samples_dir}/' directory.")
    print("You can now inspect them with an audio player or editor.")
    print("="*50 + "\n")

    try:
        input("Press Enter to delete the downloaded samples and exit...")
    except KeyboardInterrupt:
        print("\n\nAborted. The 'samples' directory will not be deleted.")
        return

    try:
        shutil.rmtree(local_samples_dir)
        print(f"Successfully deleted the '{local_samples_dir}/' directory.")
    except Exception as e:
        logging.error(f"Failed to delete the '{local_samples_dir}/' directory: {e}")


if __name__ == "__main__":
    # Check for AWS credentials as a safeguard
    if not (os.getenv('AWS_ACCESS_KEY_ID') and os.getenv('AWS_SECRET_ACCESS_KEY')):
         logging.error("Error: AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) not found in environment.")
         logging.error("Please configure your credentials to run this script.")
    else:
        main() 