import boto3
import random
import os
import logging
import binascii
from dotenv import load_dotenv
# `shutil` is no longer needed as we are not managing local files.

load_dotenv()

# --- Configuration ---
# R2 is S3-compatible, so we use the same bucket name.
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "podcastindex-dataset")
# You must provide your Cloudflare Account ID.
CF_ACCOUNT_ID = os.getenv("CF_ID")
# These are the R2-specific API credentials.
CF_R2_ACCESS_KEY_ID = os.getenv("R2_ID")
CF_R2_SECRET_ACCESS_KEY = os.getenv("R2_KEY")

R2_PREFIX = "raw_audio/en/"
SAMPLE_SIZE = 20
# How long the generated links will be valid for (in seconds).
URL_EXPIRATION_SECONDS = 3600 

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def generate_presigned_urls(r2_client, bucket_name, keys, expiration):
    """Generates pre-signed URLs for a list of R2 keys."""
    urls = []
    logging.info(f"Generating {len(keys)} pre-signed URLs valid for {expiration} seconds...")
    for key in keys:
        try:
            url = r2_client.generate_presigned_url(
                ClientMethod='get_object',
                Params={'Bucket': bucket_name, 'Key': key},
                ExpiresIn=expiration
            )
            urls.append(url)
        except Exception as e:
            logging.error(f"Failed to generate pre-signed URL for {key}: {e}")
            
    return urls

def get_random_samples_from_r2(r2_client, bucket_name, prefix, sample_size):
    """
    Gets a random sample of keys by jumping to random prefixes in the R2 key space.
    This is much more efficient than listing all keys.
    """
    samples = set()
    attempts = 0
    max_attempts = sample_size * 5  # Avoid an infinite loop

    logging.info("Starting efficient random sampling from R2...")
    while len(samples) < sample_size and attempts < max_attempts:
        # Generate a random 2-byte hex string for a random starting point
        random_hex = binascii.hexlify(os.urandom(2)).decode('utf-8')
        start_after = f"{prefix}{random_hex}"
        
        try:
            response = r2_client.list_objects_v2(
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
            logging.error(f"Error listing R2 objects with StartAfter={start_after}: {e}")
        
        attempts += 1
    
    if len(samples) < sample_size:
        logging.warning(f"Could only find {len(samples)} unique samples after {max_attempts} attempts.")

    return list(samples)

def main():
    """
    Connects to R2, efficiently takes a random sample of audio files,
    and generates temporary, pre-signed URLs to listen to them in a browser.
    """
    if not all([CF_ACCOUNT_ID, CF_R2_ACCESS_KEY_ID, CF_R2_SECRET_ACCESS_KEY]):
        logging.error("Error: Cloudflare credentials (CF_ACCOUNT_ID, CF_R2_ACCESS_KEY_ID, CF_R2_SECRET_ACCESS_KEY) not found in environment.")
        logging.error("Please configure your credentials in a .env file or as environment variables.")
        return

    endpoint_url = f"https://{CF_ACCOUNT_ID}.r2.cloudflarestorage.com"
    
    r2_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=CF_R2_ACCESS_KEY_ID,
        aws_secret_access_key=CF_R2_SECRET_ACCESS_KEY,
        region_name='auto',  # R2 is region-agnostic
    )
    
    logging.info(f"Connected to R2 endpoint: {endpoint_url}")
    
    sample_keys = get_random_samples_from_r2(r2_client, R2_BUCKET_NAME, R2_PREFIX, SAMPLE_SIZE)

    if not sample_keys:
        logging.error("Could not retrieve any samples from R2.")
        return
        
    presigned_urls = generate_presigned_urls(r2_client, R2_BUCKET_NAME, sample_keys, URL_EXPIRATION_SECONDS)

    if not presigned_urls:
        logging.error("Failed to generate any pre-signed URLs.")
        return

    print("\n" + "="*80)
    print(f"Successfully generated {len(presigned_urls)} pre-signed URLs.")
    print("Click a link to listen to the audio directly in your browser.")
    print(f"(Links will expire in {URL_EXPIRATION_SECONDS // 60} minutes)")
    print("="*80 + "\n")

    for url in presigned_urls:
        print(f"- {url}\n")


if __name__ == "__main__":
    main() 