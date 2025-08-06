import sqlite3
import psycopg2
import os
import sys
import logging

# --- Configuration ---
# Source SQLite database
SQLITE_DB_PATH = os.getenv("DB_PATH", "podcastindex_feeds.db")

# Destination PostgreSQL database (from environment variables)
PG_HOST = os.getenv("PG_HOST")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# --- Constants ---
BATCH_SIZE = 10000  # Process 10,000 records at a time

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

def get_postgres_conn():
    """Establishes connection to the PostgreSQL database."""
    if not all([PG_HOST, PG_DATABASE, PG_USER, PG_PASSWORD]):
        logging.error("Missing one or more required PostgreSQL environment variables (PG_HOST, PG_DATABASE, PG_USER, PG_PASSWORD).")
        sys.exit(1)
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Could not connect to PostgreSQL database: {e}")
        sys.exit(1)

def main():
    """
    Main migration function to move a lean subset of data from SQLite to PostgreSQL.
    """
    # --- Connect to databases ---
    logging.info(f"Connecting to source SQLite DB: {SQLITE_DB_PATH}")
    sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
    sqlite_cursor = sqlite_conn.cursor()

    logging.info(f"Connecting to destination PostgreSQL DB: {PG_HOST}")
    pg_conn = get_postgres_conn()
    pg_cursor = pg_conn.cursor()

    # --- Create lean table in PostgreSQL ---
    logging.info("Creating lean 'podcasts' table in PostgreSQL...")
    create_table_query = """
    DROP TABLE IF EXISTS podcasts;
    CREATE TABLE podcasts (
        id BIGINT PRIMARY KEY,
        url TEXT UNIQUE NOT NULL,
        language TEXT,
        episode_count INTEGER,
        processing_status TEXT DEFAULT 'pending' NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """
    pg_cursor.execute(create_table_query)
    pg_conn.commit()
    logging.info("Lean table 'podcasts' created successfully.")

    # --- Fetch data from SQLite and insert into PostgreSQL ---
    # We now select episodeCount and will map it to episode_count.
    sqlite_cursor.execute("SELECT id, url, language, episodeCount FROM podcasts")
    total_rows_migrated = 0

    while True:
        batch = sqlite_cursor.fetchmany(BATCH_SIZE)
        if not batch:
            break

        # The executemany function is smart enough to map the columns.
        # We are inserting into 'id', 'url', 'language', and 'episode_count'.
        # 'processing_status' will use its default value.
        insert_query = "INSERT INTO podcasts (id, url, language, episode_count) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING"
        
        pg_cursor.executemany(insert_query, batch)
        pg_conn.commit()

        total_rows_migrated += len(batch)
        logging.info(f"Migrated {total_rows_migrated:,} records...")

    # --- Clean up ---
    sqlite_conn.close()
    pg_conn.close()
    logging.info(f"--- Lean migration complete! Total records migrated: {total_rows_migrated:,} ---")


if __name__ == "__main__":
    main() 