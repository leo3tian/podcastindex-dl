import sqlite3
import psycopg2
import os
import sys
import logging

# --- Configuration ---
# Source SQLite database
SQLITE_DB_PATH = os.getenv("DB_PATH", "podcastindex.db")

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
    Main migration function to move data from SQLite to PostgreSQL.
    """
    # --- Connect to databases ---
    logging.info(f"Connecting to source SQLite DB: {SQLITE_DB_PATH}")
    sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
    sqlite_cursor = sqlite_conn.cursor()

    logging.info(f"Connecting to destination PostgreSQL DB: {PG_HOST}")
    pg_conn = get_postgres_conn()
    pg_cursor = pg_conn.cursor()

    # --- Create table in PostgreSQL ---
    logging.info("Creating 'podcasts' table in PostgreSQL...")
    # This schema matches the original, plus the new status column.
    # It also sets more appropriate PostgreSQL data types.
    create_table_query = """
    CREATE TABLE IF NOT EXISTS podcasts (
        id BIGINT PRIMARY KEY,
        url TEXT UNIQUE,
        title TEXT,
        lastUpdate BIGINT,
        link TEXT,
        lastHttpStatus INT,
        dead INT,
        contentType TEXT,
        itunesId BIGINT,
        originalUrl TEXT,
        itunesAuthor TEXT,
        itunesOwnerName TEXT,
        explicit INT,
        imageUrl TEXT,
        itunesType TEXT,
        generator TEXT,
        newestItemPubdate BIGINT,
        language TEXT,
        oldestItemPubdate BIGINT,
        episodeCount INT,
        popularityScore INT,
        priority INT,
        createdOn BIGINT,
        updateFrequency INT,
        chash TEXT,
        host TEXT,
        newestEnclosureUrl TEXT,
        podcastGuid TEXT,
        description TEXT,
        category1 TEXT,
        category2 TEXT,
        category3 TEXT,
        category4 TEXT,
        category5 TEXT,
        category6 TEXT,
        category7 TEXT,
        category8 TEXT,
        category9 TEXT,
        category10 TEXT,
        newestEnclosureDuration INT,
        processing_status TEXT DEFAULT 'pending' NOT NULL
    );
    """
    pg_cursor.execute(create_table_query)
    pg_conn.commit()
    logging.info("Table 'podcasts' created successfully (if it didn't exist).")

    # --- Fetch data from SQLite and insert into PostgreSQL ---
    sqlite_cursor.execute("SELECT * FROM podcasts")
    total_rows_migrated = 0

    while True:
        batch = sqlite_cursor.fetchmany(BATCH_SIZE)
        if not batch:
            break

        # Prepare the data for insertion
        records_to_insert = []
        for row in batch:
            # Add the 'pending' status to each row
            records_to_insert.append(row + ('pending',))

        # Build the INSERT statement
        # This uses %s placeholders, which psycopg2 safely populates.
        # It handles the 39 columns from SQLite + 1 new status column.
        placeholders = ", ".join(["%s"] * 40)
        insert_query = f"INSERT INTO podcasts VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING"
        
        pg_cursor.executemany(insert_query, records_to_insert)
        pg_conn.commit()

        total_rows_migrated += len(batch)
        logging.info(f"Migrated {total_rows_migrated:,} records...")

    # --- Clean up ---
    sqlite_conn.close()
    pg_conn.close()
    logging.info(f"--- Migration complete! Total records migrated: {total_rows_migrated:,} ---")


if __name__ == "__main__":
    main() 