import os
import logging
import psycopg2
from dotenv import load_dotenv

# Setup
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def delete_all_rows(chunk_size=100000):
    """
    Deletes all rows from the energy_generation_events table in chunks
    to avoid transaction timeouts.
    """
    db_config = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT", "26257"),
        "dbname": os.getenv("DB_NAME"),
    }

    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        conn.autocommit = True # CockroachDB handles DELETE LIMIT better with autocommit or explicit transactions
        cursor = conn.cursor()
        
        total_deleted = 0
        logging.info("--- Starting Batched Deletion ---")

        while True:
            # CockroachDB supports DELETE ... LIMIT which is perfect for this
            cursor.execute(f"DELETE FROM energy_generation_events LIMIT {chunk_size};")
            deleted_count = cursor.rowcount

            total_deleted += deleted_count
            logging.info(f"Deleted {deleted_count} rows. (Total: {total_deleted})")

            if deleted_count == 0:
                break

            # Optional: Sleep briefly to let the DB breathe if it's under heavy load
            # time.sleep(0.5) 

        logging.info("--- Deletion Complete ---")
        logging.info(f"Total rows removed: {total_deleted}")

    except Exception as e:
        logging.error(f"Deletion failed: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    delete_all_rows()