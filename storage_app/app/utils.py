import psycopg2
import logging
from typing import List

def perform_bulk_insert(conn, table_name: str, columns: List[str], conflict_columns: List[str], events: List[dict]):
    if not events:
        return 0

    data_tuples = [
        tuple(event.get(col) for col in columns)
        for event in events
    ]

    if not conflict_columns:
        insert_query = f"""
            INSERT INTO {table_name} ({", ".join(f'"{c}"' for c in columns)})
            VALUES %s;
        """
    else:
        insert_query = f"""
            INSERT INTO {table_name} ({", ".join(f'"{c}"' for c in columns)})
            VALUES %s
            ON CONFLICT ({", ".join(f'"{c}"' for c in conflict_columns)})
            DO NOTHING;
        """

    cursor = None
    try:
        cursor = conn.cursor()

        # Perform the bulk insert
        psycopg2.extras.execute_values(
            cursor,
            insert_query,
            data_tuples,
            template=None,
            page_size=100
        )
        conn.commit()

        inserted_count = cursor.rowcount
        logging.info(f"--- Attempted insert of {inserted_count} rows into {table_name} ---")
        return inserted_count
    
    except Exception as e:
        logging.error(f"Database bulk insert failed for {table_name}: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
