import psycopg2.extras
import logging
from typing import List
from .config import settings

def perform_bulk_insert(conn, table_name: str, columns: List[str], conflict_columns: List[str], events: List[dict]):
    """
    Insert the event into the database.
    """
    
    if not events:
        return 0

    # Psycopg2 requires that the values be tuples rather than dicts
    data_tuples = [
        tuple(event.get(col) for col in columns)
        for event in events
    ]

    # Format the SQL query
    if not conflict_columns:
        insert_query = f"""
            INSERT INTO {table_name} ({", ".join(f'"{c}"' for c in columns)})
            VALUES %s
            RETURNING 1;
        """
    else:
        insert_query = f"""
            INSERT INTO {table_name} ({", ".join(f'"{c}"' for c in columns)})
            VALUES %s
            ON CONFLICT ({", ".join(f'"{c}"' for c in conflict_columns)})
            DO NOTHING
            RETURNING 1;
        """

    cursor = None
    try:
        cursor = conn.cursor()

        # Perform the bulk insert
        results = psycopg2.extras.execute_values(
            cursor,
            insert_query,
            data_tuples,
            template=None,
            page_size=settings.MAX_BATCH_SIZE,
            fetch=True
        )
        conn.commit()

        return len(results)
    
    except Exception as e:
        logging.error(f"Database bulk insert failed for {table_name}: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
