import logging
import psycopg2

class PostgresRepo():
    """
    Handles Postgres operations.
    This only exists for dependency injection purposes.
    """

    def __init__(self, connection):
        self._conn = connection
    
    def bulk_insert(self, table_name: str, columns: list[str], conflict_columns: list[str], events: list[dict]) -> int:
        """
        Inserts events into the db.
        Returns the number of rows inserted.
        """
        if not events:
            return 0

        # Psycopg2 requires that the values be tuples rather than dicts
        data_tuples = [
            tuple(event.get(col) for col in columns)
            for event in events
        ]

        # Build the SQL query
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
            cursor = self._conn.cursor()
            psycopg2.extras.execute_values(
                cursor,
                insert_query,
                data_tuples,
                template=None,
                page_size=1000,
                fetch=True
            )
            self._conn.commit()
            
            return cursor.rowcount if cursor.rowcount != -1 else len(events)
            
        except Exception as e:
            self._conn.rollback()
            logging.error(f"DB Insert failed: {e}")
            raise
        finally:
            if cursor:
                cursor.close()