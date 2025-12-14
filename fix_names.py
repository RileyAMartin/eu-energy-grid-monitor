import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def check_kosovo():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    
    try:
        with conn.cursor() as cur:
            # Check what "Kosovo" entries actually exist
            print("Searching for variations of 'Kosovo' in energy_generation_events...")
            cur.execute("""
                SELECT DISTINCT eic_long_name 
                FROM energy_generation_events 
                WHERE eic_long_name LIKE '%Kosovo%';
            """)
            
            rows = cur.fetchall()
            if not rows:
                print("No rows found containing 'Kosovo'.")
            else:
                print(f"Found {len(rows)} variations:")
                for r in rows:
                    # Print with quotes so we can see trailing spaces
                    print(f" -> '{r[0]}'")

    finally:
        conn.close()

if __name__ == "__main__":
    check_kosovo()