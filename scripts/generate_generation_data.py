import logging
import os
import json
import regex as re
import psycopg2
from psycopg2.sql import SQL, Identifier
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

_ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = os.path.join(_ROOT_DIR, "docs", "data")

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def write_json(folder, filename, data):
    """Write JSON into docs/data/{folder}/{filename}."""
    path = os.path.join(DATA_DIR, folder)
    os.makedirs(path, exist_ok=True)

    with open(os.path.join(path, filename), "w") as f:
        json.dump(data, f)

def safe_format_filename(filename: str) -> str:
    """Remove unsafe characters from a filename."""
    return re.sub(r'[[^a-zA-Z0-9\- ]]', '', filename)

def get_zones(conn, table_name: list[str]) -> list[str]:
    """Return distinct (eic_code, eic_long_name) pairs from a table."""
    with conn.cursor() as cur:
        cur.execute(
            SQL("SELECT DISTINCT eic_code, eic_long_name FROM {};")
                .format(Identifier(table_name))
        )
        return cur.fetchall()

def get_flush_window(days: int = 30) -> tuple[datetime, datetime]:
    """
    Calculate a strict hourly window ending at the most recent full hour.
    """
    now = datetime.now()
    end_window = now.replace(minute=0, second=0, microsecond=0)    
    start_window = end_window - timedelta(days=days)
    return start_window, end_window

def generate_aggregated_all(conn, start_dt, end_dt):
    """Generate a JSON file aggregating all zones."""
    
    with conn.cursor() as cur:
        query = """
            SELECT
                psr_type_name,
                SUM(quantity_mw),
                SUM(quantity_mwh),
                start_time
            FROM energy_generation_events
            WHERE
                start_time >= %s
                AND start_time < %s
            GROUP BY psr_type_name, start_time
            ORDER BY start_time ASC;
        """
        cur.execute(query, (start_dt, end_dt))
        rows = cur.fetchall()

    if not rows:
        return None

    chart_data = defaultdict(list)
    for psr, mw, mwh, start in rows:
        chart_data[psr].append([start.isoformat(), float(mw), float(mwh)])

    filename = "all_regions.json"
    write_json("generation", filename, chart_data)

    return {
        "code": "ALL",
        "label": "All Regions",
        "file": filename
    }

def generate_generation_mix(conn, zones: list[str]) -> None:
    """Build per-zone generation series and metadata for recent history."""
    logging.info("Generating generation mix...")

    start_dt, end_dt = get_flush_window(days=30)
    logging.info(f"Window: {start_dt} to {end_dt}")

    metadata = []

    all_meta = generate_aggregated_all(conn, start_dt, end_dt)
    if all_meta:
        metadata.append(all_meta)

    for eic_code, eic_name in zones:
        with conn.cursor() as cur:
            query = """
                SELECT
                    psr_type_name,
                    quantity_mw,
                    quantity_mwh,
                    start_time
                FROM energy_generation_events
                WHERE
                    eic_code = %s
                    AND start_time >= %s
                    AND start_time < %s
                ORDER BY start_time ASC;
            """
            cur.execute(query, (eic_code, start_dt, end_dt))
            rows = cur.fetchall()
        
        if not rows: continue

        chart_data = defaultdict(list)
        for psr, mw, mwh, start in rows:
            chart_data[psr].append([start.isoformat(), float(mw), float(mwh)])

        filename = f"{safe_format_filename(eic_code)}.json"
        write_json("generation", filename, chart_data)

        metadata.append({
            "code": eic_code,
            "label": eic_name,
            "file": filename
        })
        logging.info(f"Generated {eic_name} mix ({len(rows)} pts)")
    
    write_json("generation", "metadata.json", metadata)


def main():
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

    try:
        conn = get_db_connection()        
        gen_zones = get_zones(conn, 'energy_generation_events')
        generate_generation_mix(conn, gen_zones)
    
    except Exception as e:
        logging.error(f"Script failed: {e}", exc_info=True) 
    finally:
        conn.close()

if __name__ == "__main__":
    main()