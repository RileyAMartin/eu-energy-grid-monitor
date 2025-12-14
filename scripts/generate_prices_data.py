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

CLEAN_TYPES = [
    "Biomass", "Geothermal", "Hydro Pumped Storage", 
    "Hydro Run-of-river and poundage", "Hydro Water Reservoir", 
    "Marine", "Nuclear", "Other renewable", 
    "Solar", "Wind Offshore", "Wind Onshore"
]

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
    return re.sub(r'[^a-zA-Z0-9\- ]', '', filename)

def get_zones(conn, table_name: str) -> list[tuple]:
    """Return distinct (eic_code, eic_long_name) pairs from a table."""
    with conn.cursor() as cur:
        cur.execute(
            SQL("SELECT DISTINCT eic_code, eic_long_name FROM {} WHERE eic_code IS NOT NULL;")
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

def generate_latest_prices_overview(conn):
    """
    Generate overview.json containing the latest price per zone.
    """
    logging.info("Generating latest prices overview...")

    with conn.cursor() as cur:
        query = """
            SELECT DISTINCT ON (eic_code) eic_code, price_amount, start_time
            FROM energy_price_events
            WHERE price_amount IS NOT NULL
            ORDER BY eic_code, start_time DESC;
        """
        cur.execute(query)
        rows = cur.fetchall()

    if not rows:
        logging.warning("No price data found for overview.")
        return

    latest_data = {}
    for eic, price, start in rows:
        latest_data[eic] = {
            "price": float(price),
            "time": start.isoformat()
        }

    write_json("prices", "overview.json", {
        "latest": latest_data
    })
    logging.info(f"Saved overview.json with {len(latest_data)} zones.")

def generate_zone_details(conn, zones: list[tuple]) -> None:
    """
    Build per-zone price and clean generation series for recent history.
    """
    logging.info("Generating zone details...")

    start_dt, end_dt = get_flush_window(days=30)
    logging.info(f"Window: {start_dt} to {end_dt}")

    metadata = []

    generate_latest_prices_overview(conn)

    for eic_code, eic_name in zones:
        with conn.cursor() as cur:
            query = """
                WITH clean_gen AS (
                    SELECT 
                        start_time, 
                        SUM(quantity_mw) as clean_mw
                    FROM energy_generation_events
                    WHERE 
                        eic_code = %s
                        AND psr_type_name = ANY(%s)
                        AND start_time >= %s AND start_time < %s
                    GROUP BY start_time
                )
                SELECT 
                    p.start_time,
                    p.price_amount,
                    cg.clean_mw
                FROM energy_price_events p
                LEFT JOIN clean_gen cg ON p.start_time = cg.start_time
                WHERE 
                    p.eic_code = %s
                    AND p.start_time >= %s AND p.start_time < %s
                ORDER BY p.start_time ASC;
            """
            
            cur.execute(query, (
                eic_code, CLEAN_TYPES, start_dt, end_dt,
                eic_code, start_dt, end_dt
            ))
            rows = cur.fetchall()
        
        if not rows: continue

        unified_data = []
        for start, price, clean_mw in rows:
            if price is None: continue
            
            val_clean = float(clean_mw) if clean_mw is not None else None
            
            unified_data.append([
                start.isoformat(), 
                float(price), 
                val_clean
            ])

        filename = f"{safe_format_filename(eic_code)}.json"
        
        write_json("prices", filename, {
            "history": unified_data
        })

        metadata.append({
            "code": eic_code,
            "label": eic_name,
            "file": filename
        })
        logging.info(f"Generated {eic_name} details ({len(rows)} pts)")
    
    metadata.sort(key=lambda x: x['label'])
    write_json("prices", "metadata.json", metadata)

def main():
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

    try:
        conn = get_db_connection()        
        price_zones = get_zones(conn, 'energy_price_events')
        generate_zone_details(conn, price_zones)
    
    except Exception as e:
        logging.error(f"Script failed: {e}", exc_info=True) 
    finally:
        conn.close()

if __name__ == "__main__":
    main()