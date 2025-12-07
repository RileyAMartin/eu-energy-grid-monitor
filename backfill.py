import argparse
import logging
import time
import psycopg2
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from ingest_app.app.api.client import EntsoeClient
from ingest_app.app.ingestors.generation import GenerationIngestor 
from process_app.app.processors.generation import process_generation_event
from storage_app.app.repository import PostgresRepo
from storage_app.app.config import settings as storage_settings
from ingest_app.app.config import settings as ingest_settings
from process_app.app.config import settings as process_settings
from eugrid_monitor_core.topics import ENRICHED_GENERATION_EVENTS

# Setup
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_local_backfill(days: int):
    
    # The backfill begins today
    end_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=days)
    
    logging.info(f"--- Starting Backfill ({days} days) ---")
    logging.info(f"Window: {start_time.date()} -> {end_time.date()}")

    client = EntsoeClient(api_key=ingest_settings.ENTSOE_API_KEY)
    
    # Connect to DB
    logging.info("Connecting to Database...")
    db_conn = psycopg2.connect(
        user=storage_settings.DB_USER,
        password=storage_settings.DB_PASSWORD,
        host=storage_settings.DB_HOST,
        port=storage_settings.DB_PORT,
        dbname=storage_settings.DB_NAME,
    )
    repo = PostgresRepo(db_conn)
    
    # Load Configs
    eic_codes = ingest_settings.EIC_CODES_GENERATION
    db_config = storage_settings.DB_MAPPINGS[ENRICHED_GENERATION_EVENTS]
    
    total_codes = len(eic_codes)    
    for i, eic_code in enumerate(eic_codes):
        logging.info(f"Processing {eic_code} ({i+1}/{total_codes})")
        
        ingestor = GenerationIngestor(
            producer=None, 
            eic_code=eic_code, 
            client=client, 
            query_config=None
        )

        chunk_start = start_time
        while chunk_start < end_time:
            chunk_end = min(chunk_start + timedelta(days=7), end_time)
            
            try:
                # Fetch the XML from the API
                params = ingestor._get_query_params(chunk_start, chunk_end)
                xml_bytes = client.get_data(params)
                
                # Parse the XML to get raw events
                raw_events = ingestor._parse_response(xml_bytes)
                
                if not raw_events:
                    logging.info(f"  -> No data for {chunk_start.date()}")
                    chunk_start = chunk_end
                    continue

                # Enrich the raw events
                enriched_events = []
                for raw in raw_events:
                    processed = process_generation_event(
                        raw, 
                        process_settings.PSR_TYPE_MAPPINGS, 
                        process_settings.EIC_MAPPINGS
                    )
                    enriched_events.extend([e.model_dump(mode='json') for e in processed])

                # Upload the enriched events to the db
                if enriched_events:
                    count = repo.bulk_insert(
                        table_name=db_config.table_name,
                        columns=db_config.columns,
                        conflict_columns=db_config.conflict_columns,
                        events=enriched_events
                    )
                    logging.info(f"  -> Inserted {count} rows for {chunk_start.date()}")

            except Exception as e:
                logging.error(f"Failed chunk {chunk_start.date()} for {eic_code}: {e}")
            
            chunk_start = chunk_end
            time.sleep(0.5) 

    logging.info("Backfill Complete.")
    db_conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a direct local backfill (No Kafka)")
    parser.add_argument("days", type=int, help="Number of days to backfill")
    
    args = parser.parse_args()
    run_local_backfill(args.days)