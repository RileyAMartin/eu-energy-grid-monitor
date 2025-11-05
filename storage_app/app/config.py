from typing import List
from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from eugrid_monitor_core.models import EnrichedGenerationEvent
from eugrid_monitor_core.topics import ENRICHED_GENERATION_EVENTS

load_dotenv()

DB_MAPPINGS = {
    ENRICHED_GENERATION_EVENTS: {
        "table_name": "energy_generation_events",
        "columns": [
            "eic_code",
            "eic_display_name",
            "eic_long_name",
            "quantity_mw",
            "carbon_output_kg_co2e",
            "psr_type_code",
            "psr_type_name",
            "countries",
            "bidding_zone",
            "start_time",
            "end_time"
        ],
        "model": EnrichedGenerationEvent,
        "conflict_columns": []
    }
}

class Settings(BaseSettings):

    # Database Config
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str
    DB_PORT: str
    DB_NAME: str

    # Kafka config
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_SASL_USERNAME: str
    KAFKA_SASL_PASSWORD: str
    KAFKA_GROUP_ID: str = "storage-sink-v1"

    # Table mappings for each enriched Kafka topic
    DB_MAPPINGS: dict = DB_MAPPINGS

    # Consts
    MAX_BATCH_SIZE: int = 1000
    MAX_BATCH_INTERVAL_SECONDS: int = 10

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()