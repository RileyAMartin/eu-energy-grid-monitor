from dataclasses import dataclass
from typing import Type, Optional
from dotenv import load_dotenv
from pydantic import ConfigDict, BaseModel
from pydantic_settings import BaseSettings
from eugrid_monitor_core.models import EnrichedGenerationEvent, EnrichedPriceEvent
from eugrid_monitor_core.topics import ENRICHED_GENERATION_EVENTS, ENRICHED_PRICE_EVENTS

load_dotenv()

@dataclass
class TableMapping():
    """
    Config object that maps a Kafak topic to a db table.
    """
    table_name: str
    model: Type[BaseModel]
    conflict_columns: list[str]
    override_columns: Optional[list[str]] = None  # Specific columns to write to the db

    @property
    def columns(self) -> list[str]:
        """
        Returns the list of columns to insert to the db.
        If no override columns are provided, returns all fields from the data model.
        """
        if self.override_columns:
            return self.override_columns
        return self.model.model_fields.keys()

DB_MAPPINGS = {
    ENRICHED_GENERATION_EVENTS: TableMapping(
        table_name="energy_generation_events",
        model=EnrichedGenerationEvent,
        conflict_columns=[
            "eic_code",
            "psr_type_code",
            "start_time"
        ],
        override_columns=[
            "eic_code",
            "eic_display_name",
            "eic_long_name",
            "quantity_mw",
            "quantity_mwh",
            "carbon_output_kg_co2e",
            "psr_type_code",
            "psr_type_name",
            "countries",
            "start_time",
            "end_time"
        ]
    ),
    ENRICHED_PRICE_EVENTS: TableMapping(
        table_name="energy_price_events",
        model=EnrichedPriceEvent,
        conflict_columns=[
            "eic_code",
            "start_time"
        ],
        override_columns=[
            "eic_code",
            "eic_display_name",
            "eic_long_name",
            "countries",
            "price_amount",
            "currency",
            "start_time",
            "end_time"
        ]
    )
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

    model_config = ConfigDict(
        extra="ignore"
    )

settings = Settings()