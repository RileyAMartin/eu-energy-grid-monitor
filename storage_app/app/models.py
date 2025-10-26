from datetime import datetime
from typing import List
from pydantic import BaseModel

class Event(BaseModel):
    """Represents an event from the ENTSO-E API."""
    eic_code: str
    start_time: datetime
    end_time: datetime

class RawGenerationEvent(Event):
    """Model to represent a raw generation event consumed from Kafka."""
    quantity_mw: float
    psr_type_code: str
    measurement_unit: str
    source_document_mrid: str

class EnrichedGenerationEvent(RawGenerationEvent):
    """Represents an enriched generation event that will be uploaded to Kafka."""
    eic_display_name: str
    eic_long_name: str
    bidding_zone: str | None
    countries: List[str]
    carbon_output_kg_co2e: float
    psr_type_name: str
