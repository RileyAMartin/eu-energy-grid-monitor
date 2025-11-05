import json
from datetime import datetime
from enum import Enum
from typing import List
from pydantic import BaseModel

class EntsoeEvent(BaseModel):
    """Represents an event from the ENTSO-E API."""
    eic_code: str
    start_time: datetime
    end_time: datetime

class RawGenerationEvent(EntsoeEvent):
    """Model to represent a raw generation event consumed from Kafka."""
    quantity_mw: float
    psr_type_code: str
    measurement_unit: str
    source_document_mrid: str

class EnrichedGenerationEvent(RawGenerationEvent):
    """Represents an enriched generation event that will be uploaded to Kafka."""
    eic_display_name: str
    eic_long_name: str
    bidding_zone: str
    countries: List[str]
    carbon_output_kg_co2e: float
    psr_type_name: str

class EventJSONDecoder(json.JSONEncoder):
    """Custom decoder to convert events to JSON format."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class DlqErrorTypesEnum(str, Enum):
    """The types of errors that can be stored in the DLQ."""
    NETWORK = "network"
    VALIDATION = "validation"
    PARSING = "parsing"
    OTHER = "other"

class DlqIngestionEvent(EntsoeEvent):
    """
        Represents an error message to be uploaded to the DLQ.
        Must contain eic_code and start/end-time to later refetch the message.
    """
    error_type: DlqErrorTypesEnum
    error_msg: str | None
