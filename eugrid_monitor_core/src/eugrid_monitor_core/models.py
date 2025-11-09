import base64
import json
from datetime import datetime
from enum import Enum
from typing import List
from pydantic import BaseModel, field_validator

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

class DlqIngestionEvent(BaseModel):
    """
        Represents an error message to be uploaded to the ingestion DLQ.
        Must contain eic_code and start/end-time to later refetch/rehandle the message.
    """
    eic_code: str
    start_time: datetime  # Start time of the failed time window
    end_time: datetime  # End time of the failed time window
    failed_at: datetime
    error_type: str
    error_msg: str | None

class DlqProcessingEvent(BaseModel):
    """A record for a message that failed during the processing stage."""
    failed_at: datetime
    error_type: str
    error_msg: str | None
    original_message: str

    @field_validator("original_message", mode="before")
    def encode_message_as_base64(cls, v):
        """
        If the original message is passed in bytes (likely a JSON validation error),
        they'll be converted to a Base64-encoded string.
        """
        if isinstance(v, bytes):
            return base64.b64encode(v).decode("utf-8")
        return v

