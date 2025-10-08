from datetime import datetime
from typing import List
from pydantic import BaseModel

class RawGenerationEvent(BaseModel):
    """Model to represent a raw generation event consumed from Kafka."""
    eic_code: str
    start_time: datetime
    end_time: datetime
    quantity: float
    psr_type_code: str
    measurement_unit: str
    source_document_mrid: str

class EnrichedGenerationEvent(RawGenerationEvent):
    countries: List[str]
    carbon_output_gco2e: float
    psr_type_name: str
