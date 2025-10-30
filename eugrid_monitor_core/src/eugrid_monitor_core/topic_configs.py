from .models import KafkaTopicConfig
from .models import RawGenerationEvent, EnrichedGenerationEvent, DlqIngestionEvent

RAW_GENERATION_EVENTS = KafkaTopicConfig(
    topic_name="raw-generation-events",
    value_schema=RawGenerationEvent
)

ENRICHED_GENERATION_EVENTS = KafkaTopicConfig(
    topic_name="enriched-generation-events",
    value_schema=EnrichedGenerationEvent    
)

DLQ_INGESTION_EVENTS = KafkaTopicConfig(
    topic_name="dlq-ingestion",
    value_schema=DlqIngestionEvent
)