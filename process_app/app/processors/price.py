from .utils import split_event
from eugrid_monitor_core.models import RawPriceEvent, EnrichedPriceEvent
from ..exceptions import InvalidEicCodeError

def process_price_event(raw_event: RawPriceEvent, eic_mappings: dict) -> list[EnrichedPriceEvent]:
    """
    Enriches a RawPriceEvent with country metadata and splits it into 15-minute intervals.
    """
    
    eic_details = eic_mappings.get(raw_event.eic_code)
    if eic_details is None:
        raise InvalidEicCodeError(f"EIC code not found in mappings: {raw_event.eic_code}")

    # Enrich the event
    enriched_event = EnrichedPriceEvent(
        eic_display_name=eic_details["eic_display_name"],
        eic_long_name=eic_details["eic_long_name"],
        countries=eic_details["countries"],
        **raw_event.model_dump()
    )

    # Split the event into 15-minute intervals
    events = split_event(
        enriched_event,
        new_duration_mins=15,
        fields_to_divide=[] 
    )

    return events