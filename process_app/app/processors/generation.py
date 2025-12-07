from .utils import split_event
from eugrid_monitor_core.models import RawGenerationEvent, EnrichedGenerationEvent
from ..exceptions import InvalidEicCodeError, InvalidPsrTypeCodeError

def process_generation_event(raw_event: RawGenerationEvent, psr_type_mappings: dict, eic_mappings: dict) -> list[EnrichedGenerationEvent]:
    """
        Processes and enriches a RawGenerationEvent:
        - Splits the event up into 15-minute intervals.
        - Adds more precise PSR type information.
        - Adds kgCO2e produced depending on quantity and PSR type.
        - Adds countries list and bidding zone based on EIC code.        
    """
    
    psr_details = psr_type_mappings.get(raw_event.psr_type_code)
    if psr_details is None:
        raise InvalidPsrTypeCodeError(f"PSR type code not found in mapping: {raw_event.psr_type_code}")

    # Calculate total MWh and carbon output
    duration_hours = (raw_event.end_time - raw_event.start_time).total_seconds() / 3600
    total_mwh = raw_event.quantity_mw * duration_hours
    total_co2 = total_mwh * psr_details["kg_co2e_mwh"]

    eic_details = eic_mappings.get(raw_event.eic_code)
    if eic_details is None:
        raise InvalidEicCodeError(f"EIC code not found in mappings: {raw_event.eic_code}")

    # Enrich the event
    enriched_event = EnrichedGenerationEvent(
        eic_display_name=eic_details["eic_display_name"],
        eic_long_name=eic_details["eic_long_name"],
        countries=eic_details["countries"],
        carbon_output_kg_co2e=total_co2,
        quantity_mwh=total_mwh,
        psr_type_name=psr_details["name"],
        **raw_event.model_dump()
    )

    # Split the event into 15 minute intervals
    events = split_event(
        enriched_event,
        new_duration_mins=15,
        fields_to_divide=["quantity_mwh", "carbon_output_kg_co2e"]
    )

    return events
