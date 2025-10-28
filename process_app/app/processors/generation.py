from typing import List
from .utils import split_event
from eugrid_monitor_core.models import RawGenerationEvent, EnrichedGenerationEvent
from config import settings

def process_generation_event(raw_event: RawGenerationEvent) -> List[EnrichedGenerationEvent]:
    """
        Processes and enriches a RawGenerationEvent.
        - Splits the event up into 15-minute intervals.
        - Adds more precise PSR type information.
        - Adds kgCO2e produced depending on quantity and PSR type.
        - Adds countries list based on EIC code.        
    """
    
    # Get PSR type and kg
    psr_details = settings.PSR_TYPE_MAPPINGS[raw_event.psr_type_code]
    psr_name = psr_details["name"]
    psr_emission_factor = psr_details["kg_co2e_mwh"]
    carbon_output_kg_co2e = raw_event.quantity_mw * psr_emission_factor

    # Get generation unit name and countries
    eic_details = settings.EIC_MAPPINGS[raw_event.eic_code]
    eic_display_name = eic_details["eic_display_name"]
    eic_long_name = eic_details["eic_long_name"]
    bidding_zone = eic_details["bidding_zone"]
    countries = eic_details["countries"]

    # Enrich the event
    enriched_event = EnrichedGenerationEvent(
        eic_display_name=eic_display_name,
        eic_long_name=eic_long_name,
        countries=countries,
        bidding_zone=bidding_zone,
        carbon_output_kg_co2e=carbon_output_kg_co2e,
        psr_type_name=psr_name,
        **raw_event.model_dump()
    )

    # Split the event into 15 minute intervals
    events = split_event(enriched_event, 15)

    return events