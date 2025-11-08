from dateutil.relativedelta import relativedelta
from typing import List
from eugrid_monitor_core.models import EntsoeEvent, EnrichedGenerationEvent
from ..exceptions import InvalidEventDurationError

def split_event(event: EntsoeEvent, new_duration_mins: int, fields_to_divide: List[str] = []) -> List[EntsoeEvent]:
    """
        Splits an event into intervals of the given duration, dividing the quantity as well.
        The duration of the new intervals must allow for the creation of equal-duration events.
        E.g. a 35 minute event cannot be divided into intervals of 6 minutes.
    """

    # Calculate the number of events to create
    event_duration = event.end_time - event.start_time
    event_mins = int(event_duration.total_seconds() // 60)
    if event_mins % new_duration_mins != 0:
        raise InvalidEventDurationError(f"Invalid event duration.")
    num_intervals = event_mins // new_duration_mins

    # Args for new events
    increment = relativedelta(minutes=new_duration_mins)
    start_time = event.start_time
    end_time = start_time + increment

    # Calculate the values for each field to split
    updated_fields = {}
    for field in fields_to_divide:
        val = getattr(event, field, None)
        if val is not None and isinstance(val, (int, float)):
            updated_fields[field] = val / num_intervals

    # Split the events
    events = []
    for _ in range(num_intervals):
        new_event = event.model_copy(update={
            "start_time": start_time,
            "end_time": end_time,
            **updated_fields
        })
        events.append(new_event)
        start_time = end_time
        end_time += increment

    return events
