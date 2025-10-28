from dateutil.relativedelta import relativedelta
from typing import List
from eugrid_monitor_core.models import Event
from exceptions import InvalidEventDurationError

def split_event(event: Event, new_duration_mins: int) -> List[Event]:
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
    quantity_per_interval = event.quantity_mw / num_intervals

    # Split the events
    events = []
    for _ in range(num_intervals):
        new_event = event.model_copy(update={
            "start_time": start_time,
            "end_time": end_time,
            "quantity_mw": quantity_per_interval
        })
        events.append(new_event)
        start_time = end_time
        end_time += increment

    return events
