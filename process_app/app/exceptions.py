class InvalidEventDurationError(Exception):
    """Raised when trying to split an Event into shorter Events of an invalid duration."""