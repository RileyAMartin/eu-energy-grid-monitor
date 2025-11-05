class InvalidEventDurationError(Exception):
    """Raised when trying to split an EntsoeEvent into shorter events of an invalid duration."""