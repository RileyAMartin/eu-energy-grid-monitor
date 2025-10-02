class InvalidIntervalError(Exception):
    """Raised when the ENTSO-E API rejects the request's time interval."""
    pass

class NoDataFoundError(Exception):
    """Raised when the ENTSO-E API finds no API for the given code at the given time interval."""
    pass