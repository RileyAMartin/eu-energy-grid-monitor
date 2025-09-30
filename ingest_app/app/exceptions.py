class InvalidIntervalError(Exception):
    """Raised when the ENTSO-E API rejects the request's time interval."""
    pass