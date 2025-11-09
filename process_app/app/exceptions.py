class InvalidEventDurationError(Exception):
    """Raised when trying to split an EntsoeEvent into shorter events of an invalid duration."""

class InvalidPsrTypeCodeError(Exception):
    """Raised when trying to get the PSR details for a PSR type which isn't stored in the mapping."""

class InvalidEicCodeError(Exception):
    """Raised when trying to get the EIC details for an EIC code which isn't stored in the mapping."""