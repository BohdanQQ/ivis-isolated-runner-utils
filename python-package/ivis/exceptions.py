class IvisException(Exception):
    """Base class for exceptions in this package."""
    pass


class RequestException(IvisException):
    """Exception raised for request errors."""


class TimeoutException(IvisException):
    """Exception raised when there is timeout on input reading."""
