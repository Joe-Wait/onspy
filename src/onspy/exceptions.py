"""
Exceptions for the onspy package.

This module provides custom exceptions used throughout the package to provide more
specific error information and consistent error handling.
"""


class ONSError(Exception):
    """Base class for all ONS exceptions."""

    pass


class ONSConnectionError(ONSError):
    """Raised when there's an issue connecting to the ONS API."""

    pass


class ONSRequestError(ONSError):
    """Raised when an HTTP request to the ONS API fails."""

    def __init__(self, message: str, status_code: int = None):
        """Initialize ONSRequestError.

        Args:
            message: Error message
            status_code: HTTP status code if available
        """
        self.status_code = status_code
        super().__init__(message)


class ONSResourceNotFoundError(ONSRequestError):
    """Raised when a requested resource is not found on the ONS API."""

    def __init__(self, resource_type: str, resource_id: str):
        """Initialize ONSResourceNotFoundError.

        Args:
            resource_type: Type of resource (dataset, code list, etc.)
            resource_id: ID of the resource
        """
        self.resource_type = resource_type
        self.resource_id = resource_id
        message = f"{resource_type} with ID '{resource_id}' not found"
        super().__init__(message, status_code=404)


class ONSParameterError(ONSError):
    """Raised when invalid parameters are provided to a function."""

    pass
