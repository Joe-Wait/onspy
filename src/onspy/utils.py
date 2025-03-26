"""
Utilities for onspy package.

This module provides helper functions for data processing, and other
utilities used throughout the onspy package.
"""

import pandas as pd
import requests
from typing import Optional, Dict, Any
from io import StringIO
import logging

from .client import default_client as client
from .exceptions import ONSConnectionError, ONSRequestError

logger = logging.getLogger(__name__)

# Constants
EMPTY = ""
ENDPOINT = client.endpoint  # Expose client's endpoint to maintain backward compatibility


# Helper functions
def null_coalesce(x, y):
    """Python equivalent of R's %||% operator.

    Args:
        x: First value
        y: Default value if x is None

    Returns:
        x if x is not None, otherwise y
    """
    return y if x is None else x


def has_internet() -> bool:
    """Check if internet connection is available.

    Returns:
        bool: True if internet connection is available, False otherwise
    """
    return client.has_internet()


def set_endpoint(query: str) -> str:
    """Append query to the ONS API endpoint.

    Args:
        query: Query string to append to the endpoint

    Returns:
        Full URL with endpoint and query
    """
    return f"{client.endpoint}/{query}"


def build_request_dots(**params) -> str:
    """Build a request path from parameters.

    Args:
        **params: Key-value pairs for the request

    Returns:
        Formatted request path
    """
    param_chunks = []
    for key, value in params.items():
        if value is None:
            continue
        elif value == EMPTY:
            param_chunks.append(key)
        else:
            param_chunks.append(f"{key}/{value}")

    return "/".join([chunk for chunk in param_chunks if chunk != EMPTY])


def build_request(id, edition=None, version=None):
    """Build a request URL for a dataset.

    Args:
        id: Dataset ID
        edition: Edition name (optional)
        version: Version number (optional)

    Returns:
        Full request URL
    """
    return build_base_request(datasets=id, editions=edition, versions=version)


def build_base_request(**params) -> str:
    """Build a base request URL from parameters.

    Args:
        **params: Key-value pairs for the request

    Returns:
        Full request URL
    """
    return client.build_url(params)


def extend_request_dots(pre: str, **params) -> str:
    """Extend an existing request with additional parameters.

    Args:
        pre: Existing request URL
        **params: Additional parameters to append

    Returns:
        Extended request URL
    """
    append = build_request_dots(**params)
    return f"{pre}/{append}"


def make_request(
    query: str, limit: Optional[int] = None, offset: Optional[int] = None, **kwargs
) -> Optional[requests.Response]:
    """Make HTTP request to the ONS API.

    Args:
        query: Request URL
        limit: Number of records to return (optional)
        offset: Position in the dataset to start from (optional)
        **kwargs: Additional arguments to pass to requests.get

    Returns:
        Response object if successful, None otherwise
    """
    return client.make_request(query, limit, offset, **kwargs)


def process_response(response: requests.Response) -> Dict[str, Any]:
    """Process HTTP response and convert to JSON.

    Args:
        response: HTTP response object

    Returns:
        JSON content as dictionary
    """
    return client.process_response(response)


def get_browser_headers() -> Dict[str, str]:
    """Get browser-like headers for HTTP requests.

    Returns:
        Dictionary with browser-like headers
    """
    return client._get_browser_headers()


def read_csv(url: str, **kwargs) -> pd.DataFrame:
    """Read CSV from URL into pandas DataFrame.

    Args:
        url: URL to CSV file
        **kwargs: Additional arguments to pass to pandas.read_csv

    Returns:
        pandas DataFrame containing CSV data
    """
    logger.debug(f"Reading CSV from URL: {url}")

    try:
        # Try using browser headers to download the CSV
        if url:
            headers = get_browser_headers()

            logger.debug(f"Using browser headers to get CSV: {url}")

            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code == 200:
                logger.debug(
                    f"Successfully downloaded CSV (size: {len(response.content)})"
                )
                return pd.read_csv(StringIO(response.text), **kwargs)
            else:
                logger.debug(
                    f"Failed to get CSV with status code: {response.status_code}"
                )

        # Fall back to standard pandas read_csv if the above approach fails
        df = pd.read_csv(url, **kwargs)

        logger.debug(f"CSV loaded successfully. Shape: {df.shape}")

        return df
    except Exception as e:
        logger.error(f"Error reading CSV: {e}", exc_info=True)
        print(f"Error reading CSV: {e}")
        return pd.DataFrame()


def cat_ratio(x: Dict[str, Any]) -> None:
    """Print information about API results pagination.

    Args:
        x: API response dictionary
    """
    print(
        f"Fetched {x.get('count', 0)}/{x.get('total_count', 0)} "
        f"(limit = {x.get('limit', 'N/A')}, offset = {x.get('offset', 0)})"
    )


def cat_ratio_obs(x: Dict[str, Any]) -> None:
    """Print information about observations pagination.

    Args:
        x: API response dictionary
    """
    print(
        f"Fetched {len(x.get('observations', []))}/{x.get('total_observations', 0)} "
        f"(limit = {x.get('limit', 'N/A')}, offset = {x.get('offset', 0)})"
    )
