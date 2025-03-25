"""
Search for datasets.

This module provides functions for searching data within the ONS API.
"""

from typing import Optional, List, Dict, Any

from .utils import (
    null_coalesce,
    build_base_request,
    make_request,
    process_response,
    EMPTY,
)
from .datasets import ons_latest_edition, ons_latest_version, assert_valid_id


def ons_search(
    id: str,
    edition: str = None,
    version: str = None,
    name: str = None,
    query: str = None,
) -> Optional[List[Dict[str, Any]]]:
    """Search for a dataset.

    Args:
        id: Dataset ID
        edition: A subset of the dataset representing a specific time period
        version: A specific instance of the edition at a point in time
        name: The name of dimension to perform the query
        query: The query string to search for

    Returns:
        List of matching items, or None if the request fails

    Examples:
        >>> import onspy
        >>> ons_search("cpih01", name="aggregate", query="cpih1dim1A0")
    """
    # Validate ID
    assert_valid_id(id)

    # Get latest edition and version if not provided
    edition = null_coalesce(edition, ons_latest_edition(id))
    version = null_coalesce(version, ons_latest_version(id))

    if edition is None or version is None:
        raise ValueError("You must specify an 'edition' and 'version'.")

    if name is None:
        raise ValueError("You must specify a dimension 'name'.")

    if query is None:
        raise ValueError("You must specify a 'query'.")

    # Build the request URL
    base = build_base_request(
        **{
            "dimension-search": EMPTY,
            "datasets": id,
            "editions": edition,
            "versions": version,
            "dimensions": name,
        }
    )

    req = f"{base}?q={query}"

    res = make_request(req)
    if res is None:
        return None

    raw = process_response(res)

    # Return the items list or an empty list
    return raw.get("items", [])
