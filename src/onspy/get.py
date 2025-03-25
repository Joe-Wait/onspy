"""
Download data from ONS.

This module provides functions to download data from the Office of National Statistics API,
including functions to get datasets, observations, dimensions, and metadata.
"""

import pandas as pd
from typing import Optional, Dict, Any, List

from .utils import (
    null_coalesce,
    build_request,
    make_request,
    process_response,
    read_csv,
    cat_ratio,
    cat_ratio_obs,
)
from .datasets import ons_latest_edition, ons_latest_version, assert_valid_id


def ons_get(
    id: str = None, edition: str = None, version: str = None, **kwargs
) -> Optional[pd.DataFrame]:
    """Download data from ONS.

    This function is used to find information about data published by the ONS.
    'Datasets' are published in unique 'versions', which are categorized by 'edition'.
    Available datasets are given an 'id'. All available 'id' can be viewed with ons_ids().

    Args:
        id: Dataset ID
        edition: A subset of the dataset representing a specific time period
        version: A specific instance of the edition at a point in time
        **kwargs: Additional arguments to pass to pandas.read_csv

    Returns:
        pandas DataFrame with the dataset, or None if the request fails

    Examples:
        >>> import onspy
        >>> onspy.ons_get(id="cpih01")
        >>> # Same dataset but older version
        >>> onspy.ons_get(id="cpih01", version="5")
    """
    if id is None:
        raise ValueError("You must specify an 'id'")

    assert_valid_id(id)

    # Use the latest edition and version if not specified
    edition = null_coalesce(edition, ons_latest_edition(id))
    version = null_coalesce(version, ons_latest_version(id))

    req = build_request(id, edition, version)
    res = make_request(req)
    if res is None:
        return None

    raw = process_response(res)

    # Download the CSV
    return read_csv(raw.get("downloads", {}).get("csv", {}).get("href", None), **kwargs)


def ons_get_obs(
    id: str = None, edition: str = None, version: str = None, **kwargs
) -> Optional[pd.DataFrame]:
    """Get specific observations from a dataset.

    Args:
        id: Dataset ID
        edition: A subset of the dataset representing a specific time period
        version: A specific instance of the edition at a point in time
        **kwargs: Key-value pairs for filtering dimensions (e.g., geography='K02000001', time='*')

    Returns:
        pandas DataFrame with the observations, or None if the request fails

    Examples:
        >>> import onspy
        >>> # Take only specific observations
        >>> onspy.ons_get_obs("cpih01", geography="K02000001", aggregate="cpih1dim1A0", time="Oct-11")
        >>> # Or can use a wildcard for the time
        >>> onspy.ons_get_obs("cpih01", geography="K02000001", aggregate="cpih1dim1A0", time="*")
    """
    if id is None:
        raise ValueError("You must specify an 'id'")

    assert_valid_id(id)

    # Use the latest edition and version if not specified
    edition = null_coalesce(edition, ons_latest_edition(id))
    version = null_coalesce(version, ons_latest_version(id))

    base = build_request(id, edition, version)
    obs_params = build_request_obs(id, **kwargs)
    req = f"{base}/observations?{obs_params}"

    res = make_request(req)
    if res is None:
        return None

    raw = process_response(res)
    cat_ratio_obs(raw)

    # Convert observations to a DataFrame
    if "observations" in raw:
        return pd.DataFrame(raw["observations"])
    return pd.DataFrame()


def build_request_obs(id: str, **params) -> str:
    """Build request parameters for observations.

    Args:
        id: Dataset ID
        **params: Key-value pairs for filtering dimensions

    Returns:
        URL query parameters as string

    Raises:
        ValueError: If dimensions are misspecified
    """
    # Get all available dimensions for this dataset
    all_dims = ons_dim(id)
    param_names = list(params.keys())

    # Check if all required dimensions are specified
    if all_dims and not all(dim in param_names for dim in all_dims):
        raise ValueError(
            f"The dimensions have been misspecified. Available dimensions are: {', '.join(all_dims)}"
        )

    # In testing environments, we might get empty dimensions, so skip validation if so
    if not all_dims and not params:
        raise ValueError("No dimensions available and no parameters provided")

    # Build query parameters
    param_chunks = []
    for key, value in params.items():
        # Use only the first value for each parameter
        if isinstance(value, (list, tuple)) and len(value) > 0:
            value = value[0]
        param_chunks.append(f"{key}={value}")

    return "&".join(param_chunks)


def ons_dim(id: str = None, edition: str = None, version: str = None) -> List[str]:
    """Get dimensions for a dataset.

    Args:
        id: Dataset ID
        edition: A subset of the dataset representing a specific time period
        version: A specific instance of the edition at a point in time

    Returns:
        List of dimension names, or empty list if the request fails

    Examples:
        >>> import onspy
        >>> onspy.ons_dim(id="cpih01")
    """
    if id is None:
        raise ValueError("You must specify an 'id'")

    assert_valid_id(id)

    # Use the latest edition and version if not specified
    edition = null_coalesce(edition, ons_latest_edition(id))
    version = null_coalesce(version, ons_latest_version(id))

    req = build_request(id, edition, version)
    req = f"{req}/dimensions"

    res = make_request(req)
    if res is None:
        return []

    raw = process_response(res)

    # Extract dimension names
    if "items" in raw and isinstance(raw["items"], list):
        return [item.get("name", "") for item in raw["items"]]
    return []


def ons_dim_opts(
    id: str = None,
    edition: str = None,
    version: str = None,
    dimension: str = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[str]:
    """Get dimension options for a dataset.

    Args:
        id: Dataset ID
        edition: A subset of the dataset representing a specific time period
        version: A specific instance of the edition at a point in time
        dimension: The name of the dimension to get options for
        limit: Number of records to return
        offset: Position in the dataset to start from

    Returns:
        List of dimension option values, or empty list if the request fails

    Examples:
        >>> import onspy
        >>> onspy.ons_dim_opts(id="cpih01", dimension="time")
    """
    if id is None:
        raise ValueError("You must specify an 'id'")

    assert_valid_id(id)

    if dimension is None:
        raise ValueError("'dimension' cannot be None")

    # Check if dimension is valid
    available_dims = ons_dim(id)
    if dimension not in available_dims:
        raise ValueError(
            f"The 'dimension' argument is misspecified. Available dimensions are: {', '.join(available_dims)}"
        )

    # Use the latest edition and version if not specified
    edition = null_coalesce(edition, ons_latest_edition(id))
    version = null_coalesce(version, ons_latest_version(id))

    req = build_request(id, edition, version)
    req = f"{req}/dimensions/{dimension}/options"

    res = make_request(req, limit=limit, offset=offset)
    if res is None:
        return []

    raw = process_response(res)
    cat_ratio(raw)

    # Extract option values
    if "items" in raw and isinstance(raw["items"], list):
        return [item.get("option", "") for item in raw["items"]]
    return []


def ons_meta(
    id: str = None, edition: str = None, version: str = None
) -> Optional[Dict[str, Any]]:
    """Get metadata for a dataset.

    Args:
        id: Dataset ID
        edition: A subset of the dataset representing a specific time period
        version: A specific instance of the edition at a point in time

    Returns:
        Dictionary with metadata, or None if the request fails

    Examples:
        >>> import onspy
        >>> onspy.ons_meta(id="cpih01")
    """
    if id is None:
        raise ValueError("You must specify an 'id'")

    assert_valid_id(id)

    # Use the latest edition and version if not specified
    edition = null_coalesce(edition, ons_latest_edition(id))
    version = null_coalesce(version, ons_latest_version(id))

    req = build_request(id, edition, version)
    req = f"{req}/metadata"

    res = make_request(req)
    if res is None:
        return None

    return process_response(res)
