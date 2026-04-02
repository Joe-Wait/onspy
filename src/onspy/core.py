"""
Core business logic for onspy.

This module provides the pure functions used by both CLI and MCP server.
All functions return values only (no prints). Auto-detection of edition/version
is handled consistently via _resolve_edition_version().
"""

import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .utils import (
    build_base_request,
    build_request,
    make_request,
    process_response,
    read_csv,
    EMPTY,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Dataset Cache (avoids redundant HTTP calls within a session)
# ============================================================================

_datasets_cache: Dict[str, Any] = {"df": None, "timestamp": 0.0}
_CACHE_TTL = 60  # seconds


def _get_datasets_cached() -> pd.DataFrame:
    """Fetch datasets with a TTL cache to avoid redundant API calls.

    Returns:
        DataFrame with dataset information (may be empty)
    """
    now = time.time()
    if (
        _datasets_cache["df"] is not None
        and (now - _datasets_cache["timestamp"]) < _CACHE_TTL
    ):
        return _datasets_cache["df"]

    req = build_base_request(datasets=EMPTY)
    res = make_request(req, limit=1000)
    raw = process_response(res)
    df = pd.DataFrame(raw.get("items", []))

    # Extract nested fields
    if "links" in df.columns:
        df["latest_version_href"] = df["links"].apply(
            lambda x: (
                x.get("latest_version", {}).get("href", "")
                if isinstance(x, dict)
                else ""
            )
        )
        df["latest_version_id"] = df["links"].apply(
            lambda x: (
                x.get("latest_version", {}).get("id", "") if isinstance(x, dict) else ""
            )
        )

    if "qmi" in df.columns:
        df["qmi_href"] = df["qmi"].apply(
            lambda x: x.get("href", "") if isinstance(x, dict) else ""
        )

    _datasets_cache["df"] = df
    _datasets_cache["timestamp"] = now
    return df


def invalidate_cache() -> None:
    """Clear the datasets cache (useful for testing or long-running sessions)."""
    _datasets_cache["df"] = None
    _datasets_cache["timestamp"] = 0.0


# ============================================================================
# Validation Helpers
# ============================================================================


def _validate_id(id: str, datasets: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Validate a dataset ID and return the datasets DataFrame.

    This avoids the pattern of calling assert_valid_id() then list_datasets()
    separately, which would be two HTTP requests.

    Args:
        id: Dataset ID to validate
        datasets: Pre-fetched datasets DataFrame (avoids extra API call)

    Returns:
        The datasets DataFrame (for reuse)

    Raises:
        ValueError: If ID is None or not found
    """
    if id is None:
        raise ValueError("You must specify an 'id'")

    if datasets is None:
        datasets = _get_datasets_cached()

    if datasets.empty:
        raise ValueError("Could not fetch datasets from ONS API")

    if id not in datasets["id"].values:
        raise ValueError(f"Invalid 'id': {id}. Use list_datasets() to see valid IDs.")

    return datasets


def _resolve_edition_version(
    id: str,
    edition: Optional[str] = None,
    version: Optional[str] = None,
) -> Tuple[str, str]:
    """Resolve edition and version, auto-detecting via cross-edition scan if needed.

    This is the single source of truth for auto-detection, used by all
    functions that need edition/version. Uses find_latest_version_across_editions()
    so behavior is consistent whether called from CLI, MCP, or library.

    Args:
        id: Dataset ID (must already be validated)
        edition: Edition name (None = auto-detect)
        version: Version number (None = auto-detect)

    Returns:
        Tuple of (edition, version)

    Raises:
        ValueError: If auto-detection fails
    """
    if edition is not None and version is not None:
        return edition, version

    result = find_latest_version_across_editions(id)
    if result is None:
        raise ValueError(f"Could not determine latest version for '{id}'")

    return result


# ============================================================================
# Dataset Functions
# ============================================================================


def list_datasets(limit: Optional[int] = None) -> pd.DataFrame:
    """Get all available ONS datasets with metadata.

    Args:
        limit: Maximum number of datasets to return

    Returns:
        DataFrame with dataset information
    """
    df = _get_datasets_cached()
    if limit and not df.empty:
        return df.head(limit)
    return df


def get_dataset_ids() -> List[str]:
    """Get list of all dataset IDs.

    Returns:
        List of dataset ID strings
    """
    datasets = _get_datasets_cached()
    if datasets.empty:
        return []
    return datasets["id"].tolist()


def get_dataset_info(id: str) -> Dict[str, Any]:
    """Get detailed information about a dataset.

    Args:
        id: Dataset ID

    Returns:
        Dictionary with dataset metadata
    """
    datasets = _validate_id(id)
    row = datasets[datasets["id"] == id].iloc[0]

    keywords = row.get("keywords", [])
    if not isinstance(keywords, list):
        keywords = []

    return {
        "id": id,
        "title": row.get("title", ""),
        "description": row.get("description", ""),
        "keywords": keywords,
        "release_frequency": row.get("release_frequency", ""),
        "state": row.get("state", ""),
        "next_release": row.get("next_release", ""),
        "latest_version": row.get("latest_version_id", ""),
    }


def get_editions(id: str) -> List[str]:
    """Get available editions for a dataset.

    Args:
        id: Dataset ID

    Returns:
        List of edition names
    """
    _validate_id(id)

    req = build_base_request(datasets=id, editions=EMPTY)
    res = make_request(req)
    raw = process_response(res)
    return [item.get("edition", "") for item in raw.get("items", [])]


def find_latest_version_across_editions(id: str) -> Optional[Tuple[str, str]]:
    """Find the latest version across ALL editions of a dataset.

    This is crucial for datasets like weekly-deaths-region where the
    latest version might be in a different edition than the default.

    Args:
        id: Dataset ID

    Returns:
        Tuple of (edition, version) or None
    """
    _validate_id(id)

    editions = get_editions(id)
    if not editions:
        return None

    latest_edition = None
    latest_version_num = -1

    for edition in editions:
        req = build_base_request(datasets=id, editions=edition)
        res = make_request(req)
        raw = process_response(res)

        if "links" in raw and "latest_version" in raw["links"]:
            version_id = raw["links"]["latest_version"].get("id", "")

            try:
                ver_num = int(version_id)
            except (ValueError, TypeError):
                ver_num = -1

            if ver_num > latest_version_num:
                latest_version_num = ver_num
                latest_edition = edition

    if latest_edition is None or latest_version_num < 0:
        return None

    return (latest_edition, str(latest_version_num))


# ============================================================================
# Data Retrieval Functions
# ============================================================================


def _get_dataset_definition(id: str, edition: str, version: str) -> Dict[str, Any]:
    """Get dataset definition payload for a resolved id/edition/version."""
    req = build_request(id, edition, version)
    return process_response(make_request(req))


def _find_matching_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    """Find a matching DataFrame column using exact then case-insensitive lookup."""
    if not columns:
        return None

    by_lower = {col.lower(): col for col in columns}
    for candidate in candidates:
        if not candidate:
            continue
        if candidate in columns:
            return candidate
        lowered = candidate.lower()
        if lowered in by_lower:
            return by_lower[lowered]
    return None


def _normalize_filter_values(value: Any) -> List[str]:
    """Normalize a filter value into a list of non-empty strings."""
    if isinstance(value, (list, tuple, set)):
        values = [str(v).strip() for v in value if v is not None and str(v).strip()]
    elif value is None:
        values = []
    else:
        text = str(value).strip()
        values = [text] if text else []
    return values


def _build_dimension_column_map(
    table: pd.DataFrame,
    available_dims: List[str],
    dimensions_meta: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Optional[str]]]:
    """Infer code/label columns in downloaded tables for each dimension."""
    columns = list(table.columns)
    meta_by_name = {
        d.get("name", ""): d for d in dimensions_meta if isinstance(d, dict) and d.get("name")
    }

    mapping: Dict[str, Dict[str, Optional[str]]] = {}
    for dim in available_dims:
        meta = meta_by_name.get(dim, {})
        dim_id = str(meta.get("id") or "").strip()
        label = str(meta.get("label") or "").strip()

        code_candidates = [dim_id, dim]
        label_candidates = [label]

        if label:
            code_candidates.append(f"{label} Code")

        code_col = _find_matching_column(columns, code_candidates)
        label_col = _find_matching_column(columns, label_candidates)

        mapping[dim] = {
            "code_column": code_col,
            "label_column": label_col,
            "dimension_id": dim_id or None,
            "dimension_label": label or None,
        }

    return mapping


def _filter_table_observations(
    table: pd.DataFrame,
    available_dims: List[str],
    filters: Dict[str, Any],
    dimension_map: Dict[str, Dict[str, Optional[str]]],
) -> pd.DataFrame:
    """Apply dimension filters to a downloaded table."""
    filtered = table.copy()

    for dim in available_dims:
        values = _normalize_filter_values(filters.get(dim))
        if not values:
            raise ValueError(f"Dimension '{dim}' must include at least one filter value")

        if "*" in values:
            continue

        cols = dimension_map.get(dim, {})
        code_col = cols.get("code_column")
        label_col = cols.get("label_column")

        if not code_col and not label_col:
            raise ValueError(
                f"Could not map dimension '{dim}' to downloaded table columns"
            )

        values_set = {v.strip() for v in values}
        values_lower = {v.lower() for v in values_set}

        mask = pd.Series(False, index=filtered.index)
        if code_col:
            series = filtered[code_col].astype(str).str.strip()
            mask = mask | series.isin(values_set) | series.str.lower().isin(values_lower)
        if label_col and label_col != code_col:
            series = filtered[label_col].astype(str).str.strip()
            mask = mask | series.isin(values_set) | series.str.lower().isin(values_lower)

        filtered = filtered[mask]

    return filtered.reset_index(drop=True)


def _get_observations_via_api(
    id: str,
    edition: str,
    version: str,
    available_dims: List[str],
    filters: Dict[str, Any],
) -> pd.DataFrame:
    """Fetch observations through the ONS observations endpoint."""
    chunks: List[str] = []
    for dim in available_dims:
        values = _normalize_filter_values(filters.get(dim))
        if not values:
            raise ValueError(f"Dimension '{dim}' must include exactly one filter value")
        if "*" in values:
            raise ValueError(
                "Wildcard '*' is only supported when the dataset has a downloadable CSV table"
            )
        if len(values) != 1:
            raise ValueError(
                "API-only datasets require exactly one value per dimension"
            )
        chunks.append(f"{dim}={values[0]}")

    obs_params = "&".join(chunks)
    base = build_request(id, edition, version)
    req = f"{base}/observations?{obs_params}"
    raw = process_response(make_request(req))
    return pd.DataFrame(raw.get("observations", []))


def download_dataset(
    id: str,
    edition: Optional[str] = None,
    version: Optional[str] = None,
) -> pd.DataFrame:
    """Download a dataset.

    Args:
        id: Dataset ID
        edition: Edition name (auto-detects across all editions if None)
        version: Version number (auto-detects if None)

    Returns:
        DataFrame with dataset
    """
    _validate_id(id)
    edition, version = _resolve_edition_version(id, edition, version)

    raw = _get_dataset_definition(id, edition, version)
    csv_url = raw.get("downloads", {}).get("csv", {}).get("href", None)

    if csv_url:
        return read_csv(csv_url)

    return pd.DataFrame()


def get_dimensions(
    id: str,
    edition: Optional[str] = None,
    version: Optional[str] = None,
) -> List[str]:
    """Get dimension names for a dataset.

    Args:
        id: Dataset ID
        edition: Edition name (auto-detects if None)
        version: Version number (auto-detects if None)

    Returns:
        List of dimension names
    """
    _validate_id(id)
    edition, version = _resolve_edition_version(id, edition, version)

    req = build_request(id, edition, version)
    req = f"{req}/dimensions"

    raw = process_response(make_request(req))

    if "items" in raw and isinstance(raw["items"], list):
        return [item.get("name", "") for item in raw["items"]]
    return []


def get_dimension_options_detailed(
    id: str,
    dimension: str,
    edition: Optional[str] = None,
    version: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Get detailed option records for a specific dimension.

    Returns option values along with labels and code/code-list links.
    """
    _validate_id(id)
    edition, version = _resolve_edition_version(id, edition, version)

    available = get_dimensions(id, edition, version)
    if dimension not in available:
        raise ValueError(
            f"Invalid dimension '{dimension}'. Available: {', '.join(available)}"
        )

    req = build_request(id, edition, version)
    req = f"{req}/dimensions/{dimension}/options"
    raw = process_response(make_request(req, limit=limit, offset=offset))

    details: List[Dict[str, Any]] = []
    for item in raw.get("items", []):
        links = item.get("links", {}) if isinstance(item, dict) else {}
        code = links.get("code", {}) if isinstance(links, dict) else {}
        code_list = links.get("code_list", {}) if isinstance(links, dict) else {}

        details.append(
            {
                "option": item.get("option", ""),
                "label": item.get("label", ""),
                "dimension": item.get("dimension", dimension),
                "code_id": code.get("id", "") if isinstance(code, dict) else "",
                "code_href": code.get("href", "") if isinstance(code, dict) else "",
                "code_list_id": (
                    code_list.get("id", "") if isinstance(code_list, dict) else ""
                ),
                "code_list_href": (
                    code_list.get("href", "") if isinstance(code_list, dict) else ""
                ),
            }
        )

    return details


def get_dimension_options(
    id: str,
    dimension: str,
    edition: Optional[str] = None,
    version: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[str]:
    """Get options for a specific dimension.

    Args:
        id: Dataset ID
        dimension: Dimension name
        edition: Edition name (auto-detects if None)
        version: Version number (auto-detects if None)
        limit: Maximum options to return
        offset: Starting position

    Returns:
        List of option values
    """
    detailed = get_dimension_options_detailed(
        id=id,
        dimension=dimension,
        edition=edition,
        version=version,
        limit=limit,
        offset=offset,
    )
    return [item.get("option", "") for item in detailed]


def get_observations(
    id: str,
    filters: Dict[str, Any],
    edition: Optional[str] = None,
    version: Optional[str] = None,
) -> pd.DataFrame:
    """Get filtered observations from a dataset.

    Args:
        id: Dataset ID
        filters: Dictionary of dimension filters.
            For table-backed datasets (with CSV download), '*' is supported.
            For API-only datasets, wildcard is not supported.
        edition: Edition name (auto-detects if None)
        version: Version number (auto-detects if None)

    Returns:
        DataFrame with observations
    """
    _validate_id(id)
    edition, version = _resolve_edition_version(id, edition, version)

    # Build observation query
    available_dims = get_dimensions(id, edition, version)
    param_names = list(filters.keys())

    if available_dims and not all(dim in param_names for dim in available_dims):
        raise ValueError(
            f"Dimensions misspecified. Required: {', '.join(available_dims)}. "
            f"Got: {', '.join(param_names)}"
        )

    if not available_dims and not filters:
        raise ValueError("No dimensions available and no filters provided")

    extras = [dim for dim in param_names if dim not in available_dims]
    if extras:
        raise ValueError(
            f"Unknown dimensions in filters: {', '.join(extras)}. "
            f"Available: {', '.join(available_dims)}"
        )

    dataset_definition = _get_dataset_definition(id, edition, version)
    csv_url = dataset_definition.get("downloads", {}).get("csv", {}).get("href", "")

    if csv_url:
        table = read_csv(csv_url)
        if table.empty:
            raise ValueError(f"Downloaded dataset table is empty for '{id}'")

        metadata = get_metadata(id, edition, version)
        dimensions_meta = metadata.get("dimensions", [])
        dimension_map = _build_dimension_column_map(
            table, available_dims, dimensions_meta
        )
        return _filter_table_observations(table, available_dims, filters, dimension_map)

    return _get_observations_via_api(id, edition, version, available_dims, filters)


def get_metadata(
    id: str,
    edition: Optional[str] = None,
    version: Optional[str] = None,
) -> Dict[str, Any]:
    """Get metadata for a dataset.

    Args:
        id: Dataset ID
        edition: Edition name (auto-detects if None)
        version: Version number (auto-detects if None)

    Returns:
        Dictionary with metadata
    """
    _validate_id(id)
    edition, version = _resolve_edition_version(id, edition, version)

    req = build_request(id, edition, version)
    req = f"{req}/metadata"

    return process_response(make_request(req))


# ============================================================================
# Code List Functions
# ============================================================================


def list_codelists() -> List[str]:
    """Get list of all code list IDs.

    Returns:
        List of code list ID strings
    """
    req = build_base_request(**{"code-lists": EMPTY})
    raw = process_response(make_request(req, limit=80))

    try:
        return [item["links"]["self"]["id"] for item in raw.get("items", [])]
    except (KeyError, TypeError):
        return []


def _validate_codelist(code_id: str) -> bool:
    """Validate code list ID.

    Raises:
        ValueError: If invalid
    """
    if code_id is None:
        raise ValueError("You must specify a 'code_id'")

    ids = list_codelists()
    if code_id not in ids:
        raise ValueError(
            f"Invalid code_id '{code_id}'. Use list_codelists() to see valid IDs."
        )

    return True


def _validate_codelist_edition(code_id: str, edition: str) -> bool:
    """Validate edition for a code list.

    Raises:
        ValueError: If invalid
    """
    if edition is None:
        raise ValueError("You must specify an 'edition'")

    editions = get_codelist_editions(code_id)
    edition_names = [e.get("edition", "") for e in editions]
    if edition not in edition_names:
        raise ValueError(
            f"Invalid edition '{edition}'. Valid: {', '.join(edition_names)}"
        )

    return True


def get_codelist_info(code_id: str) -> Dict[str, Any]:
    """Get details for a code list.

    Args:
        code_id: Code list ID

    Returns:
        Dictionary with code list details
    """
    _validate_codelist(code_id)

    req = build_base_request(**{"code-lists": code_id})
    return process_response(make_request(req))


def get_codelist_editions(code_id: str) -> List[Dict[str, Any]]:
    """Get editions for a code list.

    Args:
        code_id: Code list ID

    Returns:
        List of edition dictionaries
    """
    _validate_codelist(code_id)

    req = build_base_request(**{"code-lists": code_id, "editions": EMPTY})
    raw = process_response(make_request(req))
    return raw.get("items", [])


def get_codes(code_id: str, edition: str) -> List[Dict[str, Any]]:
    """Get codes for a specific code list edition.

    Args:
        code_id: Code list ID
        edition: Edition name

    Returns:
        List of code dictionaries
    """
    _validate_codelist(code_id)
    _validate_codelist_edition(code_id, edition)

    req = build_base_request(
        **{"code-lists": code_id, "editions": edition, "codes": EMPTY}
    )
    raw = process_response(make_request(req))
    return raw.get("items", [])


def get_code_info(code_id: str, edition: str, code: str) -> Dict[str, Any]:
    """Get details for a specific code.

    Args:
        code_id: Code list ID
        edition: Edition name
        code: Code value

    Returns:
        Dictionary with code details
    """
    _validate_codelist(code_id)
    _validate_codelist_edition(code_id, edition)

    if code is None:
        raise ValueError("You must specify a 'code'")

    req = build_base_request(
        **{"code-lists": code_id, "editions": edition, "codes": code}
    )
    return process_response(make_request(req))


# ============================================================================
# Search Functions
# ============================================================================


def search_dataset(
    id: str,
    dimension: str,
    query: str,
    edition: Optional[str] = None,
    version: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Search within a dataset dimension.

    Args:
        id: Dataset ID
        dimension: Dimension name to search
        query: Search query string
        edition: Edition name (auto-detects if None)
        version: Version number (auto-detects if None)

    Returns:
        List of matching items
    """
    # Validate inputs before making any API calls
    if dimension is None:
        raise ValueError("You must specify a dimension")
    if query is None:
        raise ValueError("You must specify a query")

    _validate_id(id)
    edition, version = _resolve_edition_version(id, edition, version)

    base = build_base_request(
        **{
            "dimension-search": EMPTY,
            "datasets": id,
            "editions": edition,
            "versions": version,
            "dimensions": dimension,
        }
    )

    req = f"{base}?q={query}"

    raw = process_response(make_request(req))
    return raw.get("items", [])


# ============================================================================
# Browser Functions (return URLs, don't open)
# ============================================================================


def get_dev_url() -> str:
    """Get ONS developer documentation URL."""
    return "https://developer.ons.gov.uk/"


def get_qmi_url(id: str) -> Optional[str]:
    """Get QMI URL for a dataset.

    Args:
        id: Dataset ID

    Returns:
        URL string or None
    """
    datasets = _validate_id(id)
    row = datasets[datasets["id"] == id].iloc[0]

    # Try extracted qmi_href column first
    qmi_href = row.get("qmi_href", "")
    if qmi_href:
        return qmi_href

    # Fall back to nested dictionary
    qmi = row.get("qmi", {})
    if isinstance(qmi, dict):
        return qmi.get("href", None)

    return None
