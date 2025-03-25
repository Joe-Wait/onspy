"""
ONS Datasets module.

This module provides functions to access and interact with ONS datasets.
A dataset is a grouping of data (editions) with shared dimensions, for example Sex,
Age and Geography, and all published history of this group of data.
"""

import pandas as pd
from typing import Optional, List, Tuple

import logging
from .utils import (
    build_base_request,
    make_request,
    process_response,
    EMPTY,
)

logger = logging.getLogger(__name__)


def ons_datasets() -> Optional[pd.DataFrame]:
    """Get information about all available ONS datasets.

    Returns:
        pandas DataFrame with dataset information, or None if the request fails

    Examples:
        >>> import onspy
        >>> onspy.ons_datasets()
    """
    req = build_base_request(datasets=EMPTY)
    res = make_request(req, limit=60)
    if res is None:
        return None

    raw = process_response(res)

    # Convert the items to a DataFrame
    df = pd.DataFrame(raw.get("items", []))

    # Extract specific nested fields instead of trying to convert entire nested dictionaries
    if "links" in df.columns:
        logger.debug("Processing links column")

        # Extract the latest_version href and id separately
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

        logger.debug("Successfully extracted links data")

    if "qmi" in df.columns:
        logger.debug("Processing qmi column")

        # Extract the QMI href
        df["qmi_href"] = df["qmi"].apply(
            lambda x: x.get("href", "") if isinstance(x, dict) else ""
        )

        logger.debug("Successfully extracted qmi data")

    return df


def ons_ids() -> Optional[List[str]]:
    """Get a list of all available dataset IDs.

    Returns:
        List of dataset IDs, or None if the request fails

    Examples:
        >>> import onspy
        >>> onspy.ons_ids()
    """
    logger.debug("Getting dataset IDs")

    datasets = ons_datasets()
    if datasets is None:
        logger.debug("No datasets found")
        return None

    ids = datasets["id"].tolist()

    logger.debug(f"Found {len(ids)} dataset IDs")

    return ids


def assert_valid_id(id: str, ons_ds: Optional[pd.DataFrame] = None) -> bool:
    """Check if a dataset ID is valid.

    Args:
        id: Dataset ID to check
        ons_ds: Optional DataFrame with dataset information (to avoid making another API call)

    Returns:
        True if the ID is valid, raises ValueError otherwise
    """
    if ons_ds is None:
        ids = ons_ids()
        if ids is None:
            return False
    else:
        ids = ons_ds["id"].tolist()

    if id is None:
        raise ValueError("You must specify an 'id', see ons_ids()")

    if id not in ids:
        raise ValueError(f"Invalid 'id': {id}. See ons_ids() for valid IDs.")

    return True


def ons_desc(id: str) -> None:
    """Print a description of the specified dataset.

    Args:
        id: Dataset ID to describe

    Examples:
        >>> import onspy
        >>> onspy.ons_desc("cpih01")
    """
    datasets = ons_datasets()
    if datasets is None:
        return None

    if not assert_valid_id(id, datasets):
        return None

    # Get the row for the specified ID
    line = datasets[datasets["id"] == id].iloc[0]

    print(f"Title: {line.get('title', '')}")
    print(f"ID: {id}")

    keywords = line.get("keywords", [])
    if isinstance(keywords, list) and len(keywords) > 0:
        print(f"Keywords: {', '.join(keywords)}")

    print("-----------")
    print(f"Description: {line.get('description', '')}")
    print("-----------")
    print(f"Release Frequency: {line.get('release_frequency', '')}")
    print(f"State: {line.get('state', '')}")
    print(f"Next Release: {line.get('next_release', '')}")
    print("-----------")

    # Use the extracted field instead of trying to access nested dictionaries
    if "latest_version_id" in datasets.columns:
        print(f"Latest Version: {line.get('latest_version_id', '')}")

    editions = ons_editions(id)
    if editions:
        print(f"Edition(s): {', '.join(editions)}")


def ons_editions(id: str) -> Optional[List[str]]:
    """Get available editions for a dataset.

    Args:
        id: Dataset ID

    Returns:
        List of edition names, or None if the request fails

    Examples:
        >>> import onspy
        >>> onspy.ons_editions("cpih01")
    """
    req = build_base_request(datasets=id, editions=EMPTY)
    res = make_request(req)
    if res is None:
        return None

    raw = process_response(res)

    return [item.get("edition", "") for item in raw.get("items", [])]


def id_number(id: str, ons_ds: Optional[pd.DataFrame] = None) -> int:
    """Get the row index of a dataset ID in the datasets DataFrame.

    Args:
        id: Dataset ID
        ons_ds: Optional DataFrame with dataset information

    Returns:
        Row index of the dataset
    """
    if ons_ds is None:
        ons_ds = ons_datasets()

    return ons_ds.index[ons_ds["id"] == id].tolist()[0]


# Latest info functions
def ons_latest_href(id: str) -> Optional[str]:
    """Get the latest href for a dataset.

    Args:
        id: Dataset ID

    Returns:
        URL for the latest version of the dataset, or None if not found

    Examples:
        >>> import onspy
        >>> onspy.ons_latest_href("cpih01")
    """
    datasets = ons_datasets()
    if datasets is None:
        return None

    if not assert_valid_id(id, datasets):
        return None

    idx = id_number(id, datasets)

    # Use the extracted field if available
    if "latest_version_href" in datasets.columns:
        return datasets.iloc[idx].get("latest_version_href", None)

    # Fall back to original method if needed
    try:
        links = datasets.iloc[idx].get("links", {})
        if isinstance(links, dict) and "latest_version" in links:
            return links["latest_version"].get("href", None)
        return None
    except (IndexError, KeyError, AttributeError):
        return None


def ons_latest_version(id: str) -> Optional[str]:
    """Get the latest version number for a dataset.

    Args:
        id: Dataset ID

    Returns:
        Latest version number, or None if not found

    Examples:
        >>> import onspy
        >>> onspy.ons_latest_version("cpih01")
    """
    href = ons_latest_href(id)
    if href is None:
        return None

    # Extract version from href using string manipulation
    import re

    match = re.search(r"versions/(.+)", href)
    if match:
        return match.group(1)
    return None


def ons_latest_edition(id: str) -> Optional[str]:
    """Get the latest edition name for a dataset.

    Args:
        id: Dataset ID

    Returns:
        Latest edition name, or None if not found

    Examples:
        >>> import onspy
        >>> onspy.ons_latest_edition("cpih01")
    """
    href = ons_latest_href(id)
    if href is None:
        return None

    # Extract edition from href using string manipulation
    import re

    match = re.search(r"editions/(.+)/versions", href)
    if match:
        return match.group(1)
    return None


def ons_find_latest_version_across_editions(id: str) -> Optional[Tuple[str, str]]:
    """Find the latest version across all editions of a dataset.

    This function examines all editions of a dataset and returns the
    edition and version with the highest version number.

    Args:
        id: Dataset ID

    Returns:
        Tuple containing the edition name and version number, or None if not found

    Examples:
        >>> import onspy
        >>> edition, version = onspy.ons_find_latest_version_across_editions("weekly-deaths-region")
        >>> print(f"Latest version is {version} in edition {edition}")
    """
    # Get all editions for this dataset
    editions = ons_editions(id)
    if not editions:
        logger.debug(f"No editions found for dataset {id}")
        return None

    logger.debug(f"Found {len(editions)} editions for dataset {id}: {editions}")

    latest_edition = None
    latest_version = "0"  # Start with version 0

    # For each edition, get the latest version info
    for edition in editions:
        req = build_base_request(datasets=id, editions=edition)
        res = make_request(req)
        if res is None:
            continue

        raw = process_response(res)

        # Get the latest version href from this edition
        if "links" in raw and "latest_version" in raw["links"]:
            version_id = raw["links"]["latest_version"].get("id", "")

            logger.debug(f"Edition {edition} has latest version: {version_id}")

            # Check if this version is higher than our current highest
            try:
                if int(version_id) > int(latest_version):
                    latest_version = version_id
                    latest_edition = edition
                    logger.debug(
                        f"Found new highest version: {latest_version} in edition {latest_edition}"
                    )
            except (ValueError, TypeError):
                # If we can't convert to int, just compare as strings
                if version_id > latest_version:
                    latest_version = version_id
                    latest_edition = edition
                    logger.debug(
                        f"Found new highest version: {latest_version} in edition {latest_edition}"
                    )

    if latest_edition is None or latest_version == "0":
        logger.debug(f"No valid versions found across {len(editions)} editions")
        return None

    logger.debug(
        f"Determined highest version: {latest_version} in edition {latest_edition}"
    )

    return (latest_edition, latest_version)
