"""
Browser functionality for ONS.

This module provides functions to quickly open ONS webpages in a browser.
"""

import webbrowser
from typing import Optional

from .datasets import ons_datasets, assert_valid_id, id_number


def ons_browse() -> str:
    """Quickly browse to ONS' developer webpage.

    This function opens the ONS developer webpage in a browser.

    Returns:
        The URL of the webpage

    Examples:
        >>> import onspy
        >>> onspy.ons_browse()
    """
    url = "https://developer.ons.gov.uk/"
    _open_url(url)
    return url


def ons_browse_qmi(id: str = None) -> Optional[str]:
    """Quickly browse to dataset's Quality and Methodology Information (QMI).

    This function opens the QMI webpage for a dataset in a browser.

    Args:
        id: Dataset ID

    Returns:
        The URL of the webpage, or None if the dataset is not found

    Examples:
        >>> import onspy
        >>> onspy.ons_browse_qmi("cpih01")
    """
    datasets = ons_datasets()
    if datasets is None:
        return None

    if not assert_valid_id(id, datasets):
        return None

    idx = id_number(id, datasets)

    # Handle nested dictionary
    try:
        if hasattr(datasets.iloc[idx], "qmi") and hasattr(
            datasets.iloc[idx].qmi, "href"
        ):
            url = datasets.iloc[idx].qmi.href
        elif isinstance(datasets.iloc[idx].get("qmi", {}), dict):
            url = datasets.iloc[idx]["qmi"].get("href", None)
        else:
            return None

        if url:
            _open_url(url)
            return url
        return None
    except (AttributeError, KeyError, IndexError):
        return None


def _open_url(url: str, open_browser: bool = True) -> str:
    """Open a URL in the default browser.

    Args:
        url: The URL to open
        open_browser: Whether to actually open the browser (default: True)

    Returns:
        The URL
    """
    if open_browser:
        webbrowser.open(url)
    return url
