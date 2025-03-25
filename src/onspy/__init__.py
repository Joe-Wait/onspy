"""
onspy: Python client for the Office of National Statistics (ONS) API

This package provides client functions for accessing the Office of National Statistics API
at https://api.beta.ons.gov.uk/v1.
"""

import logging
import os

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add console handler if not already added
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# Enable debug logging if ONS_DEBUG is set
if os.environ.get("ONS_DEBUG", "").lower() in ("1", "true", "yes"):
    logger.setLevel(logging.DEBUG)
    for handler in logger.handlers:
        handler.setLevel(logging.DEBUG)
    logger.debug("Debug logging enabled for onspy")

from .datasets import (
    ons_datasets,
    ons_ids,
    ons_desc,
    ons_editions,
    ons_latest_href,
    ons_latest_version,
    ons_latest_edition,
    ons_find_latest_version_across_editions,
)
from .get import ons_get, ons_get_obs, ons_dim, ons_dim_opts, ons_meta
from .code_lists import (
    ons_codelists,
    ons_codelist,
    ons_codelist_editions,
    ons_codelist_edition,
    ons_codes,
    ons_code,
    ons_code_dataset,
)
from .search import ons_search
from .browse import ons_browse, ons_browse_qmi

__version__ = "0.1.0"
__author__ = "Joe Wait"
