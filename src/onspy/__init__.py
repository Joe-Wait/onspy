"""
onspy: Python client for the Office of National Statistics (ONS) API

This package provides client functions for accessing the Office of National Statistics API
at https://api.beta.ons.gov.uk/v1.

Usage as library:
    import onspy
    df = onspy.list_datasets()
    info = onspy.get_dataset_info("cpih01")
    data = onspy.download_dataset("cpih01")
    sync = onspy.download_all_parquet(output_dir="ons_datasets")

Usage as CLI:
    onspy call-tool list_datasets --limit 10
    onspy call-tool get_dataset_info --id cpih01
    onspy mcp  # start MCP server for AI agents
"""

import logging
import os

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

if os.environ.get("ONS_DEBUG", "").lower() in ("1", "true", "yes"):
    logger.setLevel(logging.DEBUG)
    for handler in logger.handlers:
        handler.setLevel(logging.DEBUG)
    logger.debug("Debug logging enabled for onspy")

from .core import (
    list_datasets,
    get_dataset_ids,
    get_dataset_info,
    get_editions,
    find_latest_version_across_editions,
    download_dataset,
    get_dimensions,
    get_dimension_options,
    get_dimension_options_detailed,
    get_observations,
    get_metadata,
    search_dataset,
    list_codelists,
    get_codelist_info,
    get_codelist_editions,
    get_codes,
    get_code_info,
    get_dev_url,
    get_qmi_url,
    invalidate_cache,
)
from .parquet_sync import download_all_parquet, download_datasets_parquet
from .boundaries import list_boundaries, download_boundary

__version__ = "0.2.2"
__author__ = "Joe Wait"
