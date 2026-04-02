"""
ONS MCP Server - the single source of truth for all onspy operations.

This is the MCP server definition. Every tool defined here is:
1. Available to AI agents via MCP (stdio or HTTP)
2. Available to humans via the auto-generated CLI (fastmcp generate-cli)

Run as MCP server:
    fastmcp run src/onspy/server.py
    # or: onspy mcp

Generate human CLI:
    fastmcp generate-cli src/onspy/server.py onspy-cli.py
"""

from fastmcp import FastMCP
from typing import Optional, List, Dict, Any

try:
    from . import core
    from . import boundaries
    from . import parquet_sync
except ImportError:
    import onspy.core as core
    import onspy.boundaries as boundaries
    import onspy.parquet_sync as parquet_sync

mcp = FastMCP(
    "onspy",
    instructions=(
        "Access UK Office for National Statistics (ONS) data. "
        "Search datasets, download data, sync parquet files, download mapping boundaries, "
        "and explore dimensions and code lists."
    ),
)


# ============================================================================
# Dataset Tools
# ============================================================================


@mcp.tool
def list_datasets(
    limit: int | None = None,
) -> List[Dict[str, Any]]:
    """List all available ONS datasets with metadata.

    Returns datasets with id, title, description, keywords, release
    frequency, and publication state.
    """
    df = core.list_datasets(limit=limit)
    if df.empty:
        return []

    columns = ["id", "title", "description", "keywords", "release_frequency", "state"]
    available = [c for c in columns if c in df.columns]
    return df[available].to_dict(orient="records")


@mcp.tool
def get_dataset_ids() -> List[str]:
    """Get a simple list of all dataset IDs.

    Useful for scripting and discovering what datasets exist.
    """
    return core.get_dataset_ids()


@mcp.tool
def get_dataset_info(id: str) -> Dict[str, Any]:
    """Get detailed information about a specific dataset.

    Returns title, description, keywords, release frequency, state,
    next release date, latest version, and available editions.

    Args:
        id: Dataset ID (e.g., 'cpih01', 'weekly-deaths-region')
    """
    info = core.get_dataset_info(id)
    info["editions"] = core.get_editions(id)
    return info


@mcp.tool
def get_editions(id: str) -> List[str]:
    """List available editions for a dataset.

    Args:
        id: Dataset ID
    """
    return core.get_editions(id)


@mcp.tool
def find_latest_version(id: str) -> Dict[str, str]:
    """Find the latest version across ALL editions of a dataset.

    Some datasets have multiple editions (e.g., 'time-series', 'covid-19').
    This checks all editions and returns the one with the highest version.

    Args:
        id: Dataset ID
    """
    result = core.find_latest_version_across_editions(id)
    if result is None:
        raise ValueError(f"Could not find latest version for '{id}'")
    edition, version = result
    return {"id": id, "edition": edition, "version": version}


# ============================================================================
# Data Retrieval Tools
# ============================================================================


@mcp.tool
def download_dataset(
    id: str,
    edition: str | None = None,
    version: str | None = None,
    preview_rows: int = 10,
) -> Dict[str, Any]:
    """Download a dataset from ONS.

    Automatically finds the latest version if edition/version not specified.
    Returns metadata, column names, and a preview of the first N rows.

    Args:
        id: Dataset ID
        edition: Edition name (auto-detects if not provided)
        version: Version number (auto-detects if not provided)
        preview_rows: Number of rows to include in preview
    """
    edition, version = core._resolve_edition_version(id, edition, version)
    df = core.download_dataset(id, edition, version)

    if df.empty:
        raise ValueError(f"Downloaded dataset is empty for {id}")

    return {
        "dataset_id": id,
        "edition": edition,
        "version": version,
        "total_rows": len(df),
        "columns": list(df.columns),
        "preview": df.head(preview_rows).to_dict(orient="records"),
    }


@mcp.tool
def download_all_parquet(
    output_dir: str = "ons_datasets",
    resume: bool = False,
    delay: float = 2.0,
) -> Dict[str, Any]:
    """Download every available ONS dataset as parquet.

    Saves one parquet file per dataset in output_dir and writes a
    manifest.json summary for downstream tooling.

    Args:
        output_dir: Target directory for parquet files.
        resume: Skip datasets that already exist in output_dir.
        delay: Seconds to wait between dataset downloads.
    """
    return parquet_sync.download_all_parquet(
        output_dir=output_dir,
        resume=resume,
        delay=delay,
    )


@mcp.tool
def download_datasets_parquet(
    dataset_ids: List[str],
    output_dir: str = "ons_datasets",
    resume: bool = False,
    delay: float = 2.0,
) -> Dict[str, Any]:
    """Download specific ONS datasets as parquet.

    Useful when you only need a subset for local DuckDB analysis.

    Args:
        dataset_ids: List of dataset IDs to download.
        output_dir: Target directory for parquet files.
        resume: Skip datasets that already exist in output_dir.
        delay: Seconds to wait between dataset downloads.
    """
    return parquet_sync.download_datasets_parquet(
        dataset_ids=dataset_ids,
        output_dir=output_dir,
        resume=resume,
        delay=delay,
    )


@mcp.tool
def list_boundaries() -> List[Dict[str, Any]]:
    """List curated geography boundary files for choropleth mapping.

    Returns IDs, year, coverage, code column, name column, and source URL.
    """
    return boundaries.list_boundaries()


@mcp.tool
def download_boundary(
    boundary_id: str,
    output_dir: str = "ons_boundaries",
    overwrite: bool = False,
) -> Dict[str, Any]:
    """Download a curated boundary GeoJSON file.

    Args:
        boundary_id: Boundary ID from list_boundaries()
        output_dir: Directory to save GeoJSON file
        overwrite: Overwrite if file already exists
    """
    return boundaries.download_boundary(
        boundary_id=boundary_id,
        output_dir=output_dir,
        overwrite=overwrite,
    )


@mcp.tool
def get_dimensions(id: str) -> List[str]:
    """List available dimensions (filterable columns) for a dataset.

    Dimensions are categorical columns you can filter by when querying
    observations (e.g., 'geography', 'time', 'aggregate').

    Args:
        id: Dataset ID
    """
    return core.get_dimensions(id)


@mcp.tool
def get_dimension_options(
    id: str,
    dimension: str,
    limit: int | None = None,
) -> List[str]:
    """Get available values for a specific dimension.

    Use this to discover valid filter values before calling get_observations.
    For example, get all geography codes or time periods.

    Args:
        id: Dataset ID
        dimension: Dimension name (e.g., 'geography', 'time')
        limit: Maximum options to return
    """
    return core.get_dimension_options(id, dimension, limit=limit)


@mcp.tool
def get_dimension_options_detailed(
    id: str,
    dimension: str,
    limit: int | None = None,
) -> List[Dict[str, Any]]:
    """Get dimension options with labels and code-list links.

    Returns option value plus human-readable label and associated
    code/code-list identifiers when available.

    Args:
        id: Dataset ID
        dimension: Dimension name (e.g., 'geography', 'time')
        limit: Maximum options to return
    """
    return core.get_dimension_options_detailed(id, dimension, limit=limit)


@mcp.tool
def get_observations(
    id: str,
    filters: Dict[str, str],
    edition: str | None = None,
    version: str | None = None,
) -> Dict[str, Any]:
    """Get filtered observations from a dataset.

    All dimensions must be specified.

    Wildcard '*' is supported for table-backed datasets (those with downloadable
    CSV tables). API-only datasets require explicit values for each dimension.

    Call get_dimensions() first to see required filters, then
    get_dimension_options() to see valid values.

    Example filters: {"geography": "K02000001", "aggregate": "cpih1dim1A0", "time": "*"}

    Args:
        id: Dataset ID
        filters: Dictionary mapping dimension names to filter values
        edition: Edition name (auto-detects if not provided)
        version: Version number (auto-detects if not provided)
    """
    df = core.get_observations(id, filters, edition, version)

    if df.empty:
        return {"observations": [], "total": 0}

    return {
        "observations": df.to_dict(orient="records"),
        "total": len(df),
    }


@mcp.tool
def get_metadata(
    id: str,
    edition: str | None = None,
    version: str | None = None,
) -> Dict[str, Any]:
    """Get metadata for a dataset version.

    Includes contact details, methodology, dimension descriptions, etc.

    Args:
        id: Dataset ID
        edition: Edition name (auto-detects if not provided)
        version: Version number (auto-detects if not provided)
    """
    return core.get_metadata(id, edition, version)


# ============================================================================
# Search Tools
# ============================================================================


@mcp.tool
def search_dataset(
    id: str,
    dimension: str,
    query: str,
    edition: str | None = None,
    version: str | None = None,
) -> List[Dict[str, Any]]:
    """Search for values within a dataset dimension.

    Useful for finding specific codes or categories when you know
    the dataset but not the exact dimension value.

    Args:
        id: Dataset ID
        dimension: Dimension name to search (e.g., 'aggregate')
        query: Search query string
        edition: Edition name (auto-detects if not provided)
        version: Version number (auto-detects if not provided)
    """
    return core.search_dataset(id, dimension, query, edition, version)


# ============================================================================
# Code List Tools
# ============================================================================


@mcp.tool
def list_codelists() -> List[str]:
    """List all available code lists (standardized categories).

    Code lists provide standardized values for dimensions across datasets,
    such as geography codes, time periods, or classifications.
    """
    return core.list_codelists()


@mcp.tool
def get_codelist_info(code_id: str) -> Dict[str, Any]:
    """Get detailed information about a code list.

    Args:
        code_id: Code list ID (e.g., 'quarter', 'geography')
    """
    return core.get_codelist_info(code_id)


@mcp.tool
def get_codelist_editions(code_id: str) -> List[Dict[str, Any]]:
    """Get available editions for a code list.

    Args:
        code_id: Code list ID
    """
    return core.get_codelist_editions(code_id)


@mcp.tool
def get_codes(code_id: str, edition: str) -> List[Dict[str, Any]]:
    """Get codes for a specific code list edition.

    Returns the standardized values and their meanings.

    Args:
        code_id: Code list ID
        edition: Edition name
    """
    return core.get_codes(code_id, edition)


@mcp.tool
def get_code_info(code_id: str, edition: str, code: str) -> Dict[str, Any]:
    """Get details for a specific code.

    Args:
        code_id: Code list ID
        edition: Edition name
        code: Code value
    """
    return core.get_code_info(code_id, edition, code)


# ============================================================================
# Browser / URL Tools
# ============================================================================


@mcp.tool
def get_dev_url() -> str:
    """Get the ONS developer documentation URL."""
    return core.get_dev_url()


@mcp.tool
def get_qmi_url(id: str) -> str:
    """Get the Quality and Methodology Information (QMI) URL for a dataset.

    Args:
        id: Dataset ID
    """
    url = core.get_qmi_url(id)
    if url is None:
        raise ValueError(f"No QMI URL available for '{id}'")
    return url


# ============================================================================
# Resources
# ============================================================================


@mcp.resource("ons://developer-docs")
def developer_docs_resource() -> str:
    """ONS Developer Documentation URL."""
    return core.get_dev_url()


# ============================================================================
# Prompts
# ============================================================================


@mcp.prompt
def explore_dataset(id: str) -> str:
    """Guide through exploring a dataset: info, dimensions, preview."""
    return f"""I'd like to explore the ONS dataset '{id}'.

Please help me:
1. Get the dataset info to understand what it contains
2. List the available dimensions (filterable columns)
3. Show me the dimension options for key dimensions
4. Download a preview of the data
5. Suggest what analyses might be interesting"""


@mcp.prompt
def filter_observations(id: str) -> str:
    """Guide through filtering observations from a dataset."""
    return f"""I need to get specific observations from the '{id}' dataset.

To help me filter effectively:
1. First, what dimensions are available in this dataset?
2. What are the options for each dimension?
3. What time periods are available?

Once I know the available values, I'll tell you which filters to apply."""
