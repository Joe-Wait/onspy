"""Curated geographic boundary helpers for mapping workflows."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import requests


BOUNDARIES: Dict[str, Dict[str, Any]] = {
    "lad_2021_uk_bfc": {
        "id": "lad_2021_uk_bfc",
        "title": "Local Authority Districts (December 2021) Boundaries UK BFC",
        "coverage": "UK",
        "year": 2021,
        "code_column": "LAD21CD",
        "name_column": "LAD21NM",
        "source": "ONS ArcGIS Open Data",
        "url": (
            "https://opendata.arcgis.com/api/v3/datasets/"
            "52b4dd427ca54be8a2f7fa1abeb264d6/downloads/data"
            "?format=geojson&spatialRefId=4326"
        ),
    },
    "lad_2023_uk_bfc": {
        "id": "lad_2023_uk_bfc",
        "title": "Local Authority Districts (December 2023) Boundaries UK BFC",
        "coverage": "UK",
        "year": 2023,
        "code_column": "LAD23CD",
        "name_column": "LAD23NM",
        "source": "ONS ArcGIS Open Data",
        "url": (
            "https://opendata.arcgis.com/api/v3/datasets/"
            "127c4bda06314409a1fa0df505f510e6/downloads/data"
            "?format=geojson&spatialRefId=4326"
        ),
    },
}


def list_boundaries() -> List[Dict[str, Any]]:
    """List curated boundary files available for download."""
    return [BOUNDARIES[key] for key in sorted(BOUNDARIES.keys())]


def download_boundary(
    boundary_id: str,
    output_dir: str = "ons_boundaries",
    overwrite: bool = False,
    timeout_seconds: int = 180,
) -> Dict[str, Any]:
    """Download a curated boundary GeoJSON file.

    Args:
        boundary_id: Boundary ID from list_boundaries().
        output_dir: Directory to save downloaded GeoJSON.
        overwrite: Overwrite existing file if present.
        timeout_seconds: HTTP timeout for download.
    """
    if boundary_id not in BOUNDARIES:
        valid = ", ".join(sorted(BOUNDARIES.keys()))
        raise ValueError(f"Unknown boundary_id '{boundary_id}'. Valid: {valid}")

    boundary = BOUNDARIES[boundary_id]
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    filename = f"{boundary_id}.geojson"
    path = out / filename

    if path.exists() and not overwrite:
        return {
            "boundary_id": boundary_id,
            "path": str(path.resolve()),
            "skipped": True,
            "bytes": path.stat().st_size,
            "code_column": boundary["code_column"],
            "name_column": boundary["name_column"],
            "coverage": boundary["coverage"],
            "year": boundary["year"],
        }

    response = requests.get(boundary["url"], timeout=timeout_seconds)
    response.raise_for_status()
    path.write_bytes(response.content)

    return {
        "boundary_id": boundary_id,
        "path": str(path.resolve()),
        "skipped": False,
        "bytes": len(response.content),
        "code_column": boundary["code_column"],
        "name_column": boundary["name_column"],
        "coverage": boundary["coverage"],
        "year": boundary["year"],
    }
