"""Tests for curated boundary helpers."""

from pathlib import Path
from unittest.mock import Mock

import onspy.boundaries as boundaries


def test_list_boundaries_contains_core_fields():
    items = boundaries.list_boundaries()

    assert len(items) >= 1
    first = items[0]
    for field in ["id", "title", "coverage", "year", "code_column", "name_column", "url"]:
        assert field in first


def test_download_boundary_skips_existing_file(tmp_path):
    target = tmp_path / "lad_2021_uk_bfc.geojson"
    target.write_text("{}", encoding="utf-8")

    result = boundaries.download_boundary(
        boundary_id="lad_2021_uk_bfc",
        output_dir=str(tmp_path),
        overwrite=False,
    )

    assert result["skipped"] is True
    assert Path(result["path"]).exists()


def test_download_boundary_fetches_remote_content(tmp_path, monkeypatch):
    mock_response = Mock()
    mock_response.content = b'{"type":"FeatureCollection","features":[]}'
    mock_response.raise_for_status = Mock()

    get_mock = Mock(return_value=mock_response)
    monkeypatch.setattr(boundaries.requests, "get", get_mock)

    result = boundaries.download_boundary(
        boundary_id="lad_2021_uk_bfc",
        output_dir=str(tmp_path),
        overwrite=True,
    )

    assert result["skipped"] is False
    assert Path(result["path"]).exists()
    assert Path(result["path"]).read_text(encoding="utf-8").startswith('{"type"')
    get_mock.assert_called_once()
