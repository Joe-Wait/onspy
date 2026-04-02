"""Integration-style tests for package exports and MCP wrappers."""

from unittest.mock import Mock
import sys
import types

import pandas as pd

import onspy


if "fastmcp" not in sys.modules:
    fastmcp_module = types.ModuleType("fastmcp")

    class _FakeFastMCP:
        def __init__(self, *args, **kwargs):
            pass

        def tool(self, fn):
            return fn

        def resource(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

        def prompt(self, fn):
            return fn

    fastmcp_module.FastMCP = _FakeFastMCP
    sys.modules["fastmcp"] = fastmcp_module

import onspy.server as server


def test_package_exports_include_refactored_api():
    assert callable(onspy.list_datasets)
    assert callable(onspy.download_dataset)
    assert callable(onspy.download_all_parquet)
    assert callable(onspy.download_datasets_parquet)
    assert callable(onspy.get_dimension_options_detailed)
    assert callable(onspy.list_boundaries)
    assert callable(onspy.download_boundary)


def test_server_download_dataset_returns_preview(monkeypatch):
    df = pd.DataFrame(
        [
            {"time": "2025", "value": 1.1},
            {"time": "2026", "value": 1.3},
        ]
    )

    monkeypatch.setattr(
        server.core,
        "_resolve_edition_version",
        lambda dataset_id, edition, version: ("time-series", "5"),
    )
    monkeypatch.setattr(server.core, "download_dataset", lambda *args, **kwargs: df)

    result = server.download_dataset("cpih01", preview_rows=1)

    assert result["dataset_id"] == "cpih01"
    assert result["edition"] == "time-series"
    assert result["version"] == "5"
    assert result["total_rows"] == 2
    assert len(result["preview"]) == 1


def test_server_get_observations_wraps_dataframe(monkeypatch):
    df = pd.DataFrame([{"time": "2025", "value": "1.1"}])
    monkeypatch.setattr(server.core, "get_observations", lambda *args, **kwargs: df)

    result = server.get_observations("cpih01", filters={"time": "*"})

    assert result["total"] == 1
    assert result["observations"][0]["time"] == "2025"


def test_server_download_all_parquet_proxies_to_sync_module(monkeypatch):
    summary = {"requested_count": 10, "succeeded_count": 10}
    call_mock = Mock(return_value=summary)
    monkeypatch.setattr(server.parquet_sync, "download_all_parquet", call_mock)

    result = server.download_all_parquet(output_dir="tmp_data", resume=True, delay=0.5)

    assert result == summary
    call_mock.assert_called_once_with(output_dir="tmp_data", resume=True, delay=0.5)


def test_server_download_datasets_parquet_proxies_to_sync_module(monkeypatch):
    summary = {"requested_count": 2, "succeeded_count": 2}
    call_mock = Mock(return_value=summary)
    monkeypatch.setattr(server.parquet_sync, "download_datasets_parquet", call_mock)

    result = server.download_datasets_parquet(
        dataset_ids=["cpih01", "weekly-deaths-region"],
        output_dir="tmp_data",
        resume=False,
        delay=0,
    )

    assert result == summary
    call_mock.assert_called_once_with(
        dataset_ids=["cpih01", "weekly-deaths-region"],
        output_dir="tmp_data",
        resume=False,
        delay=0,
    )


def test_server_get_dimension_options_detailed(monkeypatch):
    payload = [{"option": "K02000001", "label": "United Kingdom"}]
    call_mock = Mock(return_value=payload)
    monkeypatch.setattr(server.core, "get_dimension_options_detailed", call_mock)

    result = server.get_dimension_options_detailed("cpih01", "geography", limit=5)

    assert result == payload
    call_mock.assert_called_once_with("cpih01", "geography", limit=5)


def test_server_list_boundaries_proxies_module(monkeypatch):
    payload = [{"id": "lad_2021_uk_bfc"}]
    call_mock = Mock(return_value=payload)
    monkeypatch.setattr(server.boundaries, "list_boundaries", call_mock)

    result = server.list_boundaries()

    assert result == payload
    call_mock.assert_called_once_with()


def test_server_download_boundary_proxies_module(monkeypatch):
    payload = {"boundary_id": "lad_2021_uk_bfc", "path": "/tmp/x.geojson"}
    call_mock = Mock(return_value=payload)
    monkeypatch.setattr(server.boundaries, "download_boundary", call_mock)

    result = server.download_boundary(
        boundary_id="lad_2021_uk_bfc",
        output_dir="tmp_boundaries",
        overwrite=True,
    )

    assert result == payload
    call_mock.assert_called_once_with(
        boundary_id="lad_2021_uk_bfc",
        output_dir="tmp_boundaries",
        overwrite=True,
    )
