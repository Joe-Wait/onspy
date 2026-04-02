"""Tests for data retrieval functions in onspy.core."""

from unittest.mock import Mock

import pandas as pd
import pytest

import onspy.core as core


def test_download_dataset_uses_csv_link(monkeypatch):
    expected = pd.DataFrame({"x": [1, 2], "y": ["a", "b"]})
    read_csv_mock = Mock(return_value=expected)

    monkeypatch.setattr(core, "_validate_id", lambda dataset_id: pd.DataFrame())
    monkeypatch.setattr(
        core,
        "_resolve_edition_version",
        lambda dataset_id, edition, version: ("time-series", "7"),
    )
    monkeypatch.setattr(
        core,
        "_get_dataset_definition",
        lambda *args, **kwargs: {"downloads": {"csv": {"href": "https://example.com/data.csv"}}},
    )
    monkeypatch.setattr(core, "read_csv", read_csv_mock)

    result = core.download_dataset("cpih01")

    assert result.equals(expected)
    read_csv_mock.assert_called_once_with("https://example.com/data.csv")


def test_get_dimensions_returns_names(monkeypatch):
    monkeypatch.setattr(core, "_validate_id", lambda dataset_id: pd.DataFrame())
    monkeypatch.setattr(
        core,
        "_resolve_edition_version",
        lambda dataset_id, edition, version: ("time-series", "1"),
    )
    monkeypatch.setattr(core, "make_request", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(
        core,
        "process_response",
        lambda response: {"items": [{"name": "geography"}, {"name": "time"}]},
    )

    assert core.get_dimensions("cpih01") == ["geography", "time"]


def test_get_dimension_options_raises_for_invalid_dimension(monkeypatch):
    monkeypatch.setattr(core, "_validate_id", lambda dataset_id: pd.DataFrame())
    monkeypatch.setattr(
        core,
        "_resolve_edition_version",
        lambda dataset_id, edition, version: ("time-series", "1"),
    )
    monkeypatch.setattr(core, "get_dimensions", lambda *args, **kwargs: ["time"])

    with pytest.raises(ValueError, match="Invalid dimension"):
        core.get_dimension_options("cpih01", "geography")


def test_get_dimension_options_returns_values(monkeypatch):
    monkeypatch.setattr(
        core,
        "get_dimension_options_detailed",
        lambda *args, **kwargs: [{"option": "2025"}, {"option": "2026"}],
    )

    result = core.get_dimension_options("cpih01", "time")

    assert result == ["2025", "2026"]


def test_get_dimension_options_detailed_returns_labels_and_links(monkeypatch):
    monkeypatch.setattr(core, "_validate_id", lambda dataset_id: pd.DataFrame())
    monkeypatch.setattr(
        core,
        "_resolve_edition_version",
        lambda dataset_id, edition, version: ("time-series", "1"),
    )
    monkeypatch.setattr(core, "get_dimensions", lambda *args, **kwargs: ["time"])
    monkeypatch.setattr(core, "make_request", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(
        core,
        "process_response",
        lambda response: {
            "items": [
                {
                    "option": "2025",
                    "label": "2025",
                    "dimension": "time",
                    "links": {
                        "code": {"id": "2025", "href": "https://example.com/code/2025"},
                        "code_list": {
                            "id": "time",
                            "href": "https://example.com/code-lists/time",
                        },
                    },
                }
            ]
        },
    )

    result = core.get_dimension_options_detailed("cpih01", "time")

    assert result[0]["option"] == "2025"
    assert result[0]["label"] == "2025"
    assert result[0]["code_id"] == "2025"
    assert result[0]["code_list_id"] == "time"


def test_get_observations_requires_all_dimensions(monkeypatch):
    monkeypatch.setattr(core, "_validate_id", lambda dataset_id: pd.DataFrame())
    monkeypatch.setattr(
        core,
        "_resolve_edition_version",
        lambda dataset_id, edition, version: ("time-series", "1"),
    )
    monkeypatch.setattr(core, "get_dimensions", lambda *args, **kwargs: ["time", "geo"])

    with pytest.raises(ValueError, match="Dimensions misspecified"):
        core.get_observations("cpih01", filters={"time": "*"})


def test_get_observations_api_only_requires_explicit_values(monkeypatch):
    monkeypatch.setattr(core, "_validate_id", lambda dataset_id: pd.DataFrame())
    monkeypatch.setattr(
        core,
        "_resolve_edition_version",
        lambda dataset_id, edition, version: ("time-series", "1"),
    )
    monkeypatch.setattr(core, "get_dimensions", lambda *args, **kwargs: ["time"])
    monkeypatch.setattr(core, "_get_dataset_definition", lambda *args, **kwargs: {})

    with pytest.raises(ValueError, match=r"Wildcard '\*' is only supported"):
        core.get_observations("cpih01", filters={"time": "*"})


def test_get_observations_api_only_returns_dataframe(monkeypatch):
    monkeypatch.setattr(core, "_validate_id", lambda dataset_id: pd.DataFrame())
    monkeypatch.setattr(
        core,
        "_resolve_edition_version",
        lambda dataset_id, edition, version: ("time-series", "1"),
    )
    monkeypatch.setattr(core, "get_dimensions", lambda *args, **kwargs: ["time"])
    monkeypatch.setattr(core, "_get_dataset_definition", lambda *args, **kwargs: {})
    monkeypatch.setattr(core, "make_request", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(
        core,
        "process_response",
        lambda response: {"observations": [{"time": "2025", "obs": "1.1"}]},
    )

    result = core.get_observations("cpih01", filters={"time": "2025"})

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result.iloc[0]["time"] == "2025"


def test_get_observations_table_backed_supports_wildcard(monkeypatch):
    table = pd.DataFrame(
        {
            "uk-only": ["K02000001", "K02000001", "W92000004"],
            "Geography": ["United Kingdom", "United Kingdom", "Wales"],
            "mmm-yy": ["Jan-26", "Dec-25", "Jan-26"],
            "Time": ["Jan-26", "Dec-25", "Jan-26"],
            "observation": [100.0, 101.0, 99.0],
        }
    )

    monkeypatch.setattr(core, "_validate_id", lambda dataset_id: pd.DataFrame())
    monkeypatch.setattr(
        core,
        "_resolve_edition_version",
        lambda dataset_id, edition, version: ("time-series", "67"),
    )
    monkeypatch.setattr(
        core,
        "get_dimensions",
        lambda *args, **kwargs: ["geography", "time"],
    )
    monkeypatch.setattr(
        core,
        "_get_dataset_definition",
        lambda *args, **kwargs: {"downloads": {"csv": {"href": "https://example.com/data.csv"}}},
    )
    monkeypatch.setattr(core, "read_csv", lambda *args, **kwargs: table)
    monkeypatch.setattr(
        core,
        "get_metadata",
        lambda *args, **kwargs: {
            "dimensions": [
                {"name": "geography", "id": "uk-only", "label": "Geography"},
                {"name": "time", "id": "mmm-yy", "label": "Time"},
            ]
        },
    )

    result = core.get_observations(
        "cpih01",
        filters={"geography": "K02000001", "time": "*"},
    )

    assert len(result) == 2
    assert set(result["uk-only"].tolist()) == {"K02000001"}


def test_get_metadata_returns_payload(monkeypatch):
    payload = {"contact": {"name": "ONS"}, "release_date": "2026-01-01"}

    monkeypatch.setattr(core, "_validate_id", lambda dataset_id: pd.DataFrame())
    monkeypatch.setattr(
        core,
        "_resolve_edition_version",
        lambda dataset_id, edition, version: ("time-series", "1"),
    )
    monkeypatch.setattr(core, "make_request", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(core, "process_response", lambda response: payload)

    assert core.get_metadata("cpih01") == payload
