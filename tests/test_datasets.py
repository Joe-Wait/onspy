"""Tests for dataset discovery functions in onspy.core."""

from unittest.mock import Mock

import pandas as pd

import onspy.core as core


def test_list_datasets_extracts_nested_fields(monkeypatch):
    mock_response = Mock()
    payload = {
        "items": [
            {
                "id": "cpih01",
                "title": "CPIH",
                "description": "Inflation index",
                "keywords": ["inflation"],
                "release_frequency": "Monthly",
                "state": "published",
                "links": {
                    "latest_version": {
                        "href": "https://example.com/datasets/cpih01/versions/1",
                        "id": "1",
                    }
                },
                "qmi": {"href": "https://example.com/qmi/cpih01"},
            }
        ]
    }

    monkeypatch.setattr(core, "make_request", lambda *args, **kwargs: mock_response)
    monkeypatch.setattr(core, "process_response", lambda response: payload)

    df = core.list_datasets()

    assert isinstance(df, pd.DataFrame)
    assert list(df["id"]) == ["cpih01"]
    assert df["latest_version_href"].iloc[0].endswith("/versions/1")
    assert df["latest_version_id"].iloc[0] == "1"
    assert df["qmi_href"].iloc[0].endswith("/qmi/cpih01")


def test_get_dataset_ids_returns_values(monkeypatch):
    monkeypatch.setattr(
        core,
        "_get_datasets_cached",
        lambda: pd.DataFrame({"id": ["cpih01", "weekly-deaths-region"]}),
    )

    assert core.get_dataset_ids() == ["cpih01", "weekly-deaths-region"]


def test_get_dataset_info_maps_expected_fields(monkeypatch):
    datasets = pd.DataFrame(
        {
            "id": ["cpih01"],
            "title": ["CPIH"],
            "description": ["Inflation"],
            "keywords": [["inflation", "prices"]],
            "release_frequency": ["Monthly"],
            "state": ["published"],
            "next_release": ["2026-01-01"],
            "latest_version_id": ["42"],
        }
    )
    monkeypatch.setattr(core, "_validate_id", lambda dataset_id: datasets)

    result = core.get_dataset_info("cpih01")

    assert result["id"] == "cpih01"
    assert result["title"] == "CPIH"
    assert result["latest_version"] == "42"
    assert "inflation" in result["keywords"]


def test_get_editions_returns_edition_names(monkeypatch):
    mock_response = Mock()
    payload = {"items": [{"edition": "time-series"}, {"edition": "annual"}]}

    monkeypatch.setattr(core, "_validate_id", lambda dataset_id: pd.DataFrame())
    monkeypatch.setattr(core, "make_request", lambda *args, **kwargs: mock_response)
    monkeypatch.setattr(core, "process_response", lambda response: payload)

    assert core.get_editions("cpih01") == ["time-series", "annual"]


def test_find_latest_version_across_editions_chooses_highest(monkeypatch):
    monkeypatch.setattr(core, "_validate_id", lambda dataset_id: pd.DataFrame())
    monkeypatch.setattr(core, "get_editions", lambda dataset_id: ["a", "b", "c"])

    responses = [
        {"links": {"latest_version": {"id": "3"}}},
        {"links": {"latest_version": {"id": "9"}}},
        {"links": {"latest_version": {"id": "4"}}},
    ]
    response_iter = iter(responses)

    monkeypatch.setattr(core, "make_request", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(core, "process_response", lambda response: next(response_iter))

    assert core.find_latest_version_across_editions("cpih01") == ("b", "9")
