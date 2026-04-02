"""Tests for search behavior in onspy.core."""

from unittest.mock import Mock

import pandas as pd
import pytest

import onspy.core as core


def test_search_dataset_returns_items(monkeypatch):
    monkeypatch.setattr(core, "_validate_id", lambda dataset_id: pd.DataFrame())
    monkeypatch.setattr(
        core,
        "_resolve_edition_version",
        lambda dataset_id, edition, version: ("time-series", "3"),
    )
    monkeypatch.setattr(core, "make_request", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(
        core,
        "process_response",
        lambda response: {
            "items": [
                {"id": "cpih1dim1A0", "label": "All items"},
                {"id": "cpih1dim1A1", "label": "Food"},
            ]
        },
    )

    result = core.search_dataset("cpih01", dimension="aggregate", query="food")

    assert len(result) == 2
    assert result[1]["label"] == "Food"


def test_search_dataset_requires_dimension():
    with pytest.raises(ValueError, match="dimension"):
        core.search_dataset("cpih01", dimension=None, query="food")


def test_search_dataset_requires_query():
    with pytest.raises(ValueError, match="query"):
        core.search_dataset("cpih01", dimension="aggregate", query=None)
