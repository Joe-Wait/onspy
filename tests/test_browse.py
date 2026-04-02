"""Tests for URL helper functions in onspy.core."""

import pandas as pd

import onspy.core as core


def test_get_dev_url_constant():
    assert core.get_dev_url() == "https://developer.ons.gov.uk/"


def test_get_qmi_url_prefers_extracted_column(monkeypatch):
    datasets = pd.DataFrame(
        {
            "id": ["cpih01"],
            "qmi_href": ["https://example.com/qmi/cpih01"],
            "qmi": [{"href": "https://ignored.example.com"}],
        }
    )
    monkeypatch.setattr(core, "_validate_id", lambda dataset_id: datasets)

    assert core.get_qmi_url("cpih01") == "https://example.com/qmi/cpih01"


def test_get_qmi_url_falls_back_to_nested_qmi(monkeypatch):
    datasets = pd.DataFrame(
        {
            "id": ["cpih01"],
            "qmi_href": [""],
            "qmi": [{"href": "https://example.com/qmi/cpih01"}],
        }
    )
    monkeypatch.setattr(core, "_validate_id", lambda dataset_id: datasets)

    assert core.get_qmi_url("cpih01") == "https://example.com/qmi/cpih01"


def test_get_qmi_url_none_when_missing(monkeypatch):
    datasets = pd.DataFrame(
        {
            "id": ["cpih01"],
            "qmi_href": [""],
            "qmi": [None],
        }
    )
    monkeypatch.setattr(core, "_validate_id", lambda dataset_id: datasets)

    assert core.get_qmi_url("cpih01") is None
