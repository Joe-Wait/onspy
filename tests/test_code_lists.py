"""Tests for code list functions in onspy.core."""

from unittest.mock import Mock

import onspy.core as core


def test_list_codelists_returns_ids(monkeypatch):
    monkeypatch.setattr(core, "make_request", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(
        core,
        "process_response",
        lambda response: {
            "items": [
                {"links": {"self": {"id": "geography"}}},
                {"links": {"self": {"id": "quarter"}}},
            ]
        },
    )

    assert core.list_codelists() == ["geography", "quarter"]


def test_get_codelist_info_returns_payload(monkeypatch):
    payload = {"id": "geography", "description": "areas"}

    monkeypatch.setattr(core, "_validate_codelist", lambda code_id: True)
    monkeypatch.setattr(core, "make_request", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(core, "process_response", lambda response: payload)

    assert core.get_codelist_info("geography") == payload


def test_get_codelist_editions_returns_items(monkeypatch):
    payload = {"items": [{"edition": "one-off"}, {"edition": "latest"}]}

    monkeypatch.setattr(core, "_validate_codelist", lambda code_id: True)
    monkeypatch.setattr(core, "make_request", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(core, "process_response", lambda response: payload)

    assert core.get_codelist_editions("geography") == payload["items"]


def test_get_codes_returns_items(monkeypatch):
    payload = {"items": [{"code": "K02000001", "label": "United Kingdom"}]}

    monkeypatch.setattr(core, "_validate_codelist", lambda code_id: True)
    monkeypatch.setattr(
        core, "_validate_codelist_edition", lambda code_id, edition: True
    )
    monkeypatch.setattr(core, "make_request", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(core, "process_response", lambda response: payload)

    result = core.get_codes("geography", "one-off")

    assert result[0]["code"] == "K02000001"


def test_get_code_info_returns_payload(monkeypatch):
    payload = {"code": "K02000001", "label": "United Kingdom"}

    monkeypatch.setattr(core, "_validate_codelist", lambda code_id: True)
    monkeypatch.setattr(
        core, "_validate_codelist_edition", lambda code_id, edition: True
    )
    monkeypatch.setattr(core, "make_request", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(core, "process_response", lambda response: payload)

    result = core.get_code_info("geography", "one-off", "K02000001")

    assert result == payload
