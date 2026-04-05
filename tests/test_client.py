"""Tests for low-level ONS client behavior."""

from unittest.mock import Mock

from onspy.client import ONSClient


def test_make_request_stream_does_not_read_content(monkeypatch):
    client = ONSClient(endpoint="https://api.example.com")
    monkeypatch.setattr(client, "has_internet", lambda: True)

    response = Mock()
    response.status_code = 200
    response.raise_for_status.return_value = None

    class _UnreadableContent:
        def __len__(self):
            raise AssertionError("response.content should not be accessed for stream=True")

    response.content = _UnreadableContent()

    get_mock = Mock(return_value=response)
    monkeypatch.setattr(client._session, "get", get_mock)

    result = client.make_request("https://api.example.com/file.csv", stream=True)

    assert result is response
    get_mock.assert_called_once()
