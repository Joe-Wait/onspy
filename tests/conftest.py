"""
Pytest configuration file.

This module contains fixtures and configuration for pytest.
"""

import os
import pytest
from unittest.mock import Mock

# Set DEBUG to False during tests
os.environ["ONS_DEBUG"] = "0"


@pytest.fixture
def mock_response():
    """Create a mock HTTP response."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {"items": []}
    return response


@pytest.fixture
def mock_empty_response():
    """Create a mock empty HTTP response."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {}
    return response


@pytest.fixture
def mock_error_response():
    """Create a mock error HTTP response."""
    response = Mock()
    response.status_code = 404
    response.raise_for_status.side_effect = Exception("404 Not Found")
    return response


@pytest.fixture
def mock_dataset_response():
    """Create a mock dataset response."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {
        "items": [
            {
                "id": "cpih01",
                "title": "Consumer Prices Index including owner occupiers' housing costs (CPIH)",
                "description": "CPIH description",
                "keywords": ["inflation", "prices"],
                "release_frequency": "Monthly",
                "state": "published",
                "links": {
                    "latest_version": {"href": "http://example.com/v1", "id": "1"}
                },
                "next_release": "2025-04-15",
            }
        ]
    }
    return response


@pytest.fixture
def mock_edition_response():
    """Create a mock edition response."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {
        "items": [
            {"edition": "time-series", "id": "time-series"},
            {"edition": "annual", "id": "annual"},
        ]
    }
    return response


@pytest.fixture
def mock_dimension_response():
    """Create a mock dimension response."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {
        "items": [
            {"name": "geography", "id": "geography"},
            {"name": "time", "id": "time"},
        ]
    }
    return response


@pytest.fixture
def mock_options_response():
    """Create a mock dimension options response."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {
        "items": [
            {"option": "K02000001", "label": "United Kingdom"},
            {"option": "E92000001", "label": "England"},
        ],
        "count": 2,
        "total_count": 2,
    }
    return response


@pytest.fixture
def mock_csv_response():
    """Create a mock CSV response."""
    response = Mock()
    response.status_code = 200
    response.text = "date,value\n2020-01,100\n2020-02,101"
    return response


@pytest.fixture
def mock_codelist_response():
    """Create a mock codelist response."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {
        "items": [
            {"links": {"self": {"id": "geography"}}},
            {"links": {"self": {"id": "time-period"}}},
        ]
    }
    return response
