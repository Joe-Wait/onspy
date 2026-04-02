"""Pytest configuration and shared fixtures."""

import os
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest


os.environ["ONS_DEBUG"] = "0"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


@pytest.fixture(autouse=True)
def clear_core_cache():
    """Ensure core dataset cache does not leak between tests."""
    import onspy.core as core

    core.invalidate_cache()
    yield
    core.invalidate_cache()


@pytest.fixture
def mock_response():
    """Return a basic successful mocked HTTP response."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {"items": []}
    return response
