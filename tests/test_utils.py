"""
Unit tests for the utils module.
"""

import unittest
from unittest.mock import patch, Mock
import pandas as pd
import requests
import json

from onspy.utils import (
    null_coalesce,
    has_internet,
    set_endpoint,
    build_request_dots,
    build_base_request,
    extend_request_dots,
    get_browser_headers,
    make_request,
    process_response,
    read_csv,
    cat_ratio,
    cat_ratio_obs,
)


class TestUtils(unittest.TestCase):
    """Test suite for the utils module."""

    def test_null_coalesce(self):
        """Test the null_coalesce function."""
        self.assertEqual(null_coalesce(None, "default"), "default")
        self.assertEqual(null_coalesce("value", "default"), "value")
        self.assertEqual(null_coalesce(0, 5), 0)
        self.assertEqual(null_coalesce(False, True), False)

    @patch("onspy.utils.client.has_internet")
    def test_has_internet_success(self, mock_client_has_internet):
        """Test has_internet when connection is successful."""
        mock_client_has_internet.return_value = True
        self.assertTrue(has_internet())
        mock_client_has_internet.assert_called_once()

    @patch("onspy.utils.client.has_internet")
    def test_has_internet_failure(self, mock_client_has_internet):
        """Test has_internet when connection fails."""
        mock_client_has_internet.return_value = False
        self.assertFalse(has_internet())
        mock_client_has_internet.assert_called_once()

    def test_set_endpoint(self):
        """Test the set_endpoint function."""
        from onspy.utils import ENDPOINT

        self.assertEqual(set_endpoint("datasets"), f"{ENDPOINT}/datasets")
        self.assertEqual(
            set_endpoint("code-lists/geography"), f"{ENDPOINT}/code-lists/geography"
        )

    def test_build_request_dots(self):
        """Test the build_request_dots function."""
        # Basic parameters
        result = build_request_dots(
            datasets="cpih01", editions="time-series", versions="1"
        )
        self.assertEqual(result, "datasets/cpih01/editions/time-series/versions/1")

        # With empty string
        from onspy.utils import EMPTY

        result = build_request_dots(datasets="cpih01", editions=EMPTY)
        self.assertEqual(result, "datasets/cpih01/editions")

        # With None (should be skipped)
        result = build_request_dots(datasets="cpih01", editions=None, versions="1")
        self.assertEqual(result, "datasets/cpih01/versions/1")

    def test_build_base_request(self):
        """Test the build_base_request function."""
        from onspy.utils import ENDPOINT, EMPTY

        # Test with regular parameters
        result = build_base_request(datasets="cpih01", editions="time-series")
        self.assertEqual(result, f"{ENDPOINT}/datasets/cpih01/editions/time-series")

        # Test with empty parameter
        result = build_base_request(datasets=EMPTY)
        self.assertEqual(result, f"{ENDPOINT}/datasets")

    def test_extend_request_dots(self):
        """Test the extend_request_dots function."""
        base_url = "https://api.example.com/datasets"
        result = extend_request_dots(base_url, editions="time-series", versions="1")
        self.assertEqual(
            result, "https://api.example.com/datasets/editions/time-series/versions/1"
        )

    def test_get_browser_headers(self):
        """Test the get_browser_headers function."""
        headers = get_browser_headers()
        self.assertIn("User-Agent", headers)
        self.assertIn("Accept", headers)
        self.assertIn("Accept-Language", headers)
        self.assertIn("Accept-Encoding", headers)

    @patch("onspy.utils.client.make_request")
    def test_make_request_success(self, mock_client_make_request):
        """Test make_request when the request is successful."""
        mock_response = Mock(status_code=200)
        mock_client_make_request.return_value = mock_response

        result = make_request("https://api.example.com/datasets")

        self.assertEqual(result, mock_response)
        mock_client_make_request.assert_called_once_with(
            "https://api.example.com/datasets", None, None
        )

    @patch("onspy.utils.client.has_internet")
    def test_make_request_no_internet(self, mock_client_has_internet):
        """Test make_request when there's no internet connection."""
        mock_client_has_internet.return_value = False

        result = make_request("https://api.example.com/datasets")

        self.assertIsNone(result)

    @patch("onspy.utils.client.make_request")
    def test_make_request_with_params(self, mock_client_make_request):
        """Test make_request with limit and offset parameters."""
        mock_response = Mock(status_code=200)
        mock_client_make_request.return_value = mock_response

        result = make_request("https://api.example.com/datasets", limit=50, offset=100)

        self.assertEqual(result, mock_response)
        mock_client_make_request.assert_called_once_with(
            "https://api.example.com/datasets", 50, 100
        )

    def test_process_response(self):
        """Test the process_response function."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "items": [{"id": "dataset1"}, {"id": "dataset2"}]
        }

        result = process_response(mock_response)

        self.assertEqual(result, {"items": [{"id": "dataset1"}, {"id": "dataset2"}]})

    def test_process_response_error(self):
        """Test process_response when JSON decoding fails."""
        mock_response = Mock()
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)

        result = process_response(mock_response)

        self.assertEqual(result, {})

    @patch("onspy.utils.requests.get")
    def test_read_csv_success(self, mock_get):
        """Test read_csv when the request is successful."""
        mock_response = Mock(status_code=200)
        mock_response.text = "id,value\n1,a\n2,b\n3,c"
        mock_response.content = b"id,value\n1,a\n2,b\n3,c"
        mock_get.return_value = mock_response

        result = read_csv("https://api.example.com/data.csv")

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        self.assertEqual(list(result.columns), ["id", "value"])

    @patch("onspy.utils.requests.get")
    def test_read_csv_error(self, mock_get):
        """Test read_csv when the request fails."""
        mock_get.side_effect = Exception("Request failed")

        result = read_csv("https://api.example.com/data.csv")

        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(result.empty)

    @patch("builtins.print")
    def test_cat_ratio(self, mock_print):
        """Test the cat_ratio function."""
        data = {"count": 10, "total_count": 50, "limit": 10, "offset": 0}

        cat_ratio(data)

        mock_print.assert_called_once_with("Fetched 10/50 (limit = 10, offset = 0)")

    @patch("builtins.print")
    def test_cat_ratio_obs(self, mock_print):
        """Test the cat_ratio_obs function."""
        data = {
            "observations": [1, 2, 3, 4, 5],
            "total_observations": 100,
            "limit": 5,
            "offset": 10,
        }

        cat_ratio_obs(data)

        mock_print.assert_called_once_with("Fetched 5/100 (limit = 5, offset = 10)")


if __name__ == "__main__":
    unittest.main()
