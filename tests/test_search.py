"""
Unit tests for the search module.
"""

import unittest
from unittest.mock import patch, Mock

from onspy.search import ons_search


class TestSearch(unittest.TestCase):
    """Test suite for the search module."""

    @patch("onspy.search.assert_valid_id")
    @patch("onspy.search.ons_latest_edition")
    @patch("onspy.search.ons_latest_version")
    @patch("onspy.search.make_request")
    @patch("onspy.search.process_response")
    def test_ons_search(
        self,
        mock_process,
        mock_make_request,
        mock_latest_version,
        mock_latest_edition,
        mock_assert,
    ):
        """Test the ons_search function."""
        # Setup mocks
        mock_assert.return_value = True
        mock_latest_edition.return_value = "time-series"
        mock_latest_version.return_value = "3"

        mock_response = Mock()
        mock_data = {
            "items": [
                {"id": "item1", "label": "Item 1"},
                {"id": "item2", "label": "Item 2"},
            ]
        }
        mock_make_request.return_value = mock_response
        mock_process.return_value = mock_data

        # Call function
        result = ons_search("dataset1", name="time", query="2020")

        # Assert
        self.assertEqual(result, mock_data["items"])

    @patch("onspy.search.assert_valid_id")
    @patch("onspy.search.ons_latest_edition")
    @patch("onspy.search.ons_latest_version")
    def test_ons_search_with_missing_params(
        self, mock_latest_version, mock_latest_edition, mock_assert
    ):
        """Test ons_search with missing parameters."""
        # Setup mocks
        mock_assert.return_value = True
        mock_latest_edition.return_value = "time-series"
        mock_latest_version.return_value = "3"

        # Test with missing name
        with self.assertRaises(ValueError):
            ons_search("dataset1", query="2020")

        # Test with missing query
        with self.assertRaises(ValueError):
            ons_search("dataset1", name="time")

    @patch("onspy.search.assert_valid_id")
    @patch("onspy.search.ons_latest_edition")
    @patch("onspy.search.ons_latest_version")
    def test_ons_search_with_missing_edition_version(
        self, mock_latest_version, mock_latest_edition, mock_assert
    ):
        """Test ons_search when latest edition and version can't be determined."""
        # Setup mocks
        mock_assert.return_value = True
        mock_latest_edition.return_value = None
        mock_latest_version.return_value = None

        # Should raise ValueError
        with self.assertRaises(ValueError):
            ons_search("dataset1", name="time", query="2020")


if __name__ == "__main__":
    unittest.main()
