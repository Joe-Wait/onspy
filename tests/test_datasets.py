"""
Unit tests for the datasets module.
"""

import unittest
from unittest.mock import patch, Mock
import pandas as pd

from onspy.datasets import (
    ons_datasets,
    ons_ids,
    assert_valid_id,
    ons_desc,
    ons_editions,
    id_number,
    ons_latest_href,
    ons_latest_version,
    ons_latest_edition,
    ons_find_latest_version_across_editions,
)


class TestDatasets(unittest.TestCase):
    """Test suite for the datasets module."""

    @patch("onspy.datasets.make_request")
    @patch("onspy.datasets.process_response")
    def test_ons_datasets(self, mock_process, mock_make_request):
        """Test the ons_datasets function."""
        # Setup mock data
        mock_response = Mock()
        mock_data = {
            "items": [
                {
                    "id": "dataset1",
                    "title": "Dataset 1",
                    "links": {
                        "latest_version": {"href": "http://example.com/v1", "id": "1"}
                    },
                },
                {
                    "id": "dataset2",
                    "title": "Dataset 2",
                    "links": {
                        "latest_version": {"href": "http://example.com/v2", "id": "2"}
                    },
                },
            ]
        }
        mock_make_request.return_value = mock_response
        mock_process.return_value = mock_data

        # Call function
        result = ons_datasets()

        # Assert
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertIn("id", result.columns)
        self.assertIn("title", result.columns)
        # Check extracted columns
        self.assertIn("latest_version_href", result.columns)
        self.assertEqual(result["latest_version_href"].iloc[0], "http://example.com/v1")

    @patch("onspy.datasets.ons_datasets")
    def test_ons_ids(self, mock_datasets):
        """Test the ons_ids function."""
        # Setup mock data
        mock_df = pd.DataFrame({"id": ["dataset1", "dataset2", "dataset3"]})
        mock_datasets.return_value = mock_df

        # Call function
        result = ons_ids()

        # Assert
        self.assertEqual(result, ["dataset1", "dataset2", "dataset3"])

    @patch("onspy.datasets.ons_ids")
    def test_assert_valid_id_valid(self, mock_ids):
        """Test assert_valid_id with a valid ID."""
        mock_ids.return_value = ["dataset1", "dataset2", "dataset3"]
        result = assert_valid_id("dataset2")
        self.assertTrue(result)

    @patch("onspy.datasets.ons_ids")
    def test_assert_valid_id_invalid(self, mock_ids):
        """Test assert_valid_id with an invalid ID."""
        mock_ids.return_value = ["dataset1", "dataset2", "dataset3"]
        with self.assertRaises(ValueError):
            assert_valid_id("invalid_dataset")

    @patch("onspy.datasets.ons_ids")
    def test_assert_valid_id_none(self, mock_ids):
        """Test assert_valid_id with None."""
        mock_ids.return_value = ["dataset1", "dataset2", "dataset3"]
        with self.assertRaises(ValueError):
            assert_valid_id(None)

    @patch("onspy.datasets.ons_datasets")
    @patch("onspy.datasets.assert_valid_id")
    @patch("builtins.print")
    def test_ons_desc(self, mock_print, mock_assert, mock_datasets):
        """Test the ons_desc function."""
        # Setup mock data
        mock_df = pd.DataFrame(
            {
                "id": ["dataset1", "dataset2"],
                "title": ["Dataset 1", "Dataset 2"],
                "description": ["Description 1", "Description 2"],
                "keywords": [["keyword1", "keyword2"], ["keyword3"]],
                "release_frequency": ["Monthly", "Annual"],
                "state": ["published", "published"],
                "next_release": ["2025-04-15", "2025-12-01"],
                "latest_version_id": ["1", "2"],
            }
        )
        mock_datasets.return_value = mock_df
        mock_assert.return_value = True

        # Mock ons_editions
        with patch("onspy.datasets.ons_editions", return_value=["time-series"]):
            # Call function
            ons_desc("dataset1")

            # Verify print calls
            # We'll just check a few of the print calls
            self.assertIn(mock_print.call_args_list[0].args[0], "Title: Dataset 1")
            self.assertIn(mock_print.call_args_list[1].args[0], "ID: dataset1")

    @patch("onspy.datasets.make_request")
    @patch("onspy.datasets.process_response")
    def test_ons_editions(self, mock_process, mock_make_request):
        """Test the ons_editions function."""
        # Setup mock data
        mock_response = Mock()
        mock_data = {"items": [{"edition": "time-series"}, {"edition": "annual"}]}
        mock_make_request.return_value = mock_response
        mock_process.return_value = mock_data

        # Call function
        result = ons_editions("dataset1")

        # Assert
        self.assertEqual(result, ["time-series", "annual"])

    @patch("onspy.datasets.ons_datasets")
    def test_id_number(self, mock_datasets):
        """Test the id_number function."""
        # Setup mock data
        mock_df = pd.DataFrame({"id": ["dataset1", "dataset2", "dataset3"]})
        mock_datasets.return_value = mock_df

        # Call function
        result = id_number("dataset2")

        # Assert
        self.assertEqual(result, 1)  # Index of "dataset2" in the DataFrame

    @patch("onspy.datasets.ons_datasets")
    @patch("onspy.datasets.assert_valid_id")
    def test_ons_latest_href(self, mock_assert, mock_datasets):
        """Test the ons_latest_href function."""
        # Setup mock data
        mock_df = pd.DataFrame(
            {
                "id": ["dataset1", "dataset2"],
                "latest_version_href": [
                    "http://example.com/v1",
                    "http://example.com/v2",
                ],
            }
        )
        mock_datasets.return_value = mock_df
        mock_assert.return_value = True

        # Call function
        result = ons_latest_href("dataset2")

        # Assert
        self.assertEqual(result, "http://example.com/v2")

    @patch("onspy.datasets.ons_latest_href")
    def test_ons_latest_version(self, mock_latest_href):
        """Test the ons_latest_version function."""
        mock_latest_href.return_value = (
            "http://example.com/datasets/dataset1/editions/time-series/versions/3"
        )
        result = ons_latest_version("dataset1")
        self.assertEqual(result, "3")

    @patch("onspy.datasets.ons_latest_href")
    def test_ons_latest_edition(self, mock_latest_href):
        """Test the ons_latest_edition function."""
        mock_latest_href.return_value = (
            "http://example.com/datasets/dataset1/editions/time-series/versions/3"
        )
        result = ons_latest_edition("dataset1")
        self.assertEqual(result, "time-series")

    @patch("onspy.datasets.ons_editions")
    @patch("onspy.datasets.make_request")
    @patch("onspy.datasets.process_response")
    def test_ons_find_latest_version_across_editions(
        self, mock_process, mock_make_request, mock_editions
    ):
        """Test the ons_find_latest_version_across_editions function."""
        # Setup mock data
        mock_editions.return_value = ["time-series", "annual"]

        # Mock responses for each edition
        mock_response1 = Mock()
        mock_data1 = {"links": {"latest_version": {"id": "2"}}}

        mock_response2 = Mock()
        mock_data2 = {"links": {"latest_version": {"id": "5"}}}

        # Make the make_request and process_response mocks return different values based on input
        def side_effect_make_request(req):
            if "time-series" in req:
                return mock_response1
            else:
                return mock_response2

        def side_effect_process(res):
            if res == mock_response1:
                return mock_data1
            else:
                return mock_data2

        mock_make_request.side_effect = side_effect_make_request
        mock_process.side_effect = side_effect_process

        # Call function
        result = ons_find_latest_version_across_editions("dataset1")

        # Assert
        self.assertEqual(result, ("annual", "5"))


if __name__ == "__main__":
    unittest.main()
