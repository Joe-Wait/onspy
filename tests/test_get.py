"""
Unit tests for the get module.
"""

import unittest
from unittest.mock import patch, Mock
import pandas as pd

from onspy.get import (
    ons_get,
    ons_get_obs,
    build_request_obs,
    ons_dim,
    ons_dim_opts,
    ons_meta,
)


class TestGet(unittest.TestCase):
    """Test suite for the get module."""

    @patch("onspy.get.assert_valid_id")
    @patch("onspy.get.ons_latest_edition")
    @patch("onspy.get.ons_latest_version")
    @patch("onspy.get.make_request")
    @patch("onspy.get.process_response")
    @patch("onspy.get.read_csv")
    def test_ons_get(
        self,
        mock_read_csv,
        mock_process,
        mock_make_request,
        mock_latest_version,
        mock_latest_edition,
        mock_assert,
    ):
        """Test the ons_get function."""
        # Setup mocks
        mock_assert.return_value = True
        mock_latest_edition.return_value = "time-series"
        mock_latest_version.return_value = "3"

        mock_response = Mock()
        mock_data = {"downloads": {"csv": {"href": "http://example.com/data.csv"}}}
        mock_make_request.return_value = mock_response
        mock_process.return_value = mock_data

        mock_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        mock_read_csv.return_value = mock_df

        # Call function
        result = ons_get("dataset1")

        # Assert
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.shape, (3, 2))
        mock_read_csv.assert_called_with("http://example.com/data.csv")

    @patch("onspy.get.assert_valid_id")
    @patch("onspy.get.ons_latest_edition")
    @patch("onspy.get.ons_latest_version")
    @patch("onspy.get.make_request")
    @patch("onspy.get.process_response")
    def test_ons_get_obs(
        self,
        mock_process,
        mock_make_request,
        mock_latest_version,
        mock_latest_edition,
        mock_assert,
    ):
        """Test the ons_get_obs function."""
        # Setup mocks
        mock_assert.return_value = True
        mock_latest_edition.return_value = "time-series"
        mock_latest_version.return_value = "3"

        mock_response = Mock()
        mock_data = {
            "observations": [
                {"time": "2020", "value": "10.5"},
                {"time": "2021", "value": "11.2"},
            ],
            "total_observations": 2,
            "limit": 10,
            "offset": 0,
        }
        mock_make_request.return_value = mock_response
        mock_process.return_value = mock_data

        # Call function with dimension filters
        result = ons_get_obs("dataset1", geography="K02000001", time="*")

        # Assert
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.shape, (2, 2))

        # The build_request_obs part will be tested separately

    def test_build_request_obs(self):
        """Test the build_request_obs function."""
        # Test with regular parameters
        with patch("onspy.get.ons_dim", return_value=["time", "geography"]):
            result = build_request_obs("dataset1", time="2020", geography="K02000001")
            self.assertIn("time=2020", result)
            self.assertIn("geography=K02000001", result)

            # Test with list parameter
            result = build_request_obs(
                "dataset1", time=["2020", "2021"], geography="K02000001"
            )
            self.assertIn("time=2020", result)
            self.assertIn("geography=K02000001", result)

            # Test with missing dimension
            with self.assertRaises(ValueError):
                build_request_obs("dataset1", time="2020")  # Missing geography

    @patch("onspy.get.assert_valid_id")
    @patch("onspy.get.ons_latest_edition")
    @patch("onspy.get.ons_latest_version")
    @patch("onspy.get.make_request")
    @patch("onspy.get.process_response")
    def test_ons_dim(
        self,
        mock_process,
        mock_make_request,
        mock_latest_version,
        mock_latest_edition,
        mock_assert,
    ):
        """Test the ons_dim function."""
        # Setup mocks
        mock_assert.return_value = True
        mock_latest_edition.return_value = "time-series"
        mock_latest_version.return_value = "3"

        mock_response = Mock()
        mock_data = {
            "items": [
                {"name": "time", "id": "time"},
                {"name": "geography", "id": "geography"},
            ]
        }
        mock_make_request.return_value = mock_response
        mock_process.return_value = mock_data

        # Call function
        result = ons_dim("dataset1")

        # Assert
        self.assertEqual(result, ["time", "geography"])

    @patch("onspy.get.assert_valid_id")
    @patch("onspy.get.ons_latest_edition")
    @patch("onspy.get.ons_latest_version")
    @patch("onspy.get.ons_dim")
    @patch("onspy.get.make_request")
    @patch("onspy.get.process_response")
    def test_ons_dim_opts(
        self,
        mock_process,
        mock_make_request,
        mock_ons_dim,
        mock_latest_version,
        mock_latest_edition,
        mock_assert,
    ):
        """Test the ons_dim_opts function."""
        # Setup mocks
        mock_assert.return_value = True
        mock_latest_edition.return_value = "time-series"
        mock_latest_version.return_value = "3"
        mock_ons_dim.return_value = ["time", "geography"]

        mock_response = Mock()
        mock_data = {
            "items": [
                {"option": "2020", "id": "2020"},
                {"option": "2021", "id": "2021"},
            ],
            "count": 2,
            "total_count": 2,
            "limit": 50,
            "offset": 0,
        }
        mock_make_request.return_value = mock_response
        mock_process.return_value = mock_data

        # Call function
        result = ons_dim_opts("dataset1", dimension="time")

        # Assert
        self.assertEqual(result, ["2020", "2021"])

        # Test with invalid dimension
        with self.assertRaises(ValueError):
            ons_dim_opts("dataset1", dimension="invalid_dim")

    @patch("onspy.get.assert_valid_id")
    @patch("onspy.get.ons_latest_edition")
    @patch("onspy.get.ons_latest_version")
    @patch("onspy.get.make_request")
    @patch("onspy.get.process_response")
    def test_ons_meta(
        self,
        mock_process,
        mock_make_request,
        mock_latest_version,
        mock_latest_edition,
        mock_assert,
    ):
        """Test the ons_meta function."""
        # Setup mocks
        mock_assert.return_value = True
        mock_latest_edition.return_value = "time-series"
        mock_latest_version.return_value = "3"

        mock_response = Mock()
        mock_data = {
            "release_date": "2023-01-01",
            "next_release": "2024-01-01",
            "contact": {"name": "Test Contact", "email": "test@example.com"},
        }
        mock_make_request.return_value = mock_response
        mock_process.return_value = mock_data

        # Call function
        result = ons_meta("dataset1")

        # Assert
        self.assertEqual(result, mock_data)
        self.assertEqual(result["release_date"], "2023-01-01")
        self.assertEqual(result["contact"]["email"], "test@example.com")


if __name__ == "__main__":
    unittest.main()
