"""
Unit tests for the browse module.
"""

import unittest
from unittest.mock import patch
import pandas as pd

from onspy.browse import ons_browse, ons_browse_qmi, _open_url


class TestBrowse(unittest.TestCase):
    """Test suite for the browse module."""

    @patch("onspy.browse.webbrowser.open")
    def test_ons_browse(self, mock_open):
        """Test the ons_browse function."""
        # Setup mock
        mock_open.return_value = True

        # Call function
        result = ons_browse()

        # Assert
        self.assertEqual(result, "https://developer.ons.gov.uk/")
        mock_open.assert_called_once_with("https://developer.ons.gov.uk/")

    @patch("onspy.browse.ons_datasets")
    @patch("onspy.browse.assert_valid_id")
    @patch("onspy.browse.id_number")
    @patch("onspy.browse.webbrowser.open")
    def test_ons_browse_qmi(
        self, mock_open, mock_id_number, mock_assert, mock_datasets
    ):
        """Test the ons_browse_qmi function."""
        # Setup mocks
        mock_assert.return_value = True
        mock_id_number.return_value = 0

        # Create mock DataFrame with qmi info
        mock_df = pd.DataFrame(
            {
                "id": ["dataset1", "dataset2"],
                "qmi": [
                    {"href": "http://example.com/qmi/dataset1"},
                    {"href": "http://example.com/qmi/dataset2"},
                ],
            }
        )
        mock_datasets.return_value = mock_df

        mock_open.return_value = True

        # Call function
        result = ons_browse_qmi("dataset1")

        # Assert
        self.assertEqual(result, "http://example.com/qmi/dataset1")
        mock_open.assert_called_once_with("http://example.com/qmi/dataset1")

    @patch("onspy.browse.ons_datasets")
    @patch("onspy.browse.assert_valid_id")
    def test_ons_browse_qmi_no_qmi(self, mock_assert, mock_datasets):
        """Test ons_browse_qmi when no QMI is available."""
        # Setup mocks
        mock_assert.return_value = True

        # Create mock DataFrame without qmi info
        mock_df = pd.DataFrame(
            {"id": ["dataset1", "dataset2"], "title": ["Dataset 1", "Dataset 2"]}
        )
        mock_datasets.return_value = mock_df

        # Call function
        result = ons_browse_qmi("dataset1")

        # Assert
        self.assertIsNone(result)

    @patch("onspy.browse.webbrowser.open")
    def test_open_url(self, mock_open):
        """Test the _open_url function."""
        # Setup mock
        mock_open.return_value = True

        # Test with browser opening
        result = _open_url("http://example.com", open_browser=True)
        self.assertEqual(result, "http://example.com")
        mock_open.assert_called_once_with("http://example.com")

        # Test without browser opening
        mock_open.reset_mock()
        result = _open_url("http://example.com", open_browser=False)
        self.assertEqual(result, "http://example.com")
        mock_open.assert_not_called()


if __name__ == "__main__":
    unittest.main()
