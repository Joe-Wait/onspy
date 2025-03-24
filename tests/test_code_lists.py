"""
Unit tests for the code_lists module.
"""

import unittest
from unittest.mock import patch, Mock

from onspy.code_lists import (
    ons_codelists,
    assert_valid_codeid,
    ons_codelist,
    ons_codelist_editions,
    assert_valid_edition,
    ons_codelist_edition,
    ons_codes,
    assert_valid_code,
    ons_code,
    ons_code_dataset,
)


class TestCodeLists(unittest.TestCase):
    """Test suite for the code_lists module."""

    @patch("onspy.code_lists.make_request")
    @patch("onspy.code_lists.process_response")
    def test_ons_codelists(self, mock_process, mock_make_request):
        """Test the ons_codelists function."""
        # Setup mocks
        mock_response = Mock()
        mock_data = {
            "items": [
                {"links": {"self": {"id": "geography"}}},
                {"links": {"self": {"id": "time-period"}}},
            ]
        }
        mock_make_request.return_value = mock_response
        mock_process.return_value = mock_data

        # Call function
        result = ons_codelists()

        # Assert
        self.assertEqual(result, ["geography", "time-period"])

    @patch("onspy.code_lists.ons_codelists")
    def test_assert_valid_codeid_valid(self, mock_codelists):
        """Test assert_valid_codeid with a valid code ID."""
        mock_codelists.return_value = ["geography", "time-period"]
        result = assert_valid_codeid("geography")
        self.assertTrue(result)

    @patch("onspy.code_lists.ons_codelists")
    def test_assert_valid_codeid_invalid(self, mock_codelists):
        """Test assert_valid_codeid with an invalid code ID."""
        mock_codelists.return_value = ["geography", "time-period"]
        with self.assertRaises(ValueError):
            assert_valid_codeid("invalid_code")

    @patch("onspy.code_lists.ons_codelists")
    def test_assert_valid_codeid_none(self, mock_codelists):
        """Test assert_valid_codeid with None."""
        mock_codelists.return_value = ["geography", "time-period"]
        with self.assertRaises(ValueError):
            assert_valid_codeid(None)

    @patch("onspy.code_lists.assert_valid_codeid")
    @patch("onspy.code_lists.make_request")
    @patch("onspy.code_lists.process_response")
    def test_ons_codelist(self, mock_process, mock_make_request, mock_assert):
        """Test the ons_codelist function."""
        # Setup mocks
        mock_assert.return_value = True
        mock_response = Mock()
        mock_data = {
            "id": "geography",
            "name": "Geography",
            "description": "Geographic areas",
        }
        mock_make_request.return_value = mock_response
        mock_process.return_value = mock_data

        # Call function
        result = ons_codelist("geography")

        # Assert
        self.assertEqual(result, mock_data)

    @patch("onspy.code_lists.assert_valid_codeid")
    @patch("onspy.code_lists.make_request")
    @patch("onspy.code_lists.process_response")
    def test_ons_codelist_editions(self, mock_process, mock_make_request, mock_assert):
        """Test the ons_codelist_editions function."""
        # Setup mocks
        mock_assert.return_value = True
        mock_response = Mock()
        mock_data = {
            "items": [
                {"edition": "one-off", "id": "one-off"},
                {"edition": "latest", "id": "latest"},
            ]
        }
        mock_make_request.return_value = mock_response
        mock_process.return_value = mock_data

        # Call function
        result = ons_codelist_editions("geography")

        # Assert
        self.assertEqual(result, mock_data["items"])

    @patch("onspy.code_lists.ons_codelist_editions")
    def test_assert_valid_edition_valid(self, mock_editions):
        """Test assert_valid_edition with a valid edition."""
        mock_editions.return_value = [
            {"edition": "one-off", "id": "one-off"},
            {"edition": "latest", "id": "latest"},
        ]
        result = assert_valid_edition("geography", "one-off")
        self.assertTrue(result)

    @patch("onspy.code_lists.ons_codelist_editions")
    def test_assert_valid_edition_invalid(self, mock_editions):
        """Test assert_valid_edition with an invalid edition."""
        mock_editions.return_value = [
            {"edition": "one-off", "id": "one-off"},
            {"edition": "latest", "id": "latest"},
        ]
        with self.assertRaises(ValueError):
            assert_valid_edition("geography", "invalid_edition")

    @patch("onspy.code_lists.assert_valid_codeid")
    @patch("onspy.code_lists.assert_valid_edition")
    @patch("onspy.code_lists.make_request")
    @patch("onspy.code_lists.process_response")
    def test_ons_codelist_edition(
        self, mock_process, mock_make_request, mock_assert_edition, mock_assert_codeid
    ):
        """Test the ons_codelist_edition function."""
        # Setup mocks
        mock_assert_codeid.return_value = True
        mock_assert_edition.return_value = True
        mock_response = Mock()
        mock_data = {
            "id": "geography",
            "edition": "one-off",
            "release_date": "2020-01-01",
        }
        mock_make_request.return_value = mock_response
        mock_process.return_value = mock_data

        # Call function
        result = ons_codelist_edition("geography", "one-off")

        # Assert
        self.assertEqual(result, mock_data)

    @patch("onspy.code_lists.assert_valid_codeid")
    @patch("onspy.code_lists.assert_valid_edition")
    @patch("onspy.code_lists.make_request")
    @patch("onspy.code_lists.process_response")
    def test_ons_codes(
        self, mock_process, mock_make_request, mock_assert_edition, mock_assert_codeid
    ):
        """Test the ons_codes function."""
        # Setup mocks
        mock_assert_codeid.return_value = True
        mock_assert_edition.return_value = True
        mock_response = Mock()
        mock_data = {
            "items": [
                {"code": "K02000001", "label": "United Kingdom"},
                {"code": "E92000001", "label": "England"},
            ]
        }
        mock_make_request.return_value = mock_response
        mock_process.return_value = mock_data

        # Call function
        result = ons_codes("geography", "one-off")

        # Assert
        self.assertEqual(result, mock_data["items"])

    @patch("onspy.code_lists.ons_codes")
    def test_assert_valid_code_valid(self, mock_codes):
        """Test assert_valid_code with a valid code."""
        mock_codes.return_value = [
            {"code": "K02000001", "label": "United Kingdom"},
            {"code": "E92000001", "label": "England"},
        ]
        result = assert_valid_code("geography", "one-off", "K02000001")
        self.assertTrue(result)

    @patch("onspy.code_lists.ons_codes")
    def test_assert_valid_code_invalid(self, mock_codes):
        """Test assert_valid_code with an invalid code."""
        mock_codes.return_value = [
            {"code": "K02000001", "label": "United Kingdom"},
            {"code": "E92000001", "label": "England"},
        ]
        with self.assertRaises(ValueError):
            assert_valid_code("geography", "one-off", "invalid_code")

    @patch("onspy.code_lists.assert_valid_codeid")
    @patch("onspy.code_lists.assert_valid_edition")
    @patch("onspy.code_lists.assert_valid_code")
    @patch("onspy.code_lists.make_request")
    @patch("onspy.code_lists.process_response")
    def test_ons_code(
        self,
        mock_process,
        mock_make_request,
        mock_assert_code,
        mock_assert_edition,
        mock_assert_codeid,
    ):
        """Test the ons_code function."""
        # Setup mocks
        mock_assert_codeid.return_value = True
        mock_assert_edition.return_value = True
        mock_assert_code.return_value = True
        mock_response = Mock()
        mock_data = {"id": "K02000001", "code": "K02000001", "label": "United Kingdom"}
        mock_make_request.return_value = mock_response
        mock_process.return_value = mock_data

        # Call function
        result = ons_code("geography", "one-off", "K02000001")

        # Assert
        self.assertEqual(result, mock_data)

    @patch("onspy.code_lists.assert_valid_codeid")
    @patch("onspy.code_lists.assert_valid_edition")
    @patch("onspy.code_lists.assert_valid_code")
    @patch("onspy.code_lists.make_request")
    @patch("onspy.code_lists.process_response")
    def test_ons_code_dataset(
        self,
        mock_process,
        mock_make_request,
        mock_assert_code,
        mock_assert_edition,
        mock_assert_codeid,
    ):
        """Test the ons_code_dataset function."""
        # Setup mocks
        mock_assert_codeid.return_value = True
        mock_assert_edition.return_value = True
        mock_assert_code.return_value = True
        mock_response = Mock()
        mock_data = {
            "items": [
                {"id": "dataset1", "title": "Dataset 1"},
                {"id": "dataset2", "title": "Dataset 2"},
            ]
        }
        mock_make_request.return_value = mock_response
        mock_process.return_value = mock_data

        # Call function
        result = ons_code_dataset("geography", "one-off", "K02000001")

        # Assert
        self.assertEqual(result, mock_data["items"])


if __name__ == "__main__":
    unittest.main()
