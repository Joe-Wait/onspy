"""
Integration tests for the onspy package.

These tests check how different functions from various modules interact with each other.
"""

import unittest
from unittest.mock import patch, Mock
import pandas as pd

from onspy import (
    ons_datasets,
    ons_ids,
    ons_get,
    ons_get_obs,
    ons_dim,
    ons_dim_opts,
    ons_codelists,
    ons_search,
)


class TestOnsIntegration(unittest.TestCase):
    """Integration tests for the onspy package."""

    @patch("onspy.datasets.make_request")
    @patch("onspy.datasets.process_response")
    def test_datasets_ids_integration(self, mock_process, mock_make_request):
        """Test integration between ons_datasets and ons_ids."""
        # Setup mocks
        mock_response = Mock()
        mock_data = {
            "items": [
                {
                    "id": "cpih01",
                    "title": "Consumer Prices Index including owner occupiers' housing costs (CPIH)",
                    "links": {
                        "latest_version": {"href": "http://example.com/v1", "id": "1"}
                    },
                },
                {
                    "id": "mid-year-pop-est",
                    "title": "Mid-year population estimates",
                    "links": {
                        "latest_version": {"href": "http://example.com/v2", "id": "2"}
                    },
                },
            ]
        }
        mock_make_request.return_value = mock_response
        mock_process.return_value = mock_data

        # Call functions
        datasets = ons_datasets()
        ids = ons_ids()

        # Assert
        self.assertIsInstance(datasets, pd.DataFrame)
        self.assertEqual(len(datasets), 2)
        self.assertEqual(ids, ["cpih01", "mid-year-pop-est"])

    @patch("onspy.datasets.make_request")
    @patch("onspy.datasets.process_response")
    @patch("onspy.get.make_request")
    @patch("onspy.get.process_response")
    @patch("onspy.get.read_csv")
    def test_get_with_latest_integration(
        self,
        mock_read_csv,
        mock_process_get,
        mock_make_request_get,
        mock_process_datasets,
        mock_make_request_datasets,
    ):
        """Test integration of ons_get with automatic latest version/edition detection."""
        # Setup mocks for datasets module
        mock_response_datasets = Mock()
        mock_data_datasets = {
            "items": [
                {
                    "id": "cpih01",
                    "title": "CPIH",
                    "links": {
                        "latest_version": {
                            "href": "http://example.com/datasets/cpih01/editions/time-series/versions/3",
                            "id": "3",
                        }
                    },
                }
            ]
        }
        mock_make_request_datasets.return_value = mock_response_datasets
        mock_process_datasets.return_value = mock_data_datasets

        # Setup mocks for get module
        mock_response_get = Mock()
        mock_data_get = {"downloads": {"csv": {"href": "http://example.com/data.csv"}}}
        mock_make_request_get.return_value = mock_response_get
        mock_process_get.return_value = mock_data_get

        # Mock CSV data
        mock_df = pd.DataFrame({"date": ["2020-01", "2020-02"], "value": [100, 101]})
        mock_read_csv.return_value = mock_df

        # Call function (which should automatically use the latest edition/version)
        result = ons_get("cpih01")

        # Assert
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.shape, (2, 2))
        # Check that the request was made with the correct edition and version
        mock_make_request_get.assert_called_once()
        args, _ = mock_make_request_get.call_args
        self.assertIn("editions/time-series/versions/3", args[0])

    def test_dimensions_observation_integration(self):
        """Test integration between dimension and observation functions."""
        # Create separate patches for more controlled mocking
        with patch("onspy.datasets.make_request") as mock_make_request_datasets, patch(
            "onspy.datasets.process_response"
        ) as mock_process_datasets, patch(
            "onspy.get.make_request"
        ) as mock_make_request_get, patch(
            "onspy.get.process_response"
        ) as mock_process_get:

            # Setup mocks for datasets module
            mock_response_datasets = Mock()
            mock_data_datasets = {
                "items": [
                    {
                        "id": "cpih01",
                        "title": "CPIH",
                        "links": {
                            "latest_version": {
                                "href": "http://example.com/datasets/cpih01/editions/time-series/versions/3",
                                "id": "3",
                            }
                        },
                    }
                ]
            }
            mock_make_request_datasets.return_value = mock_response_datasets
            mock_process_datasets.return_value = mock_data_datasets

            # Setup mock responses for dimension requests
            dimensions_response = Mock(name="dimensions_response")
            dimensions_data = {
                "items": [
                    {"name": "geography", "id": "geography"},
                    {"name": "time", "id": "time"},
                ]
            }

            # Setup mock responses for dimension options requests
            options_response = Mock(name="options_response")
            options_data = {
                "items": [
                    {"option": "K02000001", "label": "United Kingdom"},
                    {"option": "E92000001", "label": "England"},
                ],
                "count": 2,
                "total_count": 2,
            }

            # Setup mock responses for observations requests
            observations_response = Mock(name="observations_response")
            observations_data = {
                "observations": [
                    {
                        "time": "2020-01",
                        "geography": "K02000001",
                        "value": "100.0",
                    },
                    {
                        "time": "2020-02",
                        "geography": "K02000001",
                        "value": "101.0",
                    },
                ],
                "total_observations": 2,
            }

            # Configure make_request_get to return appropriate responses
            def request_side_effect(url, **kwargs):
                if "/dimensions" in url and not "/options" in url:
                    return dimensions_response
                elif "/dimensions/geography/options" in url:
                    return options_response
                elif "/observations" in url:
                    return observations_response
                return None

            mock_make_request_get.side_effect = request_side_effect

            # Configure process_response to return appropriate data
            def process_side_effect(response):
                if response == dimensions_response:
                    return dimensions_data
                elif response == options_response:
                    return options_data
                elif response == observations_response:
                    return observations_data
                return {}

            mock_process_get.side_effect = process_side_effect

            # Call functions
            dimensions = ons_dim("cpih01")
            geo_options = ons_dim_opts("cpih01", dimension="geography")
            obs = ons_get_obs("cpih01", geography="K02000001", time="*")

            # Assert
            self.assertEqual(dimensions, ["geography", "time"])
            self.assertEqual(geo_options, ["K02000001", "E92000001"])
            self.assertIsInstance(obs, pd.DataFrame)
            self.assertEqual(len(obs), 2)

    @patch("onspy.code_lists.make_request")
    @patch("onspy.code_lists.process_response")
    @patch("onspy.search.make_request")
    @patch("onspy.search.process_response")
    def test_codelists_search_integration(
        self,
        mock_process_search,
        mock_make_request_search,
        mock_process_codelists,
        mock_make_request_codelists,
    ):
        """Test integration between code lists and search functions."""
        # Setup mocks for code lists
        mock_response_codelists = Mock()
        mock_data_codelists = {
            "items": [
                {"links": {"self": {"id": "geography"}}},
                {"links": {"self": {"id": "aggregate"}}},
            ]
        }
        mock_make_request_codelists.return_value = mock_response_codelists
        mock_process_codelists.return_value = mock_data_codelists

        # Setup mocks for search
        mock_response_search = Mock()
        mock_data_search = {
            "items": [
                {"id": "cpih1dim1A0", "label": "All items"},
                {"id": "cpih1dim1A1", "label": "Food and non-alcoholic beverages"},
            ]
        }
        mock_make_request_search.return_value = mock_response_search
        mock_process_search.return_value = mock_data_search

        # Call functions
        with patch(
            "onspy.search.ons_latest_edition", return_value="time-series"
        ), patch("onspy.search.ons_latest_version", return_value="3"), patch(
            "onspy.search.assert_valid_id", return_value=True
        ):

            codelists = ons_codelists()
            search_results = ons_search("cpih01", name="aggregate", query="food")

            # Assert
            self.assertEqual(codelists, ["geography", "aggregate"])
            self.assertEqual(len(search_results), 2)
            self.assertEqual(
                search_results[1]["label"], "Food and non-alcoholic beverages"
            )


if __name__ == "__main__":
    unittest.main()
