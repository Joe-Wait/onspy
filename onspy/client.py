"""
ONS API Client module.

This module provides a centralized client class for making requests to the ONS API.
"""

import logging
import requests
import json
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class ONSClient:
    """Client for interacting with the Office of National Statistics API."""

    # API Constants
    EMPTY = ""
    ENDPOINT = "https://api.beta.ons.gov.uk/v1"
    # Identify your bot
    USER_AGENT = (
        "onspy/0.1.0 (MyOrganisation contact@myorg.com +http://www.myorg.com/bot.html)"
    )

    def __init__(self, endpoint: Optional[str] = None):
        """Initialize the ONS client.

        Args:
            endpoint: Custom API endpoint URL (optional)
        """
        self.endpoint = endpoint or self.ENDPOINT
        self._session = requests.Session()
        self._session.headers.update(self._get_browser_headers())

    def _get_browser_headers(self) -> Dict[str, str]:
        """Get browser-like headers to help with HTTP requests.

        Returns:
            Dictionary of headers that mimic a browser
        """
        return {
            "User-Agent": self.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
        }

    def has_internet(self) -> bool:
        """Check if internet connection is available.

        Returns:
            bool: True if internet connection is available, False otherwise
        """
        try:
            # Try to connect to a widely available service
            self._session.get("https://www.google.com", timeout=5)
            return True
        except requests.ConnectionError:
            return False

    def build_url(self, path_segments: Dict[str, Optional[str]]) -> str:
        """Build a request URL from path segments.

        Args:
            path_segments: Dictionary mapping path segment names to values

        Returns:
            Full request URL
        """
        # Build path from segments
        path_parts = []
        for key, value in path_segments.items():
            if value is None:
                continue
            elif value == self.EMPTY:
                path_parts.append(key)
            else:
                path_parts.append(f"{key}/{value}")

        path = "/".join(path_parts)
        url = f"{self.endpoint}/{path}"

        logger.debug(f"Built URL: {url}")
        return url

    def make_request(
        self,
        url: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> Optional[requests.Response]:
        """Make HTTP request to the ONS API.

        Args:
            url: Request URL
            limit: Number of records to return (optional)
            offset: Position in the dataset to start from (optional)
            **kwargs: Additional arguments to pass to requests.get

        Returns:
            Response object if successful, None otherwise
        """
        logger.debug(f"Making request to URL: {url}")
        if limit is not None:
            logger.debug(f"With limit: {limit}")
        if offset is not None:
            logger.debug(f"With offset: {offset}")

        if not self.has_internet():
            logger.error("Unable to connect: No internet connection available")
            print(
                "Unable to connect: Please ensure you have an active internet connection or access through a secure connection."
            )
            return None

        try:
            # Prepare parameters
            params = {}
            if limit is not None:
                params["limit"] = limit
            if offset is not None:
                params["offset"] = offset

            logger.debug(f"Request params: {params}")

            # Try up to 3 times with exponential backoff
            for attempt in range(3):
                try:
                    logger.debug(f"Attempt {attempt+1}/3")

                    response = self._session.get(
                        url, params=params, timeout=30, **kwargs
                    )

                    logger.debug(f"Response status code: {response.status_code}")

                    response.raise_for_status()  # Raise exception for HTTP errors

                    if response.status_code == 200:
                        logger.debug(
                            f"Request successful. Content length: {len(response.content)}"
                        )

                    return response

                except (
                    requests.exceptions.RequestException,
                    requests.exceptions.HTTPError,
                ) as e:
                    logger.warning(f"Request failed: {e}")

                    if attempt == 2:  # Last attempt
                        logger.error(f"Request failed after 3 attempts: {e}")
                        print(f"Request failed: {e}")
                        return None

                    logger.debug(f"Retrying...")
                    continue  # Try again

        except Exception as err:
            logger.error(f"Unexpected error during request: {err}", exc_info=True)
            print(f"Error: {err}")
            return None

    def process_response(self, response: requests.Response) -> Dict[str, Any]:
        """Process HTTP response and convert to JSON.

        Args:
            response: HTTP response object

        Returns:
            JSON content as dictionary
        """
        try:
            json_data = response.json()
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Response JSON keys: {list(json_data.keys())}")
                if "items" in json_data:
                    logger.debug(f"Number of items: {len(json_data['items'])}")
                    if len(json_data["items"]) > 0:
                        logger.debug(
                            f"First item keys: {list(json_data['items'][0].keys())}"
                        )
            return json_data
        except json.JSONDecodeError:
            logger.error("Error decoding JSON response", exc_info=True)
            print("Error decoding JSON response")
            return {}

    @classmethod
    def get_instance(cls) -> "ONSClient":
        """Get a singleton instance of the ONS client.

        Returns:
            ONSClient instance
        """
        if not hasattr(cls, "_instance"):
            cls._instance = cls()
        return cls._instance


# Create a default client instance
default_client = ONSClient.get_instance()
