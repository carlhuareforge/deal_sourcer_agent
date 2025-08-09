"""
Tests for the twitter_client module.
"""

import unittest
from unittest.mock import patch, MagicMock
from api.twitter_client import TwitterClient

class TestTwitterClient(unittest.TestCase):
    """Tests for the TwitterClient."""

    @patch('requests.get')
    def test_get_user_info_success(self, mock_get):
        """Test successful retrieval of user info."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"id_str": "123", "screen_name": "test_user"}
        mock_get.return_value = mock_response

        client = TwitterClient()
        user_info = client.get_user_info("test_user")

        self.assertIsNotNone(user_info)
        self.assertEqual(user_info['screen_name'], "test_user")

    @patch('requests.get')
    def test_get_user_info_failure(self, mock_get):
        """Test failure to retrieve user info."""
        mock_get.side_effect = requests.exceptions.RequestException("API Error")

        client = TwitterClient()
        user_info = client.get_user_info("test_user")

        self.assertIsNone(user_info)

    # Add more tests for other client methods...

if __name__ == '__main__':
    unittest.main()
