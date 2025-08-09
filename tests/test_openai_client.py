"""
Tests for the openai_client module.
"""

import unittest
from unittest.mock import patch, MagicMock
from api.openai_client import OpenAIClient

class TestOpenAIClient(unittest.TestCase):
    """Tests for the OpenAIClient."""

    @patch('openai.OpenAI')
    def test_analyze_tweets_success(self, mock_openai_client):
        """Test successful analysis of tweets."""
        mock_response = MagicMock()
        mock_response.choices[0].message.content = '{"name": "Test User", "summary": "A test user.", "categories": ["Test"]}'
        mock_openai_client.return_value.chat.completions.create.return_value = mock_response

        client = OpenAIClient()
        result = client.analyze_tweets({"profile": {}, "tweets": []})

        self.assertIsNotNone(result)
        self.assertIn("Test User", result)

    @patch('openai.OpenAI')
    def test_analyze_tweets_failure(self, mock_openai_client):
        """Test failure to analyze tweets."""
        mock_openai_client.return_value.chat.completions.create.side_effect = Exception("API Error")

        client = OpenAIClient()
        result = client.analyze_tweets({"profile": {}, "tweets": []})

        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()
