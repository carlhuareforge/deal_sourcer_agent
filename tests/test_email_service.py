"""
Tests for the email_service module.
"""

import unittest
from unittest.mock import patch, MagicMock
from services.email_service import EmailService

class TestEmailService(unittest.TestCase):
    """Tests for the EmailService."""

    @patch('smtplib.SMTP_SSL')
    @patch('api.openai_client.OpenAIClient.analyze_tweets')
    def test_send_completion_email(self, mock_analyze_tweets, mock_smtp):
        """Test sending a completion email."""
        mock_analyze_tweets.return_value = "This is a test opening."
        
        # Mock the file reading
        with patch('builtins.open', unittest.mock.mock_open(read_data="test@example.com")):
            service = EmailService()
            stats = {'total_processed': 10, 'total_uploaded': 5, 'total_skipped': 5}
            service.send_completion_email(stats)

        mock_smtp.assert_called_once()

if __name__ == '__main__':
    unittest.main()
