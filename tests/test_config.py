"""
Tests for the config module.
"""

import unittest
import config

class TestConfig(unittest.TestCase):
    """Tests for the config module."""

    def test_api_keys_loaded(self):
        """Test that the API keys are loaded from the environment."""
        self.assertIsNotNone(config.RAPID_API_KEY)
        self.assertIsNotNone(config.OPENAI_API_KEY)
        self.assertIsNotNone(config.NOTION_API_KEY)

    def test_notion_database_id_loaded(self):
        """Test that the Notion database ID is loaded from the environment."""
        self.assertIsNotNone(config.NOTION_DATABASE_ID)

    def test_file_paths_are_correct(self):
        """Test that the file paths are constructed correctly."""
        self.assertTrue(config.INPUT_FILE.endswith('input_usernames.csv'))
        self.assertTrue(config.LOGS_DIR.endswith('logs'))
        self.assertTrue(config.PROMPTS_DIR.endswith('prompts'))
        self.assertTrue(config.DATABASE_FILE.endswith('database.db'))

if __name__ == '__main__':
    unittest.main()
