"""
Tests for the repository module.
"""

import unittest
import sqlite3
from unittest.mock import patch
from db import repository

class TestRepository(unittest.TestCase):
    """Tests for the repository module."""

    def setUp(self):
        """Set up an in-memory SQLite database for testing."""
        self.conn = sqlite3.connect(":memory:")
        with open('db/schema.sql', 'r') as f:
            self.conn.executescript(f.read())

    def tearDown(self):
        """Close the database connection."""
        self.conn.close()

    @patch('db.repository.create_connection')
    def test_create_profile(self, mock_create_connection):
        """Test creating a profile."""
        mock_create_connection.return_value = self.conn
        profile_data = {"twitter_handle": "test_user", "notion_page_id": "123"}
        repository.create_profile(profile_data)
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM profiles WHERE twitter_handle = ?", ("test_user",))
        self.assertIsNotNone(cursor.fetchone())

    @patch('db.repository.create_connection')
    def test_find_profile_by_handle(self, mock_create_connection):
        """Test finding a profile by handle."""
        mock_create_connection.return_value = self.conn
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO profiles (twitter_handle, notion_page_id) VALUES (?, ?)", ("test_user", "123"))
        self.conn.commit()
        profile = repository.find_profile_by_handle("test_user")
        self.assertIsNotNone(profile)
        self.assertEqual(profile[1], "test_user")

    # Add more tests for other repository functions...

if __name__ == '__main__':
    unittest.main()
