"""
Tests for the deduplication_service module.
"""

import unittest
from unittest.mock import patch, MagicMock
from services.deduplication_service import DeduplicationService

class TestDeduplicationService(unittest.TestCase):
    """Tests for the DeduplicationService."""

    @patch('db.repository')
    def test_process_new_profile(self, mock_repository):
        """Test processing a new profile."""
        mock_repository.find_profile_by_handle.return_value = None
        result = DeduplicationService.process_profile("new_user", "source_user")
        self.assertTrue(result['is_new'])

    @patch('db.repository')
    def test_process_existing_profile(self, mock_repository):
        """Test processing an existing profile."""
        mock_repository.find_profile_by_handle.return_value = ("existing_user",)
        mock_repository.get_sources_for_profile.return_value = ["source1", "source2"]
        result = DeduplicationService.process_profile("existing_user", "source_user")
        self.assertFalse(result['is_new'])
        self.assertEqual(result['profile'], ("existing_user",))
        self.assertEqual(result['sources'], ["source1", "source2"])

    @patch('db.repository')
    def test_record_new_profile(self, mock_repository):
        """Test recording a new profile."""
        profile_data = {"twitter_handle": "test_user", "notion_page_id": "123"}
        DeduplicationService.record_new_profile(profile_data, "source_user")
        mock_repository.create_profile.assert_called_once_with(profile_data)
        mock_repository.add_relationship.assert_called_once_with("test_user", "source_user")

if __name__ == '__main__':
    unittest.main()
