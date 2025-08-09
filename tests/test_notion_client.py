"""
Tests for the notion_client module.
"""

import unittest
from unittest.mock import patch, MagicMock
from api.notion_client import NotionClient

class TestNotionClient(unittest.TestCase):
    """Tests for the NotionClient."""

    @patch('notion_client.Client')
    def test_get_existing_categories_success(self, mock_notion_client):
        """Test successful retrieval of existing categories."""
        mock_response = {
            'properties': {
                'Category': {
                    'multi_select': {
                        'options': [
                            {'name': 'Category 1'},
                            {'name': 'Category 2'}
                        ]
                    }
                }
            }
        }
        mock_notion_client.return_value.databases.retrieve.return_value = mock_response

        client = NotionClient()
        categories = client.get_existing_categories()

        self.assertEqual(categories, ['Category 1', 'Category 2'])

    @patch('notion_client.Client')
    def test_add_database_entry_success(self, mock_notion_client):
        """Test successful addition of a database entry."""
        client = NotionClient()
        data = {
            'name': 'Test Entry',
            'summary': 'This is a test entry.',
            'categories': ['Test'],
            'date': '2025-07-28',
            'source': 'test_source',
            'twitter': 'test_twitter',
            'details': 'Some details.'
        }
        client.add_database_entry(data)

        mock_notion_client.return_value.pages.create.assert_called_once()

    def test_convert_markdown_links_to_notion(self):
        """Test conversion of Markdown links to Notion rich text format."""
        text = "This is a [link](https://example.com) to a website."
        expected_output = [
            {'type': 'text', 'text': {'content': 'This is a '}},
            {'type': 'text', 'text': {'content': 'link', 'link': {'url': 'https://example.com'}}},
            {'type': 'text', 'text': {'content': ' to a website.'}}
        ]
        output = NotionClient.convert_markdown_links_to_notion(text)
        self.assertEqual(output, expected_output)

if __name__ == '__main__':
    unittest.main()
