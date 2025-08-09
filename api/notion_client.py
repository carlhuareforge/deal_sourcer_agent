import os
import requests
import json
from utils.logger import logger
from config import NOTION_API_KEY, NOTION_DATABASE_ID, NOTION_UPLOAD_ENABLED

class NotionClient:
    def __init__(self):
        self.base_url = "https://api.notion.com/v1"
        self.headers = {
            "Authorization": f"Bearer {NOTION_API_KEY}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        self.database_id = NOTION_DATABASE_ID

    async def initialize_notion_categories(self):
        if not NOTION_UPLOAD_ENABLED:
            logger.log("Notion upload is disabled. Skipping category initialization.")
            return
        if not self.database_id:
            logger.error("NOTION_DATABASE_ID is not set. Cannot initialize categories.")
            return

        logger.log("Initializing Notion categories...")
        try:
            response = requests.get(f"{self.base_url}/databases/{self.database_id}", headers=self.headers)
            response.raise_for_status()
            data = response.json()
            
            # Check if 'Category' property exists and is a multi_select type
            if 'properties' in data and 'Category' in data['properties']:
                categories_prop = data['properties']['Category']
                if categories_prop.get('type') == 'multi_select':
                    existing_options = {option['name'] for option in categories_prop['multi_select']['options']}
                    logger.log(f"Existing Notion categories: {existing_options}")
                else:
                    logger.error("'Category' property found but is not a multi-select type.")
                    raise ValueError("Category property is not configured as multi-select in Notion database")
            else:
                logger.error("⚠️ 'Category' property not found in Notion database schema.")
                raise ValueError("Category property not found in Notion database")

        except requests.exceptions.RequestException as e:
            logger.error(f"Error initializing Notion categories: {e}")
            if e.response:
                logger.error(f"Notion API response: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during Notion category initialization: {e}")

    async def get_existing_categories(self):
        if not NOTION_UPLOAD_ENABLED:
            logger.log("Notion upload is disabled. Returning default categories.")
            return ['Unknown']
        if not self.database_id:
            logger.error("NOTION_DATABASE_ID is not set. Cannot get existing categories.")
            return ['Unknown']

        try:
            response = requests.get(f"{self.base_url}/databases/{self.database_id}", headers=self.headers)
            response.raise_for_status()
            data = response.json()
            
            if 'properties' in data and 'Category' in data['properties']:
                categories_prop = data['properties']['Category']
                if categories_prop.get('type') == 'multi_select':
                    categories = [option['name'] for option in categories_prop['multi_select']['options']]
                    # Always include 'Unknown' as a fallback
                    if 'Unknown' not in categories:
                        categories.append('Unknown')
                    return categories
            logger.error("Category property not found or not configured correctly")
            return ['Unknown'] # Default if not found or not multi_select
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching existing Notion categories: {e}")
            if e.response:
                logger.error(f"Notion API response: {e.response.status_code} - {e.response.text}")
            return ['Unknown']
        except Exception as e:
            logger.error(f"An unexpected error occurred while getting Notion categories: {e}")
            return ['Unknown']

    async def add_notion_database_entry(self, entry_data):
        if not NOTION_UPLOAD_ENABLED:
            logger.log("Notion upload is disabled. Skipping database entry creation.")
            # Return a mock response similar to a successful Notion API call
            return {"id": "mock_notion_page_id", "status": "mock_success"}
        if not self.database_id:
            logger.error("NOTION_DATABASE_ID is not set. Cannot add database entry.")
            raise ValueError("NOTION_DATABASE_ID is not set.")

        logger.log(f"Attempting to add Notion entry for @{entry_data.get('screenName')}")
        
        # Log the entry data for debugging
        logger.log(f"Entry data keys: {list(entry_data.keys())}")
        logger.log(f"Categories: {entry_data.get('categories', [])}")
        
        properties = {
            "Name": {
                "title": [
                    {
                        "text": {
                            "content": entry_data.get('name', 'Unknown')
                        }
                    }
                ]
            },
            "Summary": {
                "rich_text": [
                    {
                        "text": {
                            "content": entry_data.get('summary', '')
                        }
                    }
                ]
            },
            "Date": {
                "date": {
                    "start": entry_data.get('date', '')
                }
            },
            "Details": {
                "type": "rich_text",
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": entry_data.get('content', '')[:1990]  # Limit to 1990 chars like app.js
                        }
                    }
                ]
            },
            "Source": {
                "type": "rich_text",
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": f"@{entry_data.get('sourceUsername', '')}",
                            "link": {
                                "url": f"https://x.com/{entry_data.get('sourceUsername', '')}"
                            }
                        }
                    }
                ]
            },
            "Twitter": {
                "type": "rich_text",
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": f"@{entry_data.get('screenName', '')}",
                            "link": {
                                "url": f"https://x.com/{entry_data.get('screenName', '')}"
                            }
                        }
                    }
                ]
            },
            "Category": {
                "multi_select": [
                    {"name": cat} for cat in entry_data.get('categories', ['Unknown'])
                ]
            }
        }

        data = {
            "parent": {"database_id": self.database_id},
            "properties": properties
        }

        # Log the complete request for debugging
        logger.log(f"Notion request properties: {list(properties.keys())}")

        try:
            response = requests.post(f"{self.base_url}/pages", headers=self.headers, json=data)
            
            # Check for errors before raise_for_status
            if response.status_code != 200:
                logger.error(f"Notion API returned status {response.status_code}")
                logger.error(f"Response headers: {dict(response.headers)}")
                logger.error(f"Response text: {response.text}")
                try:
                    error_json = response.json()
                    logger.error(f"Error JSON: {json.dumps(error_json, indent=2)}")
                except:
                    pass
            
            response.raise_for_status()
            logger.log(f"Successfully added Notion entry for @{entry_data.get('screenName')}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error adding Notion database entry for @{entry_data.get('screenName')}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                # Always try to get the full error details
                try:
                    error_text = e.response.text
                    logger.error(f"Notion API full response: {error_text}")
                    error_details = e.response.json()
                    logger.error(f"Notion API error details: {json.dumps(error_details, indent=2)}")
                except Exception as parse_error:
                    logger.error(f"Could not parse error response: {parse_error}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while adding Notion entry: {e}")
            raise

# Initialize a global Notion client instance
notion_client = NotionClient()

# Expose functions for direct import as in app.js
initialize_notion_categories = notion_client.initialize_notion_categories
get_existing_categories = notion_client.get_existing_categories
add_notion_database_entry = notion_client.add_notion_database_entry