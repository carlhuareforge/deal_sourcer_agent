from db.repository import repository
from utils.logger import logger

class DeduplicationService:
    @staticmethod
    async def process_profile(twitter_handle, source_username):
        """
        Processes a profile discovery, handling both new and existing profiles.
        Matches JavaScript logic exactly - checks globally, not per source.
        """
        # Check if profile exists globally (like JavaScript)
        existing_profile = repository.find_by_handle(twitter_handle)
        
        if existing_profile:
            # Update last seen date
            repository.update_last_seen(twitter_handle)
            
            # Add new source relationship
            repository.add_source_relationship(twitter_handle, source_username)
            
            # Get all sources that discovered this profile
            sources = repository.get_sources_for_profile(twitter_handle)
            
            return {
                "isNew": False,
                "profile": existing_profile,
                "sources": sources
            }
        
        return {
            "isNew": True
        }

    @staticmethod
    async def record_new_profile(profile_data, source_username):
        """
        Records a new profile as processed, updating its Notion page ID if available.
        `profile_data` should be a dict with 'twitter_handle' and 'notion_page_id'.
        """
        twitter_handle = profile_data.get('twitter_handle')
        notion_page_id = profile_data.get('notion_page_id')
        
        if not twitter_handle:
            logger.error("Cannot record new profile: 'twitter_handle' is missing.")
            return

        try:
            repository.record_new_profile(twitter_handle, notion_page_id, source_username)
            logger.debug(f"Deduplication service recorded @{twitter_handle} (source: {source_username}) with Notion ID: {notion_page_id}")
        except Exception as e:
            logger.error(f"Error recording profile @{twitter_handle} in deduplication service: {e}")
            raise

# Expose the class for direct import as in app.js
DeduplicationService = DeduplicationService