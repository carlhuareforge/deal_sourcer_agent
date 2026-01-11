from db.repository import repository
from utils.logger import logger
from datetime import datetime, timedelta

class DeduplicationService:
    @staticmethod
    async def process_profile(twitter_handle, source_username):
        """
        Processes a profile discovery, handling both new and existing profiles.
        Matches JavaScript logic exactly - checks globally, not per source.
        """
        if not twitter_handle:
            return {"isNew": True}

        # Check if profile exists globally (like JavaScript)
        existing_profile = repository.find_by_handle(twitter_handle)
        
        if existing_profile:
            # Add new source relationship
            repository.add_source_relationship(twitter_handle, source_username)
            
            # Get all sources that discovered this profile
            sources = repository.get_sources_for_profile(twitter_handle)

            seen_within_days = 28
            now = datetime.now()
            days_since_last_seen = None
            seen_recently = False

            last_updated_raw = existing_profile.get("last_updated_date")
            if last_updated_raw:
                try:
                    last_updated_dt = datetime.fromisoformat(last_updated_raw)
                    delta = now - last_updated_dt
                    days_since_last_seen = delta.days
                    seen_recently = delta < timedelta(days=seen_within_days)
                except ValueError:
                    logger.warn(f"Could not parse last_updated_date for @{twitter_handle}: {last_updated_raw}")

            # If we've seen it recently, skip (do not bump last_updated_date so it can age out).
            if seen_recently:
                return {
                    "isNew": False,
                    "profile": existing_profile,
                    "sources": sources,
                    "daysSinceLastSeen": days_since_last_seen,
                    "seenWithinDays": seen_within_days
                }

            # Older than the recency window: allow it through for re-processing.
            return {
                "isNew": True,
                "profile": existing_profile,
                "sources": sources,
                "daysSinceLastSeen": days_since_last_seen,
                "seenWithinDays": seen_within_days
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
