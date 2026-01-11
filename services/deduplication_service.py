from db.repository import repository
from utils.logger import logger
from datetime import datetime, timedelta

class DeduplicationService:
    @staticmethod
    def _parse_twitter_created_at(created_at_raw):
        if not created_at_raw or not isinstance(created_at_raw, str):
            return None

        try:
            return datetime.strptime(created_at_raw, '%a %b %d %H:%M:%S %z %Y')
        except Exception:
            pass

        try:
            normalized = created_at_raw
            if normalized.endswith("Z"):
                normalized = normalized[:-1] + "+00:00"
            return datetime.fromisoformat(normalized)
        except Exception:
            return None

    @staticmethod
    async def process_profile(twitter_handle, source_username, profile_created_at=None):
        """
        Processes a profile discovery, handling both new and existing profiles.
        Matches JavaScript logic exactly - checks globally, not per source.
        """
        if not twitter_handle:
            return {"isNew": True}

        account_age_days = None
        created_dt = DeduplicationService._parse_twitter_created_at(profile_created_at)
        if created_dt:
            now_dt = datetime.now(created_dt.tzinfo) if created_dt.tzinfo else datetime.now()
            account_age_days = (now_dt - created_dt).days

        # Check if profile exists globally (like JavaScript)
        existing_profile = repository.find_by_handle(twitter_handle)
        
        if existing_profile:
            category = existing_profile.get("category")
            if category and category.lower() == "profile":
                repository.add_source_relationship(twitter_handle, source_username)
                sources = repository.get_sources_for_profile(twitter_handle)
                return {
                    "isNew": False,
                    "profile": existing_profile,
                    "sources": sources,
                    "daysSinceLastSeen": 0,
                    "seenWithinDays": 90,
                    "skipReason": "category_profile",
                    "accountAgeDays": account_age_days,
                }

            # Add new source relationship
            repository.add_source_relationship(twitter_handle, source_username)
            
            # Get all sources that discovered this profile
            sources = repository.get_sources_for_profile(twitter_handle)

            seen_within_days = 90
            min_account_age_days_for_recheck = 365
            now = datetime.now()
            days_since_last_seen = None
            seen_recently = False

            if account_age_days is not None and account_age_days < min_account_age_days_for_recheck:
                return {
                    "isNew": False,
                    "profile": existing_profile,
                    "sources": sources,
                    "daysSinceLastSeen": days_since_last_seen,
                    "seenWithinDays": seen_within_days,
                    "skipReason": "account_too_new_for_recheck",
                    "accountAgeDays": account_age_days,
                    "minAccountAgeDaysForRecheck": min_account_age_days_for_recheck,
                }

            last_updated_raw = existing_profile.get("last_updated_date")
            if last_updated_raw:
                try:
                    normalized_last_updated = last_updated_raw
                    if isinstance(normalized_last_updated, str) and normalized_last_updated.endswith("Z"):
                        normalized_last_updated = normalized_last_updated[:-1] + "+00:00"

                    last_updated_dt = datetime.fromisoformat(normalized_last_updated)
                    now_dt = datetime.now(last_updated_dt.tzinfo) if last_updated_dt.tzinfo else now
                    delta = now_dt - last_updated_dt
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
                    "seenWithinDays": seen_within_days,
                    "skipReason": "seen_recently",
                    "accountAgeDays": account_age_days,
                    "minAccountAgeDaysForRecheck": min_account_age_days_for_recheck,
                }

            # Older than the recency window: allow it through for re-processing.
            return {
                "isNew": True,
                "profile": existing_profile,
                "sources": sources,
                "daysSinceLastSeen": days_since_last_seen,
                "seenWithinDays": seen_within_days,
                "skipReason": "eligible_for_reprocess",
                "accountAgeDays": account_age_days,
                "minAccountAgeDaysForRecheck": min_account_age_days_for_recheck,
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
        category = profile_data.get('category')
        
        if not twitter_handle:
            logger.error("Cannot record new profile: 'twitter_handle' is missing.")
            return

        try:
            repository.record_new_profile(twitter_handle, notion_page_id, source_username, category=category)
            logger.debug(f"Deduplication service recorded @{twitter_handle} (source: {source_username}) with Notion ID: {notion_page_id}")
        except Exception as e:
            logger.error(f"Error recording profile @{twitter_handle} in deduplication service: {e}")
            raise

# Expose the class for direct import as in app.js
DeduplicationService = DeduplicationService
