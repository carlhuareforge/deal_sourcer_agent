import requests
import time
import json
import asyncio
from collections import deque
from utils.logger import logger
from config import (
    RAPID_API_KEY,
    RAPID_API_HOST,
    RAPID_API_FALLBACK_HOST,
    RAPID_API_FALLBACK_KEY,
    RAPID_API_REQUESTS_PER_SECOND
)

class RapidAPISemaphore:
    def __init__(self, requests_per_second):
        self.available_tokens = requests_per_second
        self.queue = []  # Simple list like JavaScript
        self.last_refill_time = time.time()
        self.interval_ms = 1000 / requests_per_second
        self.requests_per_second = requests_per_second

    async def acquire(self):
        # Refill tokens based on time elapsed - exactly like JavaScript
        now = time.time()
        time_elapsed_ms = (now - self.last_refill_time) * 1000
        if time_elapsed_ms > self.interval_ms:
            tokens_to_add = int(time_elapsed_ms / self.interval_ms)
            self.available_tokens = min(self.requests_per_second, self.available_tokens + tokens_to_add)
            self.last_refill_time = now

        if self.available_tokens > 0:
            self.available_tokens -= 1
            return True
        
        # Wait for a token to become available - like JavaScript Promise
        future = asyncio.Future()
        self.queue.append(future)
        return await future

    def release(self):
        if len(self.queue) > 0:
            future = self.queue.pop(0)  # shift() in JavaScript
            future.set_result(True)
        else:
            self.available_tokens = min(self.requests_per_second, self.available_tokens + 1)

rapid_api_semaphore = RapidAPISemaphore(RAPID_API_REQUESTS_PER_SECOND)

async def make_http_request(options):
    """
    Helper function to make HTTP requests using requests library.
    """
    method = options.get('method', 'GET')
    url = options.get('url')
    params = options.get('params')
    headers = options.get('headers')
    data = options.get('data')

    try:
        # Run the blocking request in an executor to make it truly async
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: requests.request(method, url, params=params, headers=headers, json=data, timeout=30)
        )
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        
        content_type = response.headers.get('Content-Type', '')
        
        if 'application/json' in content_type or response.text.strip().startswith(('{', '[')):
            try:
                parsed_data = response.json()
            except json.JSONDecodeError:
                logger.error(f"Error parsing JSON response from {url}: {response.text[:500]}...")
                parsed_data = {"error": "Failed to parse JSON", "body": response.text}
        else:
            logger.log(f"Received non-JSON response ({content_type}) from {url}")
            logger.log(f"Response status: {response.status_code} {response.reason}")
            logger.log(f"Response body: {response.text[:500]}...")
            parsed_data = {"error": "Non-JSON response", "body": response.text, "statusCode": response.status_code}

        return {
            "status": response.status_code,
            "statusText": response.reason,
            "headers": dict(response.headers),
            "data": parsed_data
        }
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err} - Response: {http_err.response.text[:500]}...")
        raise
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Connection error occurred: {conn_err}")
        raise
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Timeout error occurred: {timeout_err}")
        raise
    except requests.exceptions.RequestException as req_err:
        logger.error(f"An unexpected error occurred: {req_err}")
        raise

async def throttled_rapid_api_request(request_fn):
    """
    Enhanced throttled request function that uses the semaphore and handles 429 errors.
    """
    try:
        await rapid_api_semaphore.acquire()
        
        # Add a small delay before each request to be even more conservative (matches app.js)
        await asyncio.sleep(0.1) # 100ms delay - NON-BLOCKING
        
        try:
            return await request_fn()
        except requests.exceptions.HTTPError as error:
            if error.response is not None and error.response.status_code == 429:
                logger.error(f"Rate limit exceeded (429). Waiting 2 seconds before retry...")
                await asyncio.sleep(2) # Always wait 2 seconds on a 429 error - NON-BLOCKING
                logger.log(f"Retrying request after rate limit cooldown...")
                return await throttled_rapid_api_request(request_fn) # Recursive retry
            else:
                logger.error(f"API Error: Status {error.response.status_code}")
                logger.error(f"Error data: {json.dumps(error.response.json()) if error.response.text else ''}")
                logger.error(f"Error headers: {json.dumps(dict(error.response.headers))}")
                raise
        except requests.exceptions.RequestException as error:
            logger.error(f"Request setup error: {error}")
            raise
    finally:
        rapid_api_semaphore.release()

class TwitterClient:
    def __init__(self):
        if not RAPID_API_KEY:
            raise ValueError("RAPID_API_KEY must be set.")

        self.headers = {
            'x-rapidapi-key': RAPID_API_KEY,
            'x-rapidapi-host': RAPID_API_HOST
        }

        fallback_key = RAPID_API_FALLBACK_KEY or RAPID_API_KEY
        self.fallback_headers = {
            'x-rapidapi-key': fallback_key,
            'x-rapidapi-host': RAPID_API_FALLBACK_HOST
        }

    def _simplify_user_payload(self, user_entry):
        """
        Extract useful legacy fields into the flattened structure expected by downstream code.
        """
        def _to_int(value):
            try:
                return int(value)
            except (TypeError, ValueError):
                return value

        legacy = None
        if isinstance(user_entry, dict):
            legacy = user_entry.get('legacy')
            if not legacy and user_entry.get('result'):
                legacy = (user_entry.get('result') or {}).get('legacy')
            if not legacy:
                # Fallback responses already provide a legacy-like structure at the top level
                candidate_keys = {'screen_name', 'followers_count', 'friends_count'}
                if candidate_keys.intersection(user_entry.keys()):
                    legacy = user_entry
        legacy = legacy or {}
        if not legacy:
            return None

        user_id = legacy.get('id_str') or legacy.get('id') or user_entry.get('id_str') or user_entry.get('id')
        if user_id is not None:
            user_id = str(user_id)

        simplified = {
            "id_str": user_id,
            "screen_name": legacy.get('screen_name'),
            "name": legacy.get('name'),
            "description": legacy.get('description'),
            "followers_count": _to_int(legacy.get('followers_count')),
            "friends_count": _to_int(legacy.get('friends_count')),
            "created_at": legacy.get('created_at'),
            "profile_image_url_https": legacy.get('profile_image_url_https'),
            "profile_banner_url": legacy.get('profile_banner_url'),
            "legacy": legacy,
            "raw_user": user_entry
        }
        return simplified
    
    def _normalize_user_records(self, response):
        """
        Converts a get-users response payload into the legacy-compatible structure.
        """
        payload = response.get('data') or {}
        normalized = []

        if payload.get('result'):
            for user_record in payload['result']:
                simplified = self._simplify_user_payload(user_record)
                if simplified:
                    normalized.append({
                        "result": {
                            "legacy": simplified['legacy']
                        }
                    })
            return normalized

        if payload.get('users'):
            return payload['users']

        return normalized

    async def _request_users_by_rest_ids(self, user_ids):
        options = {
            'method': 'GET',
            'url': f"https://{RAPID_API_FALLBACK_HOST}/get-users-v2",
            'params': {
                'users': ','.join(user_ids)
            },
            'headers': self.fallback_headers
        }
        return await make_http_request(options)

    async def _fetch_users_resilient(self, user_ids):
        if not user_ids:
            return []

        try:
            response = await throttled_rapid_api_request(
                lambda ids=user_ids: self._request_users_by_rest_ids(ids)
            )
            return self._normalize_user_records(response)
        except requests.exceptions.HTTPError as http_err:
            status_code = http_err.response.status_code if http_err.response else None
            if status_code in (400, 404) and len(user_ids) > 1:
                mid = len(user_ids) // 2
                left = await self._fetch_users_resilient(user_ids[:mid])
                right = await self._fetch_users_resilient(user_ids[mid:])
                return left + right
            uid = user_ids[0] if user_ids else 'unknown'
            logger.warn(f"Skipping user id {uid} due to API error: {http_err}")
            return []
        except Exception as error:
            logger.warn(f"Unexpected error fetching IDs {user_ids}: {error}")
            if len(user_ids) > 1:
                mid = len(user_ids) // 2
                left = await self._fetch_users_resilient(user_ids[:mid])
                right = await self._fetch_users_resilient(user_ids[mid:])
                return left + right
            return []

    async def get_users_by_rest_ids(self, user_ids):
        if not user_ids:
            return {"source": "fallback", "data": {"users": []}}

        chunk_size = 40
        aggregated = {}

        for i in range(0, len(user_ids), chunk_size):
            chunk_ids = user_ids[i:i + chunk_size]
            normalized = await self._fetch_users_resilient(chunk_ids)
            for entry in normalized:
                legacy = (entry.get('result') or {}).get('legacy') or {}
                user_id = legacy.get('id_str') or legacy.get('id')
                if user_id:
                    aggregated[str(user_id)] = entry

        ordered_users = []
        for uid in user_ids:
            entry = aggregated.get(str(uid))
            if entry:
                ordered_users.append(entry)
            else:
                logger.warn(f"Missing user data for id {uid}")

        return {
            "source": "fallback",
            "data": {"users": ordered_users}
        }

    async def _request_following_ids(self, username, count, cursor=None):
        if not self.headers:
            raise ValueError("Following IDs endpoint requires headers.")
        params = {
            'username': username,
            'count': str(max(1, count))
        }
        if cursor:
            params['cursor'] = cursor
        options = {
            'method': 'GET',
            'url': f"https://{RAPID_API_FALLBACK_HOST}/following-ids",
            'params': params,
            'headers': self.fallback_headers
        }
        return await make_http_request(options)

    async def _collect_following_ids(self, username, count):
        """
        Collects up to `count` newest following IDs for the given username using the following-ids endpoint.
        """
        remaining = count
        cursor = None
        collected = []

        while remaining > 0:
            response = await throttled_rapid_api_request(
                lambda: self._request_following_ids(username, remaining, cursor)
            )
            payload = response.get('data') or response
            ids = payload.get('ids') or []
            collected.extend(str(_id) for _id in ids)
            remaining = count - len(collected)

            cursor = payload.get('next_cursor') or payload.get('next_cursor_str')
            if not cursor or str(cursor) in ('0', '-1'):
                break

        return collected[:count]

    async def _fetch_user_details(self, ordered_ids):
        """
        Fetches user profiles for the provided IDs while preserving order.
        """
        if not ordered_ids:
            return []

        chunk_size = 40
        aggregated = {}

        for i in range(0, len(ordered_ids), chunk_size):
            chunk = ordered_ids[i:i + chunk_size]
            response = await self.get_users_by_rest_ids(chunk)
            users = (response.get('data') or {}).get('users') or []
            for user_entry in users:
                simplified = self._simplify_user_payload(user_entry)
                if simplified and simplified.get('id_str'):
                    aggregated[simplified['id_str']] = simplified

        ordered_users = []
        for uid in ordered_ids:
            user = aggregated.get(str(uid))
            if user:
                ordered_users.append(user)
            else:
                logger.warn(f"Missing user data for id {uid}")

        return ordered_users

    def _extract_following_users_from_v2(self, response):
        """
        Normalizes the v2 Following response into the legacy structure.
        """
        try:
            instructions = (
                response.get('data', {})
                    .get('data', {})
                    .get('user', {})
                    .get('result', {})
                    .get('timeline', {})
                    .get('timeline', {})
                    .get('instructions', [])
            )
        except AttributeError:
            instructions = []

        normalized_users = []
        for instruction in instructions:
            if instruction.get('type') != 'TimelineAddEntries':
                continue
            for entry in instruction.get('entries', []):
                content = entry.get('content', {})
                item_content = content.get('itemContent', {})
                if item_content.get('itemType') != 'TimelineUser':
                    continue
                user_result = (item_content.get('user_results') or {}).get('result')
                simplified = self._simplify_user_payload(user_result)
                if simplified:
                    normalized_users.append(simplified)

        return normalized_users

    async def get_following(self, username, count, user_id=None):
        """
        Retrieves newest followings (count entries). Uses following-ids + user detail lookup primarily,
        with timeline/legacy endpoints as fallback.
        """
        primary_error = None

        try:
            id_list = await self._collect_following_ids(username, count)
            if id_list:
                user_details = await self._fetch_user_details(id_list)
                if user_details:
                    return {
                        "source": "following_ids",
                        "data": {
                            "users": user_details
                        }
                    }
                logger.warn(f"User detail lookup returned no results for @{username}")
        except Exception as error:
            primary_error = error
            logger.warn(f"Following IDs path failed for @{username}: {error}. Attempting timeline endpoints.")

        if primary_error:
            raise primary_error

        return {
            "source": "following_ids",
            "data": {
                "users": []
            }
        }

    async def get_user_tweets(self, user_id, count):
        options = {
            'method': 'GET',
            'url': 'https://twitter135.p.rapidapi.com/v2/UserTweets/',
            'params': {
                'id': user_id,
                'count': str(count)
            },
            'headers': self.headers
        }
        return await throttled_rapid_api_request(lambda: make_http_request(options))

    async def get_user_tweets_and_replies(self, user_id, count):
        options = {
            'method': 'GET',
            'url': 'https://twitter135.p.rapidapi.com/v2/UserTweetsAndReplies/',
            'params': {
                'id': user_id,
                'count': str(count)
            },
            'headers': self.headers
        }
        return await throttled_rapid_api_request(lambda: make_http_request(options))

    async def get_user_by_screen_name(self, screen_name):
        options = {
            'method': 'GET',
            'url': 'https://twitter135.p.rapidapi.com/v2/UserByScreenName/',
            'params': {
                'username': screen_name
            },
            'headers': self.headers
        }
        return await throttled_rapid_api_request(lambda: make_http_request(options))
