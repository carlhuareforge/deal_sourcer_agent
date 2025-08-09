import requests
import time
import json
import asyncio
from collections import deque
from utils.logger import logger
from config import RAPID_API_KEY, RAPID_API_HOST, RAPID_API_REQUESTS_PER_SECOND

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
            lambda: requests.request(method, url, params=params, headers=headers, json=data, timeout=10)
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
        self.headers = {
            'x-rapidapi-key': RAPID_API_KEY,
            'x-rapidapi-host': RAPID_API_HOST
        }

    async def get_users_by_rest_ids(self, user_ids):
        options = {
            'method': 'GET',
            'url': 'https://twitter135.p.rapidapi.com/v2/UsersByRestIds/',
            'params': {
                'ids': ','.join(user_ids)
            },
            'headers': self.headers
        }
        return await throttled_rapid_api_request(lambda: make_http_request(options))

    async def get_following(self, username, count):
        options = {
            'method': 'GET',
            'url': 'https://twitter135.p.rapidapi.com/v1.1/Following/',
            'params': {
                'username': username,
                'count': str(count)
            },
            'headers': self.headers
        }
        return await throttled_rapid_api_request(lambda: make_http_request(options))

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