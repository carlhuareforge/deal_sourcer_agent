import openai
import time
import asyncio
from collections import deque
from utils.logger import logger
from config import OPENAI_API_KEY, OPENAI_MAX_RETRIES, OPENAI_TIMEOUT_MS, OPENAI_REQUESTS_PER_MINUTE

# Initialize OpenAI client
openai_client = openai.OpenAI(
    api_key=OPENAI_API_KEY,
    max_retries=OPENAI_MAX_RETRIES,
    timeout=OPENAI_TIMEOUT_MS / 1000 # Convert ms to seconds
)

def create_throttler(requests_per_minute=OPENAI_REQUESTS_PER_MINUTE):
    """
    OpenAI request throttling factory that creates isolated throttlers.
    Each throttler instance maintains its own rate limit independently.
    """
    interval_s = 60.0 / requests_per_minute
    last_request_time = 0
    
    async def throttler_func(request_fn):
        nonlocal last_request_time
        
        # Calculate if we need to wait based on last request time
        current_time = time.time()
        time_since_last = current_time - last_request_time
        
        if time_since_last < interval_s:
            # Need to wait before making next request
            wait_time = interval_s - time_since_last
            await asyncio.sleep(wait_time)  # Non-blocking async wait
        
        # Update last request time
        last_request_time = time.time()
        
        try:
            # Execute the request
            # Since OpenAI client is synchronous, run it in executor to not block
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, request_fn)
        except Exception as e:
            # Re-raise any exceptions from the request
            raise e

    return throttler_func

# Expose the client and throttler factory
get_openai_client = lambda: openai_client