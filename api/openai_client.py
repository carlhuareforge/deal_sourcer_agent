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

class IndependentThrottler:
    """
    Truly independent throttler that maintains its own state.
    Each instance is completely isolated from others.
    """
    def __init__(self, requests_per_minute=OPENAI_REQUESTS_PER_MINUTE):
        self.interval_s = 60.0 / requests_per_minute
        self.last_request_time = 0
        self.lock = asyncio.Lock()  # Ensure thread-safety for this instance
    
    async def __call__(self, request_fn):
        async with self.lock:  # Only lock this specific throttler instance
            # Calculate if we need to wait based on last request time
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            
            if time_since_last < self.interval_s:
                # Need to wait before making next request
                wait_time = self.interval_s - time_since_last
                await asyncio.sleep(wait_time)  # Non-blocking async wait
            
            # Update last request time
            self.last_request_time = time.time()
        
        # Execute outside the lock to allow true parallelism
        try:
            # Since OpenAI client is synchronous, run it in executor to not block
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, request_fn)
        except Exception as e:
            # Re-raise any exceptions from the request
            raise e

def create_throttler(requests_per_minute=OPENAI_REQUESTS_PER_MINUTE):
    """
    Creates a truly independent throttler instance.
    Each call creates a new, isolated throttler.
    """
    return IndependentThrottler(requests_per_minute)

# Expose the client and throttler factory
get_openai_client = lambda: openai_client