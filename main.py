

import asyncio
import os
import json
import csv
from datetime import datetime
import pytz # For timezone handling
import requests
import time  # For processing_time calculations
import shutil  # For file backup operations
import logging  # Referenced but not imported
import openai  # For OpenAI exception handling

from utils.logger import logger
from config import (
    INPUT_FILE, OUTPUT_DIR, TWEETS_DIR, INPUT_HISTORY_DIR,
    PROMPTS_DIR, FOLLOWER_COUNTS_DIR, LOGS_DIR, RAW_RESPONSES_DIR, AI_TWEETS_DIR,
    MAX_PROFILES, RAPID_API_KEY, RAPID_API_HOST, RAPID_API_REQUESTS_PER_SECOND,
    OPENAI_API_KEY, OPENAI_MAX_RETRIES, OPENAI_TIMEOUT_MS, OPENAI_REQUESTS_PER_MINUTE,
    OPENAI_MODEL, MAX_FOLLOWERS, MAX_FOLLOWING, MAX_ACCOUNT_AGE_DAYS,
    MAX_CONCURRENT_REQUESTS, CONCURRENT_PROCESSES, DEBUG_MODE, RECOVERY_FILE,
    USE_S3_SYNC
)

from api.twitter_client import TwitterClient, throttled_rapid_api_request
from api.twitter_parser import simplify_twitter_data, extract_tweets_from_response
from api.notion_client import initialize_notion_categories, get_existing_categories, add_notion_database_entry, update_notion_database_entry
from api.openai_client import get_openai_client, create_throttler
from services.deduplication_service import DeduplicationService
from db.s3_sync import S3DatabaseSync
from db.repository import repository
# from services.email_service import send_completion_email  # Email functionality disabled

# Initialize clients
twitter_client = TwitterClient()
openai_client = get_openai_client()

# Global reference to Notion categories
notion_categories = ['Unknown']

# Global list for skipped profiles
skipped_profiles = []

# Global list for analysis/processing errors that resulted in a skip (no Notion upload)
analysis_errors = []

# Global list for existing profiles updated in Notion because they were stale (>= 28 days since last update)
stale_notion_updates = []

# Utility function to check if a file exists
async def file_exists(file_path):
    return os.path.exists(file_path)

async def setup_directories():
    """
    Initializes all necessary directories.
    """
    for directory in [OUTPUT_DIR, TWEETS_DIR, INPUT_HISTORY_DIR,
                       FOLLOWER_COUNTS_DIR, LOGS_DIR, RAW_RESPONSES_DIR, AI_TWEETS_DIR]:
        os.makedirs(directory, exist_ok=True)
    logger.log("All necessary directories ensured.")

async def read_input_usernames():
    """
    Reads input CSV of user screen_names + user_ids.
    """
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            records = list(reader)

        # Skip header row
        data_records = records[1:]

        max_profiles_int = int(MAX_PROFILES)
        if max_profiles_int <= 0:
            raise ValueError(f"Invalid MAX_PROFILES value: {MAX_PROFILES}")

        # Take only the specified number of records
        limited_records = data_records[:max_profiles_int]

        profiles = []
        for record in limited_records:
            if len(record) >= 2:
                profiles.append({
                    "screen_name": record[0].strip(),
                    "user_id": record[1].strip()
                })
            else:
                logger.warn(f"Skipping malformed row in input_usernames.csv: {record}")

        logger.log(f"Total profiles in file: {len(data_records)}")
        logger.log(f"Processing {len(profiles)} profiles (limited by MAX_PROFILES={max_profiles_int})")

        return profiles
    except FileNotFoundError:
        logger.error(f"Input file not found: {INPUT_FILE}")
        raise
    except Exception as e:
        logger.error(f"Error reading input usernames: {e}")
        raise

async def load_prompt(filename):
    """
    Loads the AI prompt from a file.
    """
    try:
        prompt_path = os.path.join(PROMPTS_DIR, filename)
        with open(prompt_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        logger.error(f"Prompt file not found: {prompt_path}")
        return None
    except Exception as e:
        logger.error(f"Error loading prompt file {filename}: {e}")
        return None

async def prepare_analysis_prompt():
    """
    Prepares the analysis prompt by inserting dynamic categories.
    """
    try:
        base_prompt = await load_prompt('tweet_analysis_prompt.txt')
        if not base_prompt:
            raise ValueError("Base prompt could not be loaded.")
        
        # Insert the categories array into the prompt
        final_prompt = base_prompt.replace('{categories}', json.dumps(notion_categories))
        return final_prompt
    except Exception as e:
        logger.error(f"Error preparing prompt: {e}")
        raise

async def analyze_tweets_with_ai(tweets_file_path, custom_throttler=None):
    """
    Analyzes tweets and profile with OpenAI, and optionally uploads results to Notion.
    """
    global skipped_profiles, analysis_errors, stale_notion_updates # Declare intent to modify global lists

    try:
        # 1. Read local JSON (already fetched from Twitter)
        with open(tweets_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        raw_data = json.loads(content)
        simplified_data = simplify_twitter_data(raw_data)

        screen_name = simplified_data['profile'].get('screen_name', 'Unknown')
        logger.log(f"\n🤖 Starting AI analysis for @{screen_name}")
        logger.log(f"- Profile Name: {simplified_data['profile'].get('name', 'Unknown')}")
        logger.log(f"- Followers: {simplified_data['profile'].get('followers_count', 0)}")
        logger.log(f"- Following: {simplified_data['profile'].get('friends_count', 0)}")
        logger.log(f"- Account Created: {simplified_data['profile'].get('created_at', 'Unknown')}")
        
        # 2. If no tweets, we still pass minimal data to the AI
        data_for_ai = simplified_data
        if not simplified_data['tweets']:
            logger.log("- Tweets Available: 0 (analyzing profile only)")
        else:
            logger.log(f"- Tweets Available: {len(simplified_data['tweets'])}")
            if simplified_data['tweets']:
                first_tweet = simplified_data['tweets'][0]
                logger.log(f"- First Tweet Sample: \"{first_tweet[:100]}{ '...' if len(first_tweet) > 100 else ''}\"")

        # 3. Build prompt (inserting Notion categories)
        prompt = await prepare_analysis_prompt()
        
        # Create AI input data with timestamp
        ai_input_data = {
            "timestamp": datetime.now().isoformat(),
            "prompt": prompt,
            "data": data_for_ai
        }

        # Save AI input data to persistent storage
        ai_tweets_dir = os.path.join(os.path.dirname(tweets_file_path), 'ai_tweets')
        os.makedirs(ai_tweets_dir, exist_ok=True)
        ai_input_file = os.path.join(ai_tweets_dir, f"{screen_name}_ai_input.json")
        with open(ai_input_file, 'w', encoding='utf-8') as f:
            json.dump(ai_input_data, f, indent=2)
        logger.log(f"Saved AI input data to {ai_input_file}")

        # Use the provided throttler or create a new one
        throttler = custom_throttler if custom_throttler else create_throttler()

        # Function to try OpenAI with progressively fewer tweets if we hit token limits
        async def try_analysis_with_retry(data, max_retries=OPENAI_MAX_RETRIES):
            current_data = data.copy()
            retry_count = 0
            
            model_to_use = OPENAI_MODEL
            token_limit = 200000 # Hardcoded as in app.js
            logger.log(f"Using model: {model_to_use} (token limit: {token_limit})")
            
            while retry_count <= max_retries:
                try:
                    # Adjust tweets based on retry attempts
                    if retry_count > 0 and current_data['tweets']:
                        reduced_tweet_count = max(1, len(current_data['tweets']) // 2)
                        logger.log(f"Retry {retry_count}: Reducing tweets from {len(current_data['tweets'])} to {reduced_tweet_count}")
                        current_data['tweets'] = current_data['tweets'][:reduced_tweet_count]

                    # Call OpenAI with explicit JSON response format instruction
                    completion = await throttler(lambda: openai_client.chat.completions.create(
                        model=model_to_use,
                        messages=[
                            {
                                "role": "system",
                                "content": prompt,
                            },
                            {"role": "user", "content": json.dumps(current_data, indent=2)}
                        ],
                        response_format={"type": "json_object"}
                    ))

                    # Log token usage if available
                    if completion.usage:
                        logger.log(f"Token usage - Prompt: {completion.usage.prompt_tokens}, Completion: {completion.usage.completion_tokens}, Total: {completion.usage.total_tokens}")

                    content = completion.choices[0].message.content
                    
                    # Basic validation that we have a JSON-like string
                    if not content or not isinstance(content, str) or not content.strip().startswith('{') or not content.strip().endswith('}'):
                        logger.error(f"Invalid JSON format received: {content[:50] + '...' if content else 'empty response'}")
                        raise ValueError('Invalid JSON format in response')
                    
                    return content
                except openai.APITimeoutError as e:
                    logger.error(f"OpenAI API Timeout Error: {e}")
                    if retry_count >= max_retries:
                        raise
                    retry_count += 1
                    logger.log(f"Retrying OpenAI call (attempt {retry_count}/{max_retries})")
                    await asyncio.sleep(2 ** retry_count) # Exponential backoff
                except openai.APIStatusError as e:
                    if e.status_code == 400 and "maximum context length" in str(e):
                        if retry_count >= max_retries:
                            raise ValueError(f"Failed after {max_retries} retries: {e}")
                        
                        retry_count += 1
                        logger.log(f"Token limit exceeded, retrying with fewer tweets (attempt {retry_count}/{max_retries})")
                        
                        # If it's the last retry, use only profile data
                        if retry_count == max_retries:
                            logger.log(f"Final retry: Using only profile data, no tweets")
                            current_data = {
                                "profile": current_data['profile'],
                                "tweets": [],
                                "sourceUsername": current_data['sourceUsername']
                            }
                        await asyncio.sleep(1) # Small delay before retry
                    else:
                        logger.error(f"OpenAI API Status Error: {e.status_code} - {e.response}")
                        raise
                except Exception as e:
                    logger.error(f"An unexpected error occurred during OpenAI analysis: {e}")
                    raise
            
            raise ValueError('Max retries exceeded for OpenAI analysis')

        # 4. Call OpenAI with retry logic
        ai_response = await try_analysis_with_retry(data_for_ai)
        
        if not ai_response:
            logger.error('AI returned empty response')
            analysis_errors.append({
                "username": screen_name,
                "file": os.path.basename(tweets_file_path),
                "reason": "openai_empty_response",
            })
            return None

        logger.log('AI Analysis: Received response')
        if DEBUG_MODE:
            logger.debug('Raw AI Response:\n' + ai_response)

        # 5. Parse the AI response as JSON
        parsed_data = None
        try:
            cleaned_response = ai_response.strip()
            parsed_data = json.loads(cleaned_response)
        except json.JSONDecodeError as parse_error:
            logger.error(f"Failed to parse AI response as JSON: {parse_error}")
            response_preview = ai_response[:100] + '...' if len(ai_response) > 100 else ai_response
            char_codes = ' '.join(f'{ord(c):x}' for c in ai_response[:20])
            logger.error(f"Response starts with char codes: {char_codes}")
            logger.error(f"Response preview: \"{response_preview}\"")
            
            # Attempt to recover the JSON if possible
            if '{' in ai_response and '}' in ai_response:
                try:
                    start_idx = ai_response.find('{')
                    end_idx = ai_response.rfind('}') + 1
                    json_substring = ai_response[start_idx:end_idx]
                    logger.log('Attempting to recover JSON from response...')
                    parsed_data = json.loads(json_substring)
                    logger.log('Successfully recovered JSON from response!')
                except Exception as recovery_error:
                    logger.error(f'Recovery attempt failed: {recovery_error}')
                    analysis_errors.append({
                        "username": screen_name,
                        "file": os.path.basename(tweets_file_path),
                        "reason": "openai_json_recovery_failed",
                        "error": str(recovery_error),
                    })
                    return None
            else:
                analysis_errors.append({
                    "username": screen_name,
                    "file": os.path.basename(tweets_file_path),
                    "reason": "openai_json_parse_failed",
                    "error": str(parse_error),
                })
                return None

        # Save AI response for debugging
        ai_response_file = os.path.join(ai_tweets_dir, f"{screen_name}_ai_response.json")
        with open(ai_response_file, 'w', encoding='utf-8') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "rawResponse": ai_response,
                    "parsedResponse": parsed_data
                }, f, indent=2)
        logger.log(f"Saved AI response to {ai_response_file}")
        
        logger.log(f"AI Response for @{screen_name}:")
        logger.log(f"- AI Name: {parsed_data.get('name', 'Not provided')}")
        logger.log(f"- AI Categories: {parsed_data.get('categories', [])}")
        logger.log(f"- AI Summary (first 200 chars): {parsed_data.get('summary', '')[:200]}{ '...' if len(parsed_data.get('summary', '')) > 200 else ''}")

        # 6. If it's an individual "Profile" or contains "Gaming/NFT" or meme-related categories, skip Notion
        skip_categories = ['Gaming/NFT', 'Meme', 'Memecoin', 'AI Meme']
        has_meme_category = any(cat in skip_categories or 'meme' in cat.lower() or 'memecoin' in cat.lower() for cat in parsed_data.get('categories', []))

        if has_meme_category:
            meme_category = next((cat for cat in parsed_data.get('categories', []) if cat in skip_categories or 'meme' in cat.lower() or 'memecoin' in cat.lower()), None)
            logger.log(f"- Decision Factor: Contains meme category: {meme_category}")

        if (
            isinstance(parsed_data.get('categories'), list) and
            (
                (len(parsed_data['categories']) == 1 and parsed_data['categories'][0] == 'Profile') or
                has_meme_category
            )
        ):
            skip_reason = 'Profile'
            if has_meme_category:
                skip_reason = next((cat for cat in parsed_data.get('categories', []) if cat in skip_categories or 'meme' in cat.lower() or 'memecoin' in cat.lower()), 'Meme/NFT')
            
            skipped_profiles.append({
                "username": screen_name,
                "reason": skip_reason,
                "description": simplified_data['profile'].get('description', 'No description available'),
                "categories": parsed_data.get('categories', [])
            })
            
            logger.log(f"\n🚫 SKIPPING Notion upload for @{screen_name}")
            logger.log(f"- Skip Reason: {skip_reason}")
            logger.log(f"- AI Name: {parsed_data.get('name')}")
            
            profile_name = simplified_data['profile'].get('name', 'Unknown')
            profile_description = simplified_data['profile'].get('description', 'No description')
            logger.log(f"\nFull Classification Analysis:")
            logger.log(f"- Twitter Name: {profile_name}")
            logger.log(f"- Twitter Handle: @{screen_name}")
            logger.log(f"- Bio/Description: {profile_description[:200]}{ '...' if len(profile_description) > 200 else ''}")
            logger.log(f"- Followers: {simplified_data['profile'].get('followers_count', 0)}")
            logger.log(f"- Following: {simplified_data['profile'].get('friends_count', 0)}")
            logger.log(f"- Tweet Count Analyzed: {len(simplified_data['tweets'])}")
            logger.log(f"- AI Categories Assigned: {parsed_data.get('categories', [])}")
            logger.log(f"- AI Summary: {parsed_data.get('summary', '')[:150]}{ '...' if len(parsed_data.get('summary', '')) > 150 else ''}")
            
            if len(parsed_data.get('categories', [])) == 1 and parsed_data['categories'][0] == 'Profile':
                logger.log(f"- Skip Logic: Single category 'Profile' detected")
            if has_meme_category:
                logger.log(f"- Skip Logic: Meme-related category detected")
            
            logger.log(f"--------------------------------\n")

            await DeduplicationService.record_new_profile({
                "twitter_handle": screen_name,
                "notion_page_id": None,
                "category": "Profile"
            }, simplified_data['sourceUsername'])

            return None

        # 7. Otherwise, proceed with Notion upload
        final_cats = [cat for cat in parsed_data.get('categories', []) if cat in notion_categories]
        if not final_cats:
            final_cats = ['Unknown']

        logger.log(f"\n✅ UPLOADING to Notion: @{screen_name}")
        logger.log(f"- AI Name: {parsed_data.get('name')}")
        logger.log(f"- AI Categories: {parsed_data.get('categories', [])}")
        logger.log(f"- Final Categories (filtered): {final_cats}")
        logger.log(f"- Upload Reason: Not a Profile-only or meme category")
        
        logger.log(f"\nProfile Details:")
        logger.log(f"- Twitter Name: {simplified_data['profile'].get('name', 'Unknown')}")
        logger.log(f"- Bio: {simplified_data['profile'].get('description', '')[:150]}{ '...' if len(simplified_data['profile'].get('description', '')) > 150 else ''}")
        logger.log(f"- Tweet Count: {len(simplified_data['tweets'])}")
        logger.log(f"- AI Summary: {parsed_data.get('summary', '')[:150]}{ '...' if len(parsed_data.get('summary', '')) > 150 else ''}")
        logger.log(f"--------------------------------\n")
        
        notion_entry = {
            "name": parsed_data.get('name', 'Unknown'),
            "summary": parsed_data.get('summary', ''),
            "date": datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d'),
            "content": parsed_data.get('content', ''),
            "screenName": screen_name,
            "sourceUsername": simplified_data['sourceUsername'],
            "categories": final_cats
        }

        # 8. Upload/update to Notion
        try:
            existing_profile = repository.find_by_handle(screen_name)
            existing_page_id = existing_profile.get('notion_page_id') if existing_profile else None

            days_since_last_update = None
            last_updated_raw = existing_profile.get("last_updated_date") if existing_profile else None
            if last_updated_raw:
                try:
                    last_updated_dt = datetime.fromisoformat(last_updated_raw)
                    now_dt = datetime.now(last_updated_dt.tzinfo) if last_updated_dt.tzinfo else datetime.now()
                    days_since_last_update = (now_dt - last_updated_dt).days
                except ValueError:
                    logger.warn(f"Could not parse last_updated_date for @{screen_name}: {last_updated_raw}")

            if existing_page_id:
                logger.log(f"Updating existing Notion page for @{screen_name}: {existing_page_id}")
                notion_response = await update_notion_database_entry(existing_page_id, notion_entry)
                if days_since_last_update is not None and days_since_last_update >= 28:
                    stale_notion_updates.append({
                        "username": screen_name,
                        "sourceUsername": simplified_data['sourceUsername'],
                        "notionPageId": existing_page_id,
                        "daysSinceLastUpdate": days_since_last_update,
                        "previousLastUpdatedDate": last_updated_raw,
                    })
            else:
                notion_response = await add_notion_database_entry(notion_entry)

            notion_page_id = notion_response.get('id') if isinstance(notion_response, dict) else None
            if not notion_page_id:
                notion_page_id = existing_page_id

            await DeduplicationService.record_new_profile({
                "twitter_handle": screen_name,
                "notion_page_id": notion_page_id,
                "category": "Project"
            }, simplified_data['sourceUsername'])

            return notion_response
        except Exception as notion_error:
            logger.error(f"Notion API Error Details: {notion_error}")
            raise

    except Exception as error:
        logger.error(f"Error analyzing tweets for {tweets_file_path}: {error}")

        error_username = None
        error_source_username = None
        try:
            # Attempt to extract screen_name and source_username for deduplication
            with open(tweets_file_path, 'r', encoding='utf-8') as f:
                temp_data = json.load(f)
            screen_name = temp_data['profile'].get('screen_name')
            source_username = temp_data.get('sourceUsername', 'unknown')

            error_username = screen_name
            error_source_username = source_username
            
            if screen_name:
                logger.log(f"Marking {screen_name} as processed despite error to avoid infinite retry loop")
                await DeduplicationService.record_new_profile({
                    "twitter_handle": screen_name,
                    "notion_page_id": None
                }, source_username)
        except Exception as record_error:
            logger.error(f"Failed to record error in deduplication service for {tweets_file_path}: {record_error}")

        analysis_errors.append({
            "username": error_username or "Unknown",
            "sourceUsername": error_source_username or "unknown",
            "file": os.path.basename(tweets_file_path),
            "reason": "analysis_exception",
            "error": str(error),
        })
        
        return None

async def get_following_counts(profiles):
    """
    Retrieves following counts in a batch request.
    """
    count_map = {}
    batch_size = 200
    max_retries = 3
    failed_users = set()
    
    try:
        user_ids = [p['user_id'] for p in profiles]
        
        for i in range(0, len(user_ids), batch_size):
            batch_ids = user_ids[i:i + batch_size]
            retry_count = 0
            success = False
            
            while retry_count < max_retries and not success:
                logger.log(f"🔄 Processing batch {i // batch_size + 1}/{len(user_ids) // batch_size + (1 if len(user_ids) % batch_size > 0 else 0)} ({len(batch_ids)} IDs){f' - Retry {retry_count}/{max_retries}' if retry_count > 0 else ''}")
                
                try:
                    response = await throttled_rapid_api_request(lambda: twitter_client.get_users_by_rest_ids(batch_ids))
                    
                    # Debug: Log the raw response
                    logger.log(f"API response status: {response.get('status', 'No status')}")
                    logger.log(f"API response data keys: {list(response.get('data', {}).keys()) if response.get('data') else 'No data'}")
                    
                    users_block = None
                    data_layer = response.get('data')
                    if data_layer:
                        if data_layer.get('users'):
                            users_block = data_layer['users']
                        elif isinstance(data_layer, dict) and data_layer.get('data') and data_layer['data'].get('users'):
                            users_block = data_layer['data']['users']

                    if users_block:
                        for user in users_block:
                            result = user.get('result') or {}
                            core = result.get('core') or {}
                            rel_counts = result.get('relationship_counts') or {}

                            screen_name = core.get('screen_name')
                            following_count = rel_counts.get('following')

                            if screen_name is None or following_count is None:
                                continue

                            screen_name_key = screen_name.lower()
                            count_map[screen_name_key] = following_count
                            logger.log(f"{screen_name_key}: {following_count} followings")
                        success = True
                    else:
                        raise ValueError('No users data in response')
                except requests.exceptions.HTTPError as error:
                    if error.response and error.response.status_code == 429:
                        logger.log('Rate limit hit, waiting 2 seconds before retry...')
                        await asyncio.sleep(2)
                        retry_count += 1
                    else:
                        logger.error(f"API Error for batch {i // batch_size + 1}: {error}")
                        retry_count += 1
                        if retry_count < max_retries:
                            await asyncio.sleep(5) # Wait before retry
                except Exception as error:
                    logger.error(f"Error processing batch {i // batch_size + 1}: {error}")
                    retry_count += 1
                    if retry_count < max_retries:
                        # Increase wait time for timeout errors
                        if "timeout" in str(error).lower():
                            logger.log(f"Timeout error detected, waiting 10 seconds before retry...")
                            await asyncio.sleep(10)
                        else:
                            await asyncio.sleep(5) # Wait before retry
            
            if not success:
                for uid in batch_ids:
                    failed_users.add(uid)
                logger.error(f"Failed to get counts for batch after {max_retries} retries")
        
        success_count = len(count_map)
        total_expected = len(profiles)
        failed_count = len(failed_users)
        
        logger.log(f"\nFollowing counts retrieval summary:")
        logger.log(f"- Successfully retrieved: {success_count}/{total_expected}")
        logger.log(f"- Failed to retrieve: {failed_count}")
        
        if failed_count > 0:
            logger.warn(f"Warning: {failed_count} users had failed count retrievals")
        
        return count_map
    except Exception as e:
        logger.error(f"Error checking following counts: {e}")
        return {}

async def process_username(username, is_new_username, following_counts):
    """
    Processes a single username's following changes.
    """
    try:
        current_count = following_counts.get(username.lower(), 0)
        previous_count = await get_previous_follower_count(username)
        count_diff = current_count - (previous_count or 0)

        if is_new_username:
            logger.log(' | Baseline')
            return {
                "total": current_count,
                "new": 0,
                "previousCount": 0,
                "followings": []
            }

        if count_diff == 0 and not is_new_username:
            logger.log(f"No following count change for @{username}, skipping")
            return {
                "total": current_count,
                "new": 0,
                "previousCount": previous_count or 0,
                "followings": []
            }

        fetch_count = count_diff if count_diff > 0 else 3

        retry_count = 0
        max_retries = 3
        response = None
        last_error = None

        while retry_count < max_retries:
            try:
                response = await throttled_rapid_api_request(lambda: twitter_client.get_following(username, fetch_count))

                if response['data'] and response['data'].get('error') == "Not authorized.":
                    logger.log('Profile is private')
                    return { "total": current_count, "new": 0, "previousCount": previous_count, "followings": [] }

                if not response['data'] or not response['data'].get('users'):
                    logger.error('Error: No users data in response')
                    raise ValueError('No users data in API response')

                break # Success
            except requests.exceptions.HTTPError as error:
                # Option A: Detect protected accounts from provider error body and short-circuit gracefully
                protected = False
                try:
                    if error.response is not None and error.response.text:
                        body = error.response.json()
                        if isinstance(body, dict) and body.get('error') == 'Not authorized.':
                            protected = True
                except Exception:
                    protected = False

                if protected:
                    logger.log('Protected account; skipping')
                    return { "total": current_count, "new": 0, "previousCount": previous_count, "followings": [] }

                last_error = error
                retry_count += 1
                
                error_details = {
                    "status": error.response.status_code if error.response else 'No status',
                    "message": str(error),
                    "data": error.response.json() if error.response and error.response.text else None,
                    "headers": dict(error.response.headers) if error.response else None
                }
                
                logger.log(f"\nError processing @{username} (attempt {retry_count}/{max_retries}):")
                logger.log(f"Status: {error_details['status']}")
                logger.log(f"Message: {error_details['message']}")
                if error_details['data']:
                    logger.log(f"Response data: {json.dumps(error_details['data'])}")
                
                delays = [1, 2, 4] # 1s, 2s, 4s
                delay_s = delays[retry_count - 1]
                logger.log(f"Waiting {delay_s} seconds before retry...")
                await asyncio.sleep(delay_s)
                
                if retry_count >= max_retries:
                    logger.error(f"Max retries ({max_retries}) exceeded for @{username}")
                    raise ValueError(f"Failed after {max_retries} retries: {last_error}")
            except Exception as error:
                last_error = error
                retry_count += 1
                logger.error(f"Unexpected error processing @{username} (attempt {retry_count}/{max_retries}): {error}")
                if retry_count >= max_retries:
                    raise ValueError(f"Failed after {max_retries} retries: {last_error}")
                await asyncio.sleep(1) # Small delay for other errors

        if not response or not response['data'] or not response['data'].get('users'):
            logger.error('Error: Failed to get users data after retries')
            raise ValueError('Failed to get users data after retries')

        followings = response['data']['users']
        logger.log(
            f"@{username.ljust(15)} " +
            f"{str(previous_count or 0).ljust(5) if not is_new_username else 'New'.ljust(5)} → " +
            f"{str(current_count).ljust(5)}"
        )

        if count_diff == 0 and not is_new_username:
            logger.log(' │ No changes')
        else:
            sign = '+' if count_diff > 0 else ''
            logger.log(f' │ {sign}{count_diff}')

        if not followings:
            logger.error('Error: API returned empty followings list')
            raise ValueError('API returned no followings')

        limited_followings = followings[:fetch_count]

        filtered_followings = []
        for user in limited_followings:
            screen_name = user.get('screen_name')
            
            # FIRST CHECK: Have we seen this profile before?
            dedup_check = await DeduplicationService.process_profile(screen_name, username)
            if not dedup_check['isNew']:
                seen_within_days = dedup_check.get("seenWithinDays", 28)
                days_since_last_seen = dedup_check.get("daysSinceLastSeen")
                if days_since_last_seen is not None:
                    logger.log(f"    Skip @{screen_name} - Seen {days_since_last_seen} days ago (within {seen_within_days}d)")
                else:
                    logger.log(f"    Skip @{screen_name} - Seen within {seen_within_days} days")
                continue  # Skip to next user
            
            days_since_last_seen = dedup_check.get("daysSinceLastSeen")
            if days_since_last_seen is not None:
                logger.log(f"    Include @{screen_name} - Last seen {days_since_last_seen} days ago")
            else:
                logger.log(f"    Include @{screen_name}")
            filtered_followings.append(user)

        # Note: We already deduplicate using the database (line 620)
        # No need for file-based deduplication since following_history files are legacy
        new_followings = filtered_followings

        return {
            "total": current_count,
            "new": len(new_followings),
            "previousCount": previous_count or 0,
            "followings": new_followings
        }
    except Exception as e:
        logger.error(f"Error processing username {username}: {e}")
        raise


async def collect_tweets_for_new_followers(new_followings, source_username):
    """
    Collects tweets for new followings and saves to JSON.
    """
    logger.log(f"\n🔍 Collecting tweets for {len(new_followings)} new followings from @{source_username}:")

    max_concurrent_requests = MAX_CONCURRENT_REQUESTS
    logger.log(f"Using concurrent processing with up to {max_concurrent_requests} simultaneous requests (conservative rate limiting)")
    
    batches = [new_followings[i:i + max_concurrent_requests] for i in range(0, len(new_followings), max_concurrent_requests)]
    
    processed_count = 0
    success_count = 0
    error_count = 0
    skipped_count = 0
    
    # Process batches of users concurrently
    for batch_index, batch in enumerate(batches):
        logger.log(f"Processing batch {batch_index + 1}/{len(batches)} ({len(batch)} users)")
        
        # Process all users in this batch concurrently
        async def process_single_user(user):
            nonlocal processed_count, success_count, error_count, skipped_count
            processed_count += 1
            try:
                # Log basic user info for debugging
                logger.log(f"Starting process for @{user.get('screen_name')} (ID: {user.get('id_str')})")
                
                # No need to filter or check duplicates - already done in process_username
                logger.log(f"    {processed_count}/{len(new_followings)} Processing @{user.get('screen_name')}")

                user_id = user.get('id_str')
                if not user_id:
                    logger.log(f"    {processed_count}/{len(new_followings)} ❌ @{user.get('screen_name')}: No valid ID")
                    error_count += 1
                    return { "user": user, "status": 'error', "reason": 'no_user_id' }

                desired_tweet_target = 5
                tweets = []
                timeline_response = None
                is_retryable_error = False

                def find_bottom_cursor(payload):
                    """
                    Recursively search the timeline payload for the Bottom cursor value.
                    """
                    if isinstance(payload, dict):
                        if payload.get('__typename') == 'TimelineTimelineCursor' and payload.get('cursor_type') == 'Bottom':
                            return payload.get('value')
                        for value in payload.values():
                            cursor = find_bottom_cursor(value)
                            if cursor:
                                return cursor
                    elif isinstance(payload, list):
                        for item in payload:
                            cursor = find_bottom_cursor(item)
                            if cursor:
                                return cursor
                    return None

                try:
                    logger.log(f"Fetching tweets for @{user.get('screen_name')} using UserTweets endpoint")
                    timeline_response = await throttled_rapid_api_request(lambda: twitter_client.get_user_tweets(user_id))

                    logger.log(f"UserTweets API raw response status: {timeline_response.get('status')}")
                    logger.log(f"UserTweets API raw response headers: {json.dumps(timeline_response.get('headers'))}")
                    logger.log(f"UserTweets API response keys: {list(timeline_response.get('data', {}).keys())}")

                    timeline_data = timeline_response.get('data')
                    if timeline_data and timeline_data.get('status') and timeline_data.get('status') != 'ok':
                        logger.log(f"UserTweets API error response: {json.dumps(timeline_data)}")

                    if timeline_data and timeline_data.get('error'):
                        logger.error(f"API returned error for @{user.get('screen_name')}: {timeline_data['error']}")
                        logger.error(f"Status code: {timeline_data.get('statusCode') or timeline_response.get('status')}")
                        raise ValueError(f"API error: {timeline_data['error']}")

                    tweets = extract_tweets_from_response(timeline_data)
                    logger.log(f"Retrieved {len(tweets)} tweets from UserTweets endpoint for @{user.get('screen_name')}")

                    raw_response_file = os.path.join(RAW_RESPONSES_DIR, f"{user.get('screen_name')}_raw_response_UserTweets_{datetime.now().isoformat().replace(':', '-').replace('.', '-')}.json")
                    with open(raw_response_file, 'w', encoding='utf-8') as f:
                        json.dump(timeline_data, f, indent=2)
                    logger.log(f"Saved raw UserTweets response to {raw_response_file}")

                    if len(tweets) < desired_tweet_target:
                        bottom_cursor = find_bottom_cursor(timeline_data)
                        if bottom_cursor:
                            logger.log(f"Retrieved {len(tweets)} tweets; fetching next page with cursor for @{user.get('screen_name')}")
                            next_page_response = await throttled_rapid_api_request(lambda: twitter_client.get_user_tweets(user_id, bottom_cursor))
                            next_page_data = next_page_response.get('data')
                            next_page_tweets = extract_tweets_from_response(next_page_data)
                            logger.log(f"Retrieved {len(next_page_tweets)} additional tweets from paginated UserTweets call for @{user.get('screen_name')}")
                            
                            if next_page_tweets:
                                tweets.extend(next_page_tweets)

                            raw_response_file_cursor = os.path.join(RAW_RESPONSES_DIR, f"{user.get('screen_name')}_raw_response_UserTweets_cursor_{datetime.now().isoformat().replace(':', '-').replace('.', '-')}.json")
                            with open(raw_response_file_cursor, 'w', encoding='utf-8') as f:
                                json.dump(next_page_data, f, indent=2)
                            logger.log(f"Saved paginated UserTweets response to {raw_response_file_cursor}")
                        else:
                            logger.log(f"No pagination cursor found in UserTweets response for @{user.get('screen_name')}")

                except requests.exceptions.HTTPError as error:
                    is_retryable_error = error.response and error.response.status_code in [500, 503, 504]
                    if is_retryable_error:
                        logger.warn(f"UserTweets endpoint failed with status {error.response.status_code}, retrying with UserTweetsAndReplies...")
                    else:
                        logger.error(f"Error fetching tweets for @{user.get('screen_name')}: {error}")
                        if error.response:
                            logger.error(f"Status: {error.response.status_code}, Data: {error.response.text[:500]}...")
                        error_count += 1
                        return { "user": user, "status": 'error', "reason": 'api_error', "error": str(error) }
                except Exception as error:
                    logger.error(f"Error fetching tweets from UserTweets for @{user.get('screen_name')}: {error}")
                    error_count += 1
                    return { "user": user, "status": 'error', "reason": 'api_error', "error": str(error) }

                if not tweets or is_retryable_error:
                    logger.log(f"No tweets found or retryable error, trying UserTweetsAndReplies endpoint for @{user.get('screen_name')}")
                    await asyncio.sleep(0.5) # Smaller delay
                    
                    try:
                        logger.log(f"UserTweetsAndReplies API call parameters: {{'user_id': '{user_id}'}}")
                        logger.log(f"User ID being used: {user_id} (type: {type(user_id)})")
                        
                        timeline_response = await throttled_rapid_api_request(lambda: twitter_client.get_user_tweets_and_replies(user_id))
                        
                        logger.log(f"UserTweetsAndReplies API raw response status: {timeline_response.get('status')}")
                        logger.log(f"UserTweetsAndReplies API raw response headers: {json.dumps(timeline_response.get('headers'))}")
                        logger.log(f"UserTweetsAndReplies API response keys: {list(timeline_response.get('data', {}).keys())}")
                        if timeline_response.get('data') and timeline_response['data'].get('status') and timeline_response['data']['status'] != 'ok':
                            logger.log(f"UserTweetsAndReplies API error response: {json.dumps(timeline_response['data'])}")

                        if timeline_response.get('data') and timeline_response['data'].get('error'):
                            logger.error(f"API returned error for @{user.get('screen_name')}: {timeline_response['data']['error']}")
                            logger.error(f"Status code: {timeline_response['data'].get('statusCode') or timeline_response.get('status')}")
                            raise ValueError(f"API error: {timeline_response['data']['error']}")
                        
                        timeline_data_replies = timeline_response.get('data')
                        tweets = extract_tweets_from_response(timeline_data_replies)
                        logger.log(f"Retrieved {len(tweets)} tweets from UserTweetsAndReplies endpoint for @{user.get('screen_name')}")

                        raw_response_file = os.path.join(RAW_RESPONSES_DIR, f"{user.get('screen_name')}_raw_response_UserTweetsAndReplies_{datetime.now().isoformat().replace(':', '-').replace('.', '-')}.json")
                        with open(raw_response_file, 'w', encoding='utf-8') as f:
                            json.dump(timeline_data_replies, f, indent=2)
                        logger.log(f"Saved raw UserTweetsAndReplies response to {raw_response_file}")

                        if len(tweets) < desired_tweet_target:
                            bottom_cursor_replies = find_bottom_cursor(timeline_data_replies)
                            if bottom_cursor_replies:
                                logger.log(f"Retrieved {len(tweets)} tweets; fetching next page of UserTweetsAndReplies with cursor for @{user.get('screen_name')}")
                                next_page_resp_replies = await throttled_rapid_api_request(lambda: twitter_client.get_user_tweets_and_replies(user_id, bottom_cursor_replies))
                                next_page_data_replies = next_page_resp_replies.get('data')
                                next_page_tweets_replies = extract_tweets_from_response(next_page_data_replies)
                                logger.log(f"Retrieved {len(next_page_tweets_replies)} additional tweets from paginated UserTweetsAndReplies call for @{user.get('screen_name')}")
                                if next_page_tweets_replies:
                                    tweets.extend(next_page_tweets_replies)

                                raw_response_file_cursor = os.path.join(RAW_RESPONSES_DIR, f"{user.get('screen_name')}_raw_response_UserTweetsAndReplies_cursor_{datetime.now().isoformat().replace(':', '-').replace('.', '-')}.json")
                                with open(raw_response_file_cursor, 'w', encoding='utf-8') as f:
                                    json.dump(next_page_data_replies, f, indent=2)
                                logger.log(f"Saved paginated UserTweetsAndReplies response to {raw_response_file_cursor}")
                            else:
                                logger.log(f"No pagination cursor found in UserTweetsAndReplies response for @{user.get('screen_name')}")

                    except Exception as error:
                        logger.error(f"Error fetching tweets from UserTweetsAndReplies for @{user.get('screen_name')}: {error}")
                        error_count += 1
                        return { "user": user, "status": 'error', "reason": 'api_error', "error": str(error) }

                user_profile = {
                    "profile": user,
                    "tweets": tweets,
                    "sourceUsername": source_username
                }

                tweets_file = os.path.join(TWEETS_DIR, f"{user.get('screen_name')}_tweets.json")
                with open(tweets_file, 'w', encoding='utf-8') as f:
                    json.dump(user_profile, f, indent=2)
                logger.log(f"    {processed_count}/{len(new_followings)} ✅ @{user.get('screen_name')} - Retrieved {len(tweets)} tweets")
                success_count += 1
                return { "user": user, "status": 'success', "tweetCount": len(tweets) }
            except Exception as error:
                logger.error(f"Unexpected error processing @{user.get('screen_name')}: {error}")
                if hasattr(error, 'stack'):
                    logger.error(f"Stack trace: {error.stack}")
                error_count += 1
                return { "user": user, "status": 'error', "reason": 'unexpected', "error": str(error) }
        # Create batch promises using list comprehension to ensure proper closure
        batch_promises = [process_single_user(user) for user in batch]
        
        # Process batch without timeout - just like JavaScript
        batch_results = await asyncio.gather(*batch_promises, return_exceptions=True)
        
        # Handle results, including exceptions
        processed_results = []
        for i, result in enumerate(batch_results):
            if isinstance(result, Exception):
                logger.error(f"Exception processing user {batch[i].get('screen_name', 'Unknown')}: {result}")
                processed_results.append({'user': batch[i], 'status': 'error', 'reason': 'exception', 'error': str(result)})
                error_count += 1
            else:
                processed_results.append(result)
        
        batch_successes = sum(1 for r in processed_results if isinstance(r, dict) and r.get('status') == 'success')
        batch_errors = sum(1 for r in processed_results if isinstance(r, dict) and r.get('status') == 'error')
        batch_skipped = sum(1 for r in processed_results if isinstance(r, dict) and r.get('status') == 'skipped')
        
        logger.log(f"Batch {batch_index + 1} complete: {batch_successes} successful, {batch_errors} errors, {batch_skipped} skipped")
        
        if batch_index < len(batches) - 1:
            logger.log(f"Waiting 0.1 seconds between batches...")
            await asyncio.sleep(0.1)
    
    logger.log(f"\n📊 Tweet collection complete:")
    logger.log(f"   Total processed: {processed_count}")
    logger.log(f"   Successful: {success_count}")
    logger.log(f"   Errors: {error_count}")
    logger.log(f"   Skipped: {skipped_count}")

async def clean_directory(directory_path, create_if_not_exists=True):
    """
    Cleans a specified directory by removing all files within it.
    """
    try:
        if not await file_exists(directory_path):
            if create_if_not_exists:
                os.makedirs(directory_path, exist_ok=True)
                logger.log(f'{os.path.basename(directory_path)} directory does not exist, creating it...')
            return

        files = os.listdir(directory_path)
        cleaned_count = 0
        error_count = 0

        for file_name in files:
            file_path = os.path.join(directory_path, file_name)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    cleaned_count += 1
            except Exception as e:
                error_count += 1
                logger.warn(f"Could not delete file {file_name} from {os.path.basename(directory_path)}: {e}")

        logger.log(f"Cleaned {os.path.basename(directory_path)}: {cleaned_count} files deleted, {error_count} errors")
    except Exception as e:
        logger.error(f"Error cleaning {os.path.basename(directory_path)}: {e}")

async def clean_ai_tweets_directory():
    """
    Cleans the AI tweets directory by removing and recreating it.
    """
    try:
        logger.log(f"Attempting to clean directory: {AI_TWEETS_DIR}")
        if os.path.exists(AI_TWEETS_DIR):
            import shutil
            shutil.rmtree(AI_TWEETS_DIR) # Use shutil.rmtree for recursive deletion
            logger.log(f"Removed directory: {AI_TWEETS_DIR}")
        
        os.makedirs(AI_TWEETS_DIR, exist_ok=True)
        logger.log(f"Recreated directory: {AI_TWEETS_DIR}")
        
        logger.log(f"Cleaned AI tweets directory successfully.")
    except Exception as e:
        logger.error(f"Error cleaning AI tweets directory: {e}")

def format_est_date_time():
    """
    Formats the current EST date and time into a string for filenames.
    """
    now = datetime.now(pytz.timezone('America/New_York'))
    return now.strftime('%Y-%m-%d_%H-%M-%S')

async def get_previous_follower_count(username):
    """
    Read previous follower count from CSV files
    """
    try:
        all_items = os.listdir(FOLLOWER_COUNTS_DIR)
        # Filter to only CSV files that match the expected pattern
        csv_files = [f for f in all_items if f.endswith('.csv') and f.startswith('follower_counts_')]
        
        if not csv_files:
            return None
        
        # Sort to get the most recent file
        sorted_files = sorted(csv_files, reverse=True)
        previous_file = sorted_files[0]
        
        with open(os.path.join(FOLLOWER_COUNTS_DIR, previous_file), 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if row[0].lower() == username.lower():
                    return int(row[1])
        return None
    except Exception as e:
        logger.error(f"Error getting previous count for {username}: {e}")
        return None

async def save_follower_counts(count_map):
    """
    Save follower counts to dated CSV file with backup
    """
    try:
        if not count_map:
            logger.error('No follower counts to save')
            return None
        
        # Validate counts
        invalid_counts = []
        for username, count in list(count_map.items()):
            if not isinstance(count, int) or count < 0:
                invalid_counts.append(username)
                del count_map[username]
        
        if invalid_counts:
            logger.warn(f"Found {len(invalid_counts)} invalid counts. Removing them before saving.")
            logger.warn(f"Invalid usernames: {', '.join(invalid_counts)}")
        
        if not count_map:
            logger.error('No valid follower counts to save after validation')
            return None
        
        date_time = format_est_date_time()
        filename = os.path.join(FOLLOWER_COUNTS_DIR, f"follower_counts_{date_time}.csv")
        
        # Create backup of previous file if it exists
        all_items = os.listdir(FOLLOWER_COUNTS_DIR)
        if all_items:
            # Filter to only CSV files and exclude backup files
            non_backup_files = [f for f in all_items if f.endswith('.csv') and f.startswith('follower_counts_') and not f.startswith('backup_')]
            if non_backup_files:
                sorted_files = sorted(non_backup_files, reverse=True)
                previous_file = sorted_files[0]
                backup_file = os.path.join(FOLLOWER_COUNTS_DIR, f"backup_{previous_file}")
                try:
                    shutil.copy(
                        os.path.join(FOLLOWER_COUNTS_DIR, previous_file),
                        backup_file
                    )
                except PermissionError:
                    # Fallback: Read and write manually
                    with open(os.path.join(FOLLOWER_COUNTS_DIR, previous_file), 'r') as src:
                        content = src.read()
                    with open(backup_file, 'w') as dst:
                        dst.write(content)
                logger.log(f"Created backup of previous counts file: {backup_file}")
        
        # Write new counts
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for username, count in count_map.items():
                writer.writerow([username, count])
        
        logger.log(f"Saved {len(count_map)} follower counts to {filename}")
        return filename
    except Exception as e:
        logger.error(f'Error saving follower counts: {e}')
        return None

# NOTE: Removed get_previous_followings and save_following_history functions
# These were for file-based deduplication using following_history directory
# We now use database-based deduplication which is superior

async def main():
    global notion_categories, skipped_profiles, analysis_errors, stale_notion_updates # Declare intent to modify global lists

    # Reset per-run tracking (important for tests or repeated invocations in the same process)
    skipped_profiles = []
    analysis_errors = []
    stale_notion_updates = []

    if not RAPID_API_KEY:
        logger.error('RAPID_API_KEY is not set in environment variables')
        raise ValueError('RAPID_API_KEY is not set in environment variables')

    total_processed = 0
    total_skipped = 0
    total_uploaded = 0
    resume_mode = False
    processed_files = []
    seen_handles = set()
    s3_sync = None

    await setup_directories()
    
    # Initialize S3 sync and download if enabled
    s3_sync = None
    if USE_S3_SYNC:
        try:
            logger.log("🔄 S3 sync enabled - checking for updates...")
            s3_sync = S3DatabaseSync()
            # Smart download - only if S3 is newer
            await s3_sync.smart_download()
            # Download latest follower counts if we don't have them
            await s3_sync.download_latest_counts()
        except Exception as e:
            logger.error(f"Failed to initialize S3 sync: {e}")
            # Continue without S3 sync if it fails
            s3_sync = None
    
    # Check for recovery marker file
    try:
        if await file_exists(RECOVERY_FILE):
            with open(RECOVERY_FILE, 'r', encoding='utf-8') as f:
                recovery_data = json.load(f)
            if recovery_data.get('processedFiles') and isinstance(recovery_data['processedFiles'], list):
                processed_files = recovery_data['processedFiles']
                total_processed = recovery_data.get('totalProcessed', 0)
                total_skipped = recovery_data.get('totalSkipped', 0)
                total_uploaded = recovery_data.get('totalUploaded', 0)
                resume_mode = True
                logger.log(f"Recovery mode: Found {len(processed_files)} already processed files")
    except Exception as err:
        logger.log(f"Could not load recovery data: {err}")

    # Only clean directories if we're not in recovery mode
    if not resume_mode:
        await clean_directory(TWEETS_DIR)
        await clean_directory(RAW_RESPONSES_DIR)
        await clean_ai_tweets_directory()

    # Initialize Notion categories once
    await initialize_notion_categories()
    notion_categories = await get_existing_categories()

    try:
        logger.log('Initializing Twitter API client...')

        # 1) Read input CSV (screen_name, user_id)
        current_profiles = await read_input_usernames()
        profile_count = len(current_profiles)

        if profile_count == 0:
            logger.error('No profiles to process')
            return

        if profile_count > MAX_PROFILES:
            logger.error(f'Profile count ({profile_count}) exceeds MAX_PROFILES ({MAX_PROFILES})')
            return

        logger.log(f'Processing {profile_count} profiles...')

        # 2) Get initial following counts
        following_counts = await get_following_counts(current_profiles)

        # 3) Process each profile
        for profile in current_profiles:
            try:
                previous_count = await get_previous_follower_count(profile['screen_name'])
                current_count = following_counts.get(profile['screen_name'].lower())
                
                if current_count is None:
                    logger.warn(f"Could not retrieve current following count for @{profile['screen_name']}. Skipping profile.")
                    continue

                count_diff = current_count - (previous_count or 0)
                is_new_username = previous_count is None

                logger.log(
                    f"@{profile['screen_name'].ljust(15)} " +
                    f"{str(previous_count).ljust(5) if not is_new_username else 'New'.ljust(5)} → " +
                    f"{str(current_count).ljust(5)}"
                )

                if count_diff == 0 and not is_new_username:
                    logger.log(' │ No changes')
                    continue

                try:
                    results = await process_username(profile['screen_name'], is_new_username, following_counts)

                    # Collect tweets for new followers if any were found
                    if results['followings']:
                        followings_to_collect = []
                        batch_seen = set()

                        for user in results['followings']:
                            screen_name = user.get('screen_name')
                            if not screen_name:
                                followings_to_collect.append(user)
                                continue

                            handle_key = screen_name.lower()

                            if handle_key in seen_handles:
                                logger.log(f"    Skip @{screen_name} - Already queued this run")
                                continue

                            if handle_key in batch_seen:
                                logger.log(f"    Skip @{screen_name} - Duplicate within this batch")
                                continue

                            batch_seen.add(handle_key)
                            followings_to_collect.append(user)

                        if followings_to_collect:
                            await collect_tweets_for_new_followers(followings_to_collect, profile['screen_name'])

                            for user in followings_to_collect:
                                screen_name = user.get('screen_name')
                                if not screen_name:
                                    continue

                                tweets_file = os.path.join(TWEETS_DIR, f"{screen_name}_tweets.json")
                                if os.path.exists(tweets_file):
                                    seen_handles.add(screen_name.lower())
                except Exception as error:
                    logger.error(f" │ Error processing profile @{profile['screen_name']}: {error}")
                    continue
            except Exception as error:
                logger.error(f" │ Unexpected error during profile iteration for @{profile['screen_name']}: {error}")

        # 4) Save the new counts
        await save_follower_counts(following_counts)

        # 5) Analyze newly created tweet files with AI
        tweet_files = [f for f in os.listdir(TWEETS_DIR) if f.endswith('_tweets.json')]

        if tweet_files:
            file_complexity = await asyncio.gather(*[
                get_file_complexity(os.path.join(TWEETS_DIR, file)) for file in tweet_files
            ])
            
            sorted_files = [item['file'] for item in sorted(file_complexity, key=lambda x: x['complexity'])]
            
            logger.log(f"Files sorted by complexity (smallest first) to optimize token usage")
            
            concurrent_processes = CONCURRENT_PROCESSES
            
            if concurrent_processes <= 1:
                logger.log(f"\nAnalyzing {len(sorted_files)} files sequentially (concurrent processing disabled)")
                
                for file in sorted_files:
                    if resume_mode and file in processed_files:
                        logger.log(f"Skipping already processed file: {file}")
                        continue
                    
                    total_processed += 1
                    file_path = os.path.join(TWEETS_DIR, file)
                    
                    file_throttler = create_throttler()
                    
                    try:
                        logger.log(f"[{total_processed}/{len(sorted_files)}] Processing: {file}")
                        start_time = time.time()
                        
                        result = await analyze_tweets_with_ai(file_path, file_throttler)
                        
                        processing_time = (time.time() - start_time)
                        
                        if result is None:
                            total_skipped += 1
                            logger.log(f"[{total_processed}/{len(sorted_files)}] ⏩ Skipped: {file} ({processing_time:.1f}s)")
                        else:
                            total_uploaded += 1
                            logger.log(f"[{total_processed}/{len(sorted_files)}] ✅ Uploaded: {file} ({processing_time:.1f}s)")
                    except Exception as error:
                        logger.error(f"Error analyzing tweets: {error}")
                        total_skipped += 1
                        analysis_errors.append({
                            "username": file.replace("_tweets.json", ""),
                            "sourceUsername": "unknown",
                            "file": file,
                            "reason": "analysis_task_exception",
                            "error": str(error),
                        })
                        logger.log(f"[{total_processed}/{len(sorted_files)}] ⏩ Skipped: {file} (error)")
            else:
                logger.log(f"\nAnalyzing {len(sorted_files)} files concurrently (concurrent processing enabled)")
                
                async def process_single_tweet_file(file, file_index):
                    if resume_mode and file in processed_files:
                        logger.log(f"Skipping already processed file: {file}")
                        return { "file": file, "status": 'skipped', "reason": 'already_processed' }
                    
                    file_path = os.path.join(TWEETS_DIR, file)
                    
                    file_throttler = create_throttler()
                    
                    try:
                        logger.log(f"[{file_index + 1}/{len(sorted_files)}] Processing: {file}")
                        start_time = time.time()
                        
                        result = await analyze_tweets_with_ai(file_path, file_throttler)
                        
                        processing_time = (time.time() - start_time)
                        
                        if result is None:
                            logger.log(f"[{file_index + 1}/{len(sorted_files)}] ⏩ Skipped: {file} ({processing_time:.1f}s)")
                            return { "file": file, "status": 'skipped', "reason": 'analysis_skipped' }
                        else:
                            logger.log(f"[{file_index + 1}/{len(sorted_files)}] ✅ Uploaded: {file} ({processing_time:.1f}s)")
                            return { "file": file, "status": 'success' }
                    except Exception as error:
                        logger.error(f"Error analyzing tweets: {error}")
                        analysis_errors.append({
                            "username": file.replace("_tweets.json", ""),
                            "sourceUsername": "unknown",
                            "file": file,
                            "reason": "analysis_task_exception",
                            "error": str(error),
                        })
                        logger.log(f"[{file_index + 1}/{len(sorted_files)}] ⏩ Skipped: {file} (error)")
                        return { "file": file, "status": 'error', "reason": 'analysis_error', "error": str(error) }

                # Create tasks for concurrent processing
                tasks = [process_single_tweet_file(file, idx) for idx, file in enumerate(sorted_files)]
                
                # Log that we're starting concurrent processing
                logger.log(f"Starting concurrent processing of {len(tasks)} files...")
                
                # Process all files concurrently like JavaScript does
                # JavaScript uses Promise.all() which is equivalent to asyncio.gather()
                results = await asyncio.gather(*tasks, return_exceptions=True)

                normalized_results = []
                for r in results:
                    if isinstance(r, Exception):
                        analysis_errors.append({
                            "username": "Unknown",
                            "sourceUsername": "unknown",
                            "file": "unknown",
                            "reason": "analysis_task_exception",
                            "error": str(r),
                        })
                        normalized_results.append({
                            "file": "unknown",
                            "status": "error",
                            "reason": "task_exception",
                            "error": str(r),
                        })
                    else:
                        normalized_results.append(r)

                successful_uploads = sum(1 for r in normalized_results if r.get("status") == "success")
                skipped_uploads = sum(1 for r in normalized_results if r.get("status") == "skipped")
                failed_uploads = sum(1 for r in normalized_results if r.get("status") == "error")
                
                # Update totals based on results
                total_processed = len(normalized_results)
                total_uploaded = successful_uploads
                total_skipped = skipped_uploads + failed_uploads
                
                logger.log(f"\nConcurrent processing complete:")
                logger.log(f"   Total processed: {total_processed}")
                logger.log(f"   Successful uploads: {successful_uploads}")
                logger.log(f"   Skipped uploads: {skipped_uploads}")
                logger.log(f"   Failed uploads: {failed_uploads}")

    except Exception as error:
        logger.error(f"Error in main function: {error}")
    
    if skipped_profiles:
        logger.log(f"\n📊 CLASSIFICATION SUMMARY - Profiles Skipped:")
        logger.log(f"Total skipped: {len(skipped_profiles)}")
        
        reason_counts = {}
        for profile in skipped_profiles:
            reason_counts[profile['reason']] = reason_counts.get(profile['reason'], 0) + 1
        
        logger.log(f"\nSkip Reasons Breakdown:")
        for reason, count in reason_counts.items():
            logger.log(f"- {reason}: {count} profiles")
        
        logger.log(f"\nDetailed Skip List:")
        for index, profile in enumerate(skipped_profiles):
            description = profile.get('description', '') or ''
            logger.log(f"{index + 1}. @{profile['username']} - Reason: {profile['reason']}")
            logger.log(f"   Categories: {profile['categories']}")
            logger.log(f"   Bio: {description[:100]}{ '...' if len(description) > 100 else ''}")

    profile_skips = sum(
        1 for profile in skipped_profiles if (profile.get("reason") or "").strip().lower() == "profile"
    )
    error_skips = len(analysis_errors)
    other_skips = max(total_skipped - profile_skips - error_skips, 0)

    stale_updates_sorted = sorted(
        stale_notion_updates,
        key=lambda u: u.get("daysSinceLastUpdate") or 0,
        reverse=True,
    )

    logger.log(f"\n📈 FINAL RUN SUMMARY:")
    logger.log(f"───────────────────────────────────────")
    logger.log(f"Total Profiles Processed: {total_processed}")
    logger.log(f"Total Profiles Uploaded to Notion: {total_uploaded}")
    logger.log(f"Total Profiles Skipped: {total_skipped}")
    logger.log(f"Skip Breakdown: Profile={profile_skips}, Other={other_skips}, Error={error_skips}")
    logger.log(f"Stale Profiles Updated in Notion (>=28d): {len(stale_updates_sorted)}")
    for update in stale_updates_sorted:
        days_since_last_update = update.get("daysSinceLastUpdate")
        logger.log(f"   - @{update.get('username')} ({days_since_last_update}d)")

    if total_processed > 0:
        upload_rate = (total_uploaded / total_processed) * 100
        skip_rate = (total_skipped / total_processed) * 100
        logger.log(f"Upload Rate: {upload_rate:.1f}%")
        logger.log(f"Skip Rate: {skip_rate:.1f}%")
    logger.log(f"───────────────────────────────────────\n")
    
    # Upload updated database and follower counts to S3 if enabled and run completed successfully
    if USE_S3_SYNC and s3_sync:
        try:
            logger.log("🔄 Uploading updated database to S3...")
            await s3_sync.upload_changes()
            logger.log("✅ Database successfully synced to S3")
            
            # Also sync the newest follower counts file
            logger.log("🔄 Syncing follower counts to S3...")
            await s3_sync.sync_follower_counts()
            logger.log("✅ Follower counts synced to S3")
        except Exception as e:
            logger.error(f"Failed to upload to S3: {e}")
            # Don't fail the entire run if S3 upload fails
    
    return {
        "totalProcessed": total_processed,
        "totalSkipped": total_skipped,
        "totalUploaded": total_uploaded,
        "skipBreakdown": {
            "profile": profile_skips,
            "other": other_skips,
            "error": error_skips,
        },
        "staleNotionUpdates": stale_updates_sorted,
        "errors": analysis_errors,
    }

async def get_file_complexity(file_path):
    """
    Calculates a complexity score for a tweet file based on size and text content length.
    """
    try:
        stats = os.stat(file_path)
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        data = json.loads(content)
        
        text_length = 0
        if data.get('profile') and data['profile'].get('description'):
            text_length += len(data['profile']['description'])
        
        if data.get('tweets') and isinstance(data['tweets'], list):
            for tweet in data['tweets']:
                if isinstance(tweet, dict):
                    if tweet.get('text'):
                        text_length += len(tweet['text'])
                    elif tweet.get('full_text'):
                        text_length += len(tweet['full_text'])
                elif isinstance(tweet, str):
                    text_length += len(tweet)
        
        return {
            "file": os.path.basename(file_path),
            "size": stats.st_size,
            "textLength": text_length,
            "complexity": stats.st_size + (text_length * 2) # Weigh text length more heavily
        }
    except Exception as e:
        logger.error(f"Error analyzing complexity of {file_path}: {e}")
        return { "file": os.path.basename(file_path), "size": float('inf'), "textLength": 0, "complexity": float('inf') }

async def run_with_email_notification():
    """
    Runs the main bot without email notifications.
    Email functionality has been disabled.
    """
    try:
        logger.log('🚀 Starting Twitter Analysis Bot...')
        stats = await main()
        
        # Email functionality disabled - no notifications will be sent
        # await send_completion_email(stats)
        
        logger.log('✅ Twitter Analysis Bot completed successfully')
    except Exception as error:
        logger.error(f'❌ Twitter Analysis Bot failed: {error}')
        # Email functionality disabled - errors are only logged

if __name__ == "__main__":
    asyncio.run(run_with_email_notification())
