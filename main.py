

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
    INPUT_FILE, HISTORY_DIR, OUTPUT_DIR, TWEETS_DIR, INPUT_HISTORY_DIR,
    PROMPTS_DIR, FOLLOWER_COUNTS_DIR, LOGS_DIR, RAW_RESPONSES_DIR, AI_TWEETS_DIR,
    MAX_PROFILES, RAPID_API_KEY, RAPID_API_HOST, RAPID_API_REQUESTS_PER_SECOND,
    OPENAI_API_KEY, OPENAI_MAX_RETRIES, OPENAI_TIMEOUT_MS, OPENAI_REQUESTS_PER_MINUTE,
    OPENAI_MODEL, MAX_FOLLOWERS, MAX_FOLLOWING, MAX_ACCOUNT_AGE_DAYS,
    MAX_CONCURRENT_REQUESTS, CONCURRENT_PROCESSES, DEBUG_MODE, RECOVERY_FILE
)

from api.twitter_client import TwitterClient, throttled_rapid_api_request
from api.twitter_parser import simplify_twitter_data, extract_tweets_from_response
from api.notion_client import initialize_notion_categories, get_existing_categories, add_notion_database_entry
from api.openai_client import get_openai_client, create_throttler
from services.deduplication_service import DeduplicationService
# from services.email_service import send_completion_email  # Email functionality disabled

# Initialize clients
twitter_client = TwitterClient()
openai_client = get_openai_client()

# Global reference to Notion categories
notion_categories = ['Unknown']

# Global list for skipped profiles
skipped_profiles = []

# Utility function to check if a file exists
async def file_exists(file_path):
    return os.path.exists(file_path)

async def setup_directories():
    """
    Initializes all necessary directories.
    """
    for directory in [HISTORY_DIR, OUTPUT_DIR, TWEETS_DIR, INPUT_HISTORY_DIR,
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
    global skipped_profiles # Declare intent to modify global list

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
                                "content": prompt + "\n\nBe extremely strict in classifying firms and individuals as Profile. When in doubt about whether something is a firm (VC/investment entity), classify it as Profile. Apply extra scrutiny to ensure no firm or individual gets misclassified. IMPORTANT: Your response MUST be a valid JSON object with no extra text or formatting before or after it."
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
                    return None
            else:
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
                "notion_page_id": None
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

        # 8. Upload to Notion
        try:
            notion_response = await add_notion_database_entry(notion_entry)

            await DeduplicationService.record_new_profile({
                "twitter_handle": screen_name,
                "notion_page_id": notion_response['id']
            }, simplified_data['sourceUsername'])

            return notion_response
        except Exception as notion_error:
            logger.error(f"Notion API Error Details: {notion_error}")
            raise

    except Exception as error:
        logger.error(f"Error analyzing tweets for {tweets_file_path}: {error}")
        
        try:
            # Attempt to extract screen_name and source_username for deduplication
            with open(tweets_file_path, 'r', encoding='utf-8') as f:
                temp_data = json.load(f)
            screen_name = temp_data['profile'].get('screen_name')
            source_username = temp_data.get('sourceUsername', 'unknown')
            
            if screen_name:
                logger.log(f"Marking {screen_name} as processed despite error to avoid infinite retry loop")
                await DeduplicationService.record_new_profile({
                    "twitter_handle": screen_name,
                    "notion_page_id": None
                }, source_username)
        except Exception as record_error:
            logger.error(f"Failed to record error in deduplication service for {tweets_file_path}: {record_error}")
        
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
                    
                    # Check if response has the expected structure
                    if response.get('data') and 'data' in response['data']:
                        # Double nested data structure
                        actual_data = response['data']['data']
                        if actual_data and actual_data.get('users'):
                            response['data'] = actual_data  # Flatten the structure
                    
                    if response['data'] and response['data'].get('users'):
                        for user in response['data']['users']:
                            if user.get('result') and user['result'].get('legacy'):
                                screen_name = user['result']['legacy']['screen_name'].lower()
                                following_count = user['result']['legacy']['friends_count']
                                count_map[screen_name] = following_count
                                logger.log(f"{screen_name}: {following_count} followings")
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
            follower_count = user.get('followers_count', 0)
            following_count = user.get('friends_count', 0)
            created_at_str = user.get('created_at')
            
            account_age = 91  # Default to old account (> 90 days)
            if created_at_str:
                try:
                    # Parse date string from Twitter API (e.g., 'Mon Apr 29 00:00:00 +0000 2024')
                    # Python's datetime.strptime is more robust for this format
                    created_at_dt = datetime.strptime(created_at_str, '%a %b %d %H:%M:%S %z %Y')
                    now_est = datetime.now(pytz.timezone('America/New_York'))
                    # Convert created_at to EST for consistent comparison
                    created_at_est = created_at_dt.astimezone(pytz.timezone('America/New_York'))
                    time_diff = now_est - created_at_est
                    account_age = time_diff.days
                except ValueError:
                    logger.warn(f"Could not parse created_at date for @{user.get('screen_name')}: {created_at_str}")
                    # Keep account_age as default (old) if parsing fails

            is_new_account = account_age <= 90  # Changed from MAX_ACCOUNT_AGE_DAYS to 90
            
            # REMOVED FILTERING LOGIC - Include ALL profiles regardless of follower/following counts or age
            logger.log(f"    Include @{user.get('screen_name')} (Followers: {follower_count}, Following: {following_count}, Age: {account_age} days)")
            
            # Add all users to filtered_followings (no filtering)
            filtered_followings.append(user)

        new_followings = filtered_followings
        if not is_new_username and count_diff > 0:
            # If we had a real increase, we want to ensure these are truly new
            previous_followings = await get_previous_followings(username)
            if previous_followings:
                prev_set = set(
                    f.get('screen_name', '').lower() 
                    for f in previous_followings 
                    if isinstance(f, dict) and f.get('screen_name')
                )
                new_followings = [f for f in filtered_followings if f.get('screen_name', '').lower() not in prev_set]
            else:
                logger.log(f"Warning: Could not get previous followings for {username}")

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
                follower_count = user.get('followers_count', 0)
                following_count = user.get('friends_count', 0)
                created_at_str = user.get('created_at')
                
                # Log basic user info for debugging
                logger.log(f"Starting process for @{user.get('screen_name')} (ID: {user.get('id_str')})")
                
                account_age = 91  # Default to old account (> 90 days)
                if created_at_str:
                    try:
                        created_at_dt = datetime.strptime(created_at_str, '%a %b %d %H:%M:%S %z %Y')
                        now_est = datetime.now(pytz.timezone('America/New_York'))
                        created_at_est = created_at_dt.astimezone(pytz.timezone('America/New_York'))
                        time_diff = now_est - created_at_est
                        account_age = time_diff.days
                    except ValueError:
                        logger.warn(f"Could not parse created_at date for @{user.get('screen_name')}: {created_at_str}")

                is_new_account = account_age <= 90  # Changed from MAX_ACCOUNT_AGE_DAYS to 90
                
                # REMOVED FILTERING LOGIC - Process ALL profiles regardless of follower/following counts or age
                logger.log(f"    {processed_count}/{len(new_followings)} ✅ @{user.get('screen_name')} (Followers: {follower_count}, Following: {following_count}, Age: {account_age} days) - Processing ALL profiles")

                # Check if we already processed this user
                deduplication_result = await DeduplicationService.process_profile(
                    user.get('screen_name'),
                    source_username
                )
                if not deduplication_result['isNew']:
                    logger.log(f"    {processed_count}/{len(new_followings)} ❌ @{user.get('screen_name')} (Duplicate)")
                    skipped_count += 1
                    return { "user": user, "status": 'skipped', "reason": 'duplicate' }

                user_id = user.get('id_str')
                if not user_id:
                    logger.log(f"    {processed_count}/{len(new_followings)} ❌ @{user.get('screen_name')}: No valid ID")
                    error_count += 1
                    return { "user": user, "status": 'error', "reason": 'no_user_id' }

                tweets = []
                timeline_response = None
                is_retryable_error = False

                try:
                        logger.log(f"Fetching tweets for @{user.get('screen_name')} using UserTweets endpoint")
                        user_tweets_params = {
                            'id': user_id,
                            'count': '5'
                        }
                        logger.log(f"UserTweets API call parameters: {json.dumps(user_tweets_params)}")
                        logger.log(f"User ID being used: {user_id} (type: {type(user_id)})")
                        
                        timeline_response = await throttled_rapid_api_request(lambda: twitter_client.get_user_tweets(user_id, '5'))
                        
                        logger.log(f"UserTweets API raw response status: {timeline_response.get('status')}")
                        logger.log(f"UserTweets API raw response headers: {json.dumps(timeline_response.get('headers'))}")
                        logger.log(f"UserTweets API response keys: {list(timeline_response.get('data', {}).keys())}")
                        if timeline_response.get('data') and timeline_response['data'].get('status') and timeline_response['data']['status'] != 'ok':
                            logger.log(f"UserTweets API error response: {json.dumps(timeline_response['data'])}")

                        if timeline_response.get('data') and timeline_response['data'].get('error'):
                            logger.error(f"API returned error for @{user.get('screen_name')}: {timeline_response['data']['error']}")
                            logger.error(f"Status code: {timeline_response['data'].get('statusCode') or timeline_response.get('status')}")
                            raise ValueError(f"API error: {timeline_response['data']['error']}")

                        tweets = extract_tweets_from_response(timeline_response['data'])
                        logger.log(f"Retrieved {len(tweets)} tweets from UserTweets endpoint for @{user.get('screen_name')}")
                        
                        raw_response_file = os.path.join(RAW_RESPONSES_DIR, f"{user.get('screen_name')}_raw_response_UserTweets_{datetime.now().isoformat().replace(':', '-').replace('.', '-')}.json")
                        with open(raw_response_file, 'w', encoding='utf-8') as f:
                            json.dump(timeline_response['data'], f, indent=2)
                        logger.log(f"Saved raw UserTweets response to {raw_response_file}")
                        
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
                        user_tweets_and_replies_params = {
                            'id': user_id,
                            'count': '5'
                        }
                        logger.log(f"UserTweetsAndReplies API call parameters: {json.dumps(user_tweets_and_replies_params)}")
                        logger.log(f"User ID being used: {user_id} (type: {type(user_id)})")
                        
                        timeline_response = await throttled_rapid_api_request(lambda: twitter_client.get_user_tweets_and_replies(user_id, '5'))
                        
                        logger.log(f"UserTweetsAndReplies API raw response status: {timeline_response.get('status')}")
                        logger.log(f"UserTweetsAndReplies API raw response headers: {json.dumps(timeline_response.get('headers'))}")
                        logger.log(f"UserTweetsAndReplies API response keys: {list(timeline_response.get('data', {}).keys())}")
                        if timeline_response.get('data') and timeline_response['data'].get('status') and timeline_response['data']['status'] != 'ok':
                            logger.log(f"UserTweetsAndReplies API error response: {json.dumps(timeline_response['data'])}")

                        if timeline_response.get('data') and timeline_response['data'].get('error'):
                            logger.error(f"API returned error for @{user.get('screen_name')}: {timeline_response['data']['error']}")
                            logger.error(f"Status code: {timeline_response['data'].get('statusCode') or timeline_response.get('status')}")
                            raise ValueError(f"API error: {timeline_response['data']['error']}")
                        
                        tweets = extract_tweets_from_response(timeline_response['data'])
                        logger.log(f"Retrieved {len(tweets)} tweets from UserTweetsAndReplies endpoint for @{user.get('screen_name')}")

                        raw_response_file = os.path.join(RAW_RESPONSES_DIR, f"{user.get('screen_name')}_raw_response_UserTweetsAndReplies_{datetime.now().isoformat().replace(':', '-').replace('.', '-')}.json")
                        with open(raw_response_file, 'w', encoding='utf-8') as f:
                            json.dump(timeline_response['data'], f, indent=2)
                        logger.log(f"Saved raw UserTweetsAndReplies response to {raw_response_file}")

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
        files = os.listdir(FOLLOWER_COUNTS_DIR)
        if not files:
            return None
        
        sorted_files = sorted(files, reverse=True)
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
        files = os.listdir(FOLLOWER_COUNTS_DIR)
        if files:
            # Filter out backup files
            non_backup_files = [f for f in files if not f.startswith('backup_')]
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

async def get_previous_followings(username):
    """
    Get previous followings from history directory
    """
    try:
        files = os.listdir(HISTORY_DIR)
        if not files:
            return []
        
        user_files = [f for f in files if f.startswith(f"{username}_")]
        if not user_files:
            return []
        
        latest_file = sorted(user_files, reverse=True)[0]
        with open(os.path.join(HISTORY_DIR, latest_file), 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and 'users' in data and isinstance(data['users'], list):
            return data['users']
        else:
            logger.log(f"Warning: Invalid data format in {latest_file}")
            return []
    except Exception as e:
        logger.log(f"Error reading previous followings for {username}: {e}")
        return []

async def save_following_history(username, followings):
    """
    Save following history to JSON file
    """
    try:
        date_str = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d')
        filename = os.path.join(HISTORY_DIR, f"{username}_{date_str}.json")
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({"users": followings, "timestamp": datetime.now().isoformat()}, f, indent=2)
        
        logger.log(f"Saved following history for {username} to {filename}")
    except Exception as e:
        logger.error(f"Error saving following history: {e}")

async def main():
    global notion_categories, skipped_profiles # Declare intent to modify global lists

    if not RAPID_API_KEY:
        logger.error('RAPID_API_KEY is not set in environment variables')
        raise ValueError('RAPID_API_KEY is not set in environment variables')

    total_processed = 0
    total_skipped = 0
    total_uploaded = 0
    resume_mode = False
    processed_files = []

    await setup_directories()
    
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
                        await collect_tweets_for_new_followers(results['followings'], profile['screen_name'])
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
                        logger.log(f"[{file_index + 1}/{len(sorted_files)}] ⏩ Skipped: {file} (error)")
                        return { "file": file, "status": 'error', "reason": 'analysis_error', "error": str(error) }

                # Create tasks for concurrent processing
                tasks = [process_single_tweet_file(file, idx) for idx, file in enumerate(sorted_files)]
                
                # Log that we're starting concurrent processing
                logger.log(f"Starting concurrent processing of {len(tasks)} files...")
                
                # Process all files concurrently like JavaScript does
                # JavaScript uses Promise.all() which is equivalent to asyncio.gather()
                results = await asyncio.gather(*tasks, return_exceptions=True)

                successful_uploads = sum(1 for r in results if r['status'] == 'success')
                skipped_uploads = sum(1 for r in results if r['status'] == 'skipped')
                failed_uploads = sum(1 for r in results if r['status'] == 'error')
                
                # Update totals based on results
                total_processed = len(results)
                total_uploaded = successful_uploads
                total_skipped = skipped_uploads
                
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

    logger.log(f"\n📈 FINAL RUN SUMMARY:")
    logger.log(f"───────────────────────────────────────")
    logger.log(f"Total Profiles Processed: {total_processed}")
    logger.log(f"Total Profiles Uploaded to Notion: {total_uploaded}")
    logger.log(f"Total Profiles Skipped: {total_skipped}")
    if total_processed > 0:
        upload_rate = (total_uploaded / total_processed) * 100
        skip_rate = (total_skipped / total_processed) * 100
        logger.log(f"Upload Rate: {upload_rate:.1f}%")
        logger.log(f"Skip Rate: {skip_rate:.1f}%")
    logger.log(f"───────────────────────────────────────\n")
    
    return {
        "totalProcessed": total_processed,
        "totalSkipped": total_skipped,
        "totalUploaded": total_uploaded,
        "errors": [] # Placeholder for collecting errors
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