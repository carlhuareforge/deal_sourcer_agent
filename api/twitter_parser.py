import json
from utils.logger import logger

def simplify_twitter_data(raw_data):
    """
    Simplifies raw Twitter API data into a more manageable format.
    Extracts profile information and a list of tweet texts.
    """
    simplified = {
        "profile": {},
        "tweets": [],
        "sourceUsername": raw_data.get('sourceUsername', 'unknown')
    }

    # Extract profile data
    if raw_data.get('profile'):
        profile = raw_data['profile']
        simplified['profile'] = {
            "screen_name": profile.get('screen_name'),
            "user_id": profile.get('id_str'),
            "name": profile.get('name'),
            "followers_count": profile.get('followers_count'),
            "friends_count": profile.get('friends_count'),
            "created_at": profile.get('created_at'),
            "description": profile.get('description'),
            "verified": profile.get('verified', False)
        }

    # Extract tweets
    if raw_data.get('tweets') and isinstance(raw_data['tweets'], list):
        for tweet in raw_data['tweets']:
            if isinstance(tweet, str):
                # If tweets are already simplified strings
                simplified['tweets'].append(tweet)
            elif isinstance(tweet, dict):
                # Prioritize full_text, then text
                text = tweet.get('full_text') or tweet.get('text')
                if text:
                    simplified['tweets'].append(text)
    
    logger.debug(f"Simplified data for @{simplified['profile'].get('screen_name')}: {len(simplified['tweets'])} tweets")
    return simplified

def _find_full_text_recursively(data, results):
    """
    Helper function to recursively find tweet text within nested JSON structures.
    Prioritizes 'note_tweet_results' for longer tweets, then 'full_text'.
    """
    if not data:
        return

    if isinstance(data, list):
        for item in data:
            _find_full_text_recursively(item, results)
    elif isinstance(data, dict):
        # Check for note_tweet (for longer tweets)
        if 'note_tweet' in data and isinstance(data['note_tweet'], dict):
            note_tweet = data['note_tweet']
            if 'note_tweet_results' in note_tweet and isinstance(note_tweet['note_tweet_results'], dict):
                note_result = note_tweet['note_tweet_results'].get('result')
                if note_result and isinstance(note_result, dict) and note_result.get('text'):
                    results.append(note_result['text'])
                    return # Found a note tweet, no need to recurse further into this object

        # Check for full_text
        if 'full_text' in data and isinstance(data['full_text'], str) and data['full_text'].strip() != '':
            results.append(data['full_text'])
            return # Found full_text, no need to recurse further into this object

        # Recurse into other dictionary properties
        for key in data:
            _find_full_text_recursively(data[key], results)

def extract_tweets_from_response(data):
    """
    Extracts tweet texts from raw Twitter API response data.
    """
    results = []
    _find_full_text_recursively(data, results)
    logger.debug(f"Extracted {len(results)} tweets from raw response.")
    return results