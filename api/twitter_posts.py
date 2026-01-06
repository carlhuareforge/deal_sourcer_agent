import argparse
import asyncio
import json
import os
import sys
from datetime import datetime

import hashlib
import pytz
import requests

# Allow running from repo root or the api/ directory
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from api.twitter_client import make_http_request, throttled_rapid_api_request
from config import BASE_DIR, RAPID_API_KEY
from utils.logger import logger

RAPID_API_HOST_283 = 'twitter283.p.rapidapi.com'
BASE_URL = f"https://{RAPID_API_HOST_283}"
ANALYSIS_DIR = os.path.join(BASE_DIR, 'twitter_post_analysis')


def format_est_date_time():
    """
    Returns a timestamp string in EST for filenames.
    """
    now = datetime.now(pytz.timezone('America/New_York'))
    return now.strftime('%Y-%m-%d_%H-%M-%S')


def _extract_user_id(payload):
    """
    Recursively finds a user id from the UserResult response.
    """
    if isinstance(payload, dict):
        for key in ('rest_id', 'id_str', 'id'):
            if key in payload and isinstance(payload[key], (str, int)):
                return str(payload[key])
        for value in payload.values():
            found = _extract_user_id(value)
            if found:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _extract_user_id(item)
            if found:
                return found
    return None


def _extract_next_cursor(payload):
    """
    Attempts to find the next cursor in a paginated timeline response.
    Prefers 'Bottom' cursors but falls back to any cursor-like field.
    """
    cursors = {"bottom": [], "any": []}

    def record_typed_cursor(cursor_type_value, value):
        """
        Records cursor values that explicitly declare their type (e.g., Top/Bottom).
        """
        if cursor_type_value is None or not isinstance(value, (str, int)):
            return

        cursor_type = str(cursor_type_value).lower()
        value_str = str(value)

        if cursor_type == 'bottom':
            cursors['bottom'].append(value_str)
        elif cursor_type:
            cursors['any'].append(value_str)

    def walk(node):
        if isinstance(node, dict):
            cursor_type_value = node.get('cursorType', node.get('cursor_type'))
            record_typed_cursor(cursor_type_value, node.get('value'))

            for key, val in node.items():
                key_lower = key.lower()
                if key_lower not in ('cursortype', 'cursor_type') and 'cursor' in key_lower and isinstance(val, (str, int)):
                    cursors['any'].append(str(val))
                if isinstance(val, (dict, list)):
                    walk(val)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)

    if cursors['bottom']:
        return cursors['bottom'][0]
    if cursors['any']:
        return cursors['any'][0]
    return None


def _clean_username(username):
    return username.lstrip('@').strip()


def _compute_page_signature(data):
    """
    Creates a stable hash of the page payload so we can detect repeated pages.
    Prefer a hash of tweet ids when present to tolerate minor cursor metadata differences.
    """
    tweet_ids = sorted(set(_collect_tweet_ids(data)))
    if tweet_ids:
        basis = {"tweets": tweet_ids}
    else:
        basis = data

    serialized = json.dumps(basis, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(serialized.encode('utf-8')).hexdigest()


def _collect_tweet_ids(payload):
    """
    Recursively collects tweet/rest ids from timeline entries.
    """
    ids = []

    if isinstance(payload, dict):
        # Direct tweet result hit
        tweet_result = payload.get('tweet_results')
        if isinstance(tweet_result, dict):
            rid = _extract_user_id(tweet_result)  # reuse id finder; rest_id/id_str are covered
            if rid:
                ids.append(rid)
        # Some timeline structures put tweet id under 'rest_id' on result
        if payload.get('__typename') == 'Tweet':
            rid = _extract_user_id(payload)
            if rid:
                ids.append(rid)

        for val in payload.values():
            ids.extend(_collect_tweet_ids(val))
    elif isinstance(payload, list):
        for item in payload:
            ids.extend(_collect_tweet_ids(item))

    return ids


def _iter_tweets(payload):
    """
    Yields Tweet objects from a timeline payload.
    """
    tweets = []

    def walk(node):
        if isinstance(node, dict):
            if node.get('__typename') == 'Tweet' and 'legacy' in node:
                tweets.append(node)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return tweets


def _extract_author_from_tweet(tweet):
    user_result = tweet.get('core', {}).get('user_results', {})
    user_obj = user_result.get('result', user_result) if isinstance(user_result, dict) else {}
    legacy_user = user_obj.get('legacy', {})
    core_user = user_obj.get('core', {})
    relationship = user_obj.get('relationship_counts', {})
    verification = user_obj.get('verification', {}) or user_obj.get('legacy_verification_info', {})

    return {
        "id": _extract_user_id(user_obj),
        "screenName": core_user.get('screen_name') or legacy_user.get('screen_name'),
        "name": core_user.get('name') or legacy_user.get('name'),
        "followersCount": relationship.get('followers') or legacy_user.get('followers_count'),
        "followingCount": relationship.get('following') or legacy_user.get('friends_count'),
        "verified": bool(verification.get('is_blue_verified')) if isinstance(verification, dict) else False,
        "profileImageUrl": legacy_user.get('profile_image_url_https') or legacy_user.get('profile_image_url'),
    }


def _extract_entities(legacy):
    entities = legacy.get('entities', {}) or {}
    urls = []
    for u in entities.get('urls', []):
        if not isinstance(u, dict):
            continue
        urls.append({
            "displayUrl": u.get('display_url'),
            "expandedUrl": u.get('expanded_url'),
            "url": u.get('url')
        })

    mentions = []
    for m in entities.get('user_mentions', []):
        if not isinstance(m, dict):
            continue
        mentions.append({
            "screenName": m.get('screen_name'),
            "id": m.get('id_str') or m.get('id')
        })

    hashtags = []
    for h in entities.get('hashtags', []):
        if not isinstance(h, dict):
            continue
        text = h.get('text')
        if text:
            hashtags.append(text)

    media_items = []
    media_list = (legacy.get('extended_entities') or {}).get('media') or entities.get('media') or []
    for m in media_list:
        if not isinstance(m, dict):
            continue
        media_items.append({
            "id": m.get('id_str') or m.get('id'),
            "type": m.get('type'),
            "mediaKey": m.get('media_key'),
            "displayUrl": m.get('display_url'),
            "expandedUrl": m.get('expanded_url'),
            "mediaUrl": m.get('media_url_https') or m.get('media_url'),
        })

    return urls, mentions, hashtags, media_items


def _sanitize_tweet(tweet):
    legacy = tweet.get('legacy', {}) or {}
    author = _extract_author_from_tweet(tweet)
    urls, mentions, hashtags, media_items = _extract_entities(legacy)

    # Pick metrics that are usually present in legacy
    engagement = {
        "replyCount": legacy.get('reply_count'),
        "retweetCount": legacy.get('retweet_count'),
        "quoteCount": legacy.get('quote_count'),
        "likeCount": legacy.get('favorite_count'),
        "bookmarkCount": legacy.get('bookmark_count'),
        "viewCount": None
    }
    view_info = tweet.get('view_count_info') or tweet.get('view_counts') or {}
    if isinstance(view_info, dict):
        engagement["viewCount"] = view_info.get('count')

    return {
        "id": tweet.get('rest_id') or legacy.get('id_str'),
        "conversationId": legacy.get('conversation_id_str'),
        "createdAt": legacy.get('created_at'),
        "text": legacy.get('full_text') or legacy.get('text'),
        "inReplyToStatusId": legacy.get('in_reply_to_status_id_str'),
        "inReplyToUserId": legacy.get('in_reply_to_user_id_str'),
        "quotedStatusId": legacy.get('quoted_status_id_str'),
        "isQuoteStatus": bool(legacy.get('is_quote_status')),
        "author": author,
        "engagement": engagement,
        "urls": urls,
        "mentions": mentions,
        "hashtags": hashtags,
        "media": media_items,
        "language": legacy.get('lang'),
        "source": legacy.get('source'),
        "replyToUserResults": tweet.get('reply_to_user_results')
    }


def _tweet_sort_key(tweet):
    tid = tweet.get('id')
    if tid:
        try:
            return int(tid)
        except (TypeError, ValueError):
            pass
    created_at = tweet.get('createdAt')
    if created_at:
        try:
            return datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y').timestamp()
        except Exception:
            return 0
    return 0


def _collect_referenced_ids_from_tweet(tweet):
    legacy = tweet.get('legacy', {}) or {}
    refs = set()
    for key in ('in_reply_to_status_id_str', 'quoted_status_id_str', 'retweeted_status_id_str'):
        val = legacy.get(key)
        if val:
            refs.add(str(val))
    return refs


def _collect_conversation_ids(payload):
    """
    Collects IDs listed in conversation_metadata.all_tweet_ids if present.
    """
    ids = set()

    def walk(node):
        if isinstance(node, dict):
            convo_meta = node.get('conversation_metadata')
            if isinstance(convo_meta, dict):
                ids.update(str(tid) for tid in convo_meta.get('all_tweet_ids', []) if tid)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return ids


def _build_headers():
    return {
        'x-rapidapi-key': RAPID_API_KEY,
        'x-rapidapi-host': RAPID_API_HOST_283
    }


async def fetch_user_result(screen_name):
    """
    Looks up user metadata and id from the RapidAPI endpoint.
    """
    options = {
        'method': 'GET',
        'url': f"{BASE_URL}/UserResultByScreenName",
        'params': {
            'username': screen_name
        },
        'headers': _build_headers()
    }

    logger.log(f"Resolving @{screen_name} to user id...")
    response = await throttled_rapid_api_request(lambda: make_http_request(options))
    data = response.get('data', {})

    user_id = _extract_user_id(data)
    if not user_id:
        raise ValueError(f"Could not find user id for @{screen_name}")

    logger.log(f"Found user id {user_id} for @{screen_name}")
    return user_id, data


async def fetch_all_tweets_and_replies(user_id, on_page_saved=None):
    """
    Fetches all pages of tweets and replies for a user by paging through cursors.
    Optionally persists each page via on_page_saved.
    """
    pages = []
    seen_cursors = set()
    seen_signatures = set()
    seen_tweet_ids = set()
    cursor = None
    page_index = 0

    while True:
        page_index += 1
        params = {'user_id': user_id}
        if cursor:
            params['cursor'] = cursor

        options = {
            'method': 'GET',
            'url': f"{BASE_URL}/UserTweetsReplies",
            'params': params,
            'headers': _build_headers()
        }

        logger.log(f"Requesting page {page_index} (cursor: {cursor or 'start'})")
        max_attempts = 3
        attempt = 0
        while True:
            attempt += 1
            try:
                response = await throttled_rapid_api_request(lambda: make_http_request(options))
                break
            except requests.exceptions.HTTPError as error:
                response_obj = getattr(error, 'response', None)
                status = response_obj.status_code if response_obj is not None else None
                body_text = response_obj.text if response_obj is not None else ''
                logger.error(f"HTTP error on page {page_index} attempt {attempt}/{max_attempts}: {error}")
                if body_text:
                    try:
                        logger.error(f"Error response body: {response_obj.json()}")
                    except Exception:
                        logger.error(f"Error response body (raw): {body_text[:500]}")
                if status == 400 and attempt < max_attempts:
                    logger.warn(f"400 encountered on page {page_index}; retrying (attempt {attempt + 1}/{max_attempts})...")
                    await asyncio.sleep(0.5)
                    continue
                logger.error(f"Stopping pagination after HTTP error on page {page_index}")
                response = None
                break

        if response is None:
            break

        data = response.get('data', {})
        tweet_ids = sorted(set(_collect_tweet_ids(data)))
        next_cursor = _extract_next_cursor(data)
        signature = _compute_page_signature(data)
        is_duplicate_page = signature in seen_signatures
        new_tweet_ids = [tid for tid in tweet_ids if tid not in seen_tweet_ids]

        page_record = {
            "pageNumber": page_index,
            "cursor": cursor,
            "requestedCursor": cursor,
            "nextCursor": next_cursor,
            "pageSignature": signature,
            "tweetIds": tweet_ids,
            "newTweetIds": new_tweet_ids,
            "data": data
        }
        pages.append(page_record)

        if on_page_saved:
            on_page_saved(pages, page_record)

        if is_duplicate_page:
            logger.warn(f"Page {page_index} content identical to a previous page; stopping to avoid a loop.")
            break

        if not new_tweet_ids:
            logger.warn(f"Page {page_index} contained no new tweets; stopping pagination.")
            break

        if next_cursor:
            logger.log(f"Next cursor from page {page_index}: {next_cursor}")
        else:
            logger.log("No next cursor found; pagination complete.")
            break

        if next_cursor in seen_cursors:
            logger.warn(f"Cursor {next_cursor} already seen; stopping to avoid a loop.")
            break

        seen_cursors.add(next_cursor)
        seen_signatures.add(signature)
        seen_tweet_ids.update(new_tweet_ids)
        cursor = next_cursor

    logger.log(f"Collected {len(pages)} page(s) of tweets and replies for user {user_id}")
    return pages


def _derive_missing_tweet_ids(target_user_id, pages):
    """
    Determine which referenced tweet IDs are missing from hydrated tweets (include non-target authors for context).
    """
    target_user_id = str(target_user_id)
    hydrated = {}
    referenced = set()

    for page in pages:
        data = page.get('data', {})
        for tweet in _iter_tweets(data):
            sanitized = _sanitize_tweet(tweet)
            tid = sanitized.get('id')
            if tid:
                hydrated[tid] = sanitized
            referenced.update(_collect_referenced_ids_from_tweet(tweet))
        # Conversation metadata may list thread IDs
        referenced.update(_collect_conversation_ids(data))

    # Only chase references we haven't hydrated yet
    missing_candidates = [tid for tid in referenced if tid not in hydrated]
    return hydrated, missing_candidates


async def _fetch_and_filter_missing(target_user_id, missing_ids):
    fetched = {}

    def extract_from_batch(data):
        if not isinstance(data, dict):
            return []
        candidates = []
        for key in ('results', 'tweet_results', 'tweets', 'tweetResult'):
            val = data.get(key)
            if isinstance(val, list):
                candidates.extend(val)
            elif isinstance(val, dict):
                candidates.append(val)
        inner = data.get('data')
        if isinstance(inner, list):
            candidates.extend(inner)
        elif isinstance(inner, dict):
            for key in ('results', 'tweet_results', 'tweets', 'tweetResult'):
                val = inner.get(key)
                if isinstance(val, list):
                    candidates.extend(val)
                elif isinstance(val, dict):
                    candidates.append(val)
        return candidates

    batches = [missing_ids[i:i + 20] for i in range(0, len(missing_ids), 20)]
    logger.log(f"Missing backfill split into {len(batches)} batch(es) of up to 20 ids")

    sem = asyncio.Semaphore(10)  # up to 10 concurrent API calls

    async def fetch_chunk(idx, chunk):
        async with sem:
            logger.log(f"Backfill batch {idx}/{len(batches)}: fetching {len(chunk)} id(s)")
            batch = await fetch_tweets_by_ids(chunk)
        if not batch:
            logger.warn(f"Skipping batch {idx}/{len(batches)} (size {len(chunk)}) due to repeated fetch errors.")
            return {}
        collected = {}
        for item in extract_from_batch(batch):
            tweet_obj = item.get('result') if isinstance(item, dict) else None
            if not tweet_obj:
                continue
            sanitized = _sanitize_tweet(tweet_obj)
            if sanitized.get('id'):
                collected[sanitized['id']] = sanitized
        logger.log(f"Backfill batch {idx}/{len(batches)} retrieved {len(collected)} tweet(s)")
        return collected

    results = await asyncio.gather(*(fetch_chunk(i + 1, chunk) for i, chunk in enumerate(batches)))
    for part in results:
        fetched.update(part)
    return fetched


def _build_minimal_tweet(sanitized):
    """
    Reduce tweet payload to sentiment/context-friendly essentials.
    """
    return {
        "id": sanitized.get("id"),
        "createdAt": sanitized.get("createdAt"),
        "text": sanitized.get("text"),
        "conversationId": sanitized.get("conversationId"),
        "inReplyToStatusId": sanitized.get("inReplyToStatusId"),
        "inReplyToUserId": sanitized.get("inReplyToUserId"),
        "quotedStatusId": sanitized.get("quotedStatusId"),
        "isQuoteStatus": sanitized.get("isQuoteStatus"),
        "author": {
            "id": sanitized.get("author", {}).get("id"),
            "screenName": sanitized.get("author", {}).get("screenName"),
            "name": sanitized.get("author", {}).get("name"),
        },
        "engagement": sanitized.get("engagement"),
        "urls": sanitized.get("urls"),
    }


def _build_threads(minimal_tweets):
    """
    Build threaded view: roots sorted newest->oldest; replies sorted oldest->newest.
    Replies are nested; orphans (no parent in set) appear as roots with flag.
    """
    by_id = {t["id"]: dict(t) for t in minimal_tweets if t.get("id")}
    children = {}
    for t in minimal_tweets:
        pid = t.get("inReplyToStatusId")
        tid = t.get("id")
        if pid and tid and pid in by_id:
            children.setdefault(pid, []).append(dict(t))

    def attach_replies(parent_id):
        replies = children.get(parent_id, [])
        replies.sort(key=_tweet_sort_key)  # oldest -> newest
        for r in replies:
            r_id = r.get("id")
            r["replies"] = attach_replies(r_id) if r_id else []
        return replies

    roots = []
    orphan_replies = []
    for tid, tweet in by_id.items():
        if tweet.get("inReplyToStatusId") and tweet.get("inReplyToStatusId") in by_id:
            continue  # will be attached as child
        if tweet.get("inReplyToStatusId") and tweet.get("inReplyToStatusId") not in by_id:
            tweet["orphanReply"] = True
            orphan_replies.append(tid)
        tweet["replies"] = attach_replies(tid)
        roots.append(tweet)

    roots.sort(key=_tweet_sort_key, reverse=True)  # newest -> oldest
    return roots, orphan_replies


def _strip_for_slim(node):
    """
    Remove non-essential fields for slim output (drop engagement, urls, name, inReplyToUserId, isQuoteStatus).
    """
    keep = {
        "id": node.get("id"),
        "createdAt": node.get("createdAt"),
        "text": node.get("text"),
        "conversationId": node.get("conversationId"),
        "inReplyToStatusId": node.get("inReplyToStatusId"),
        "quotedStatusId": node.get("quotedStatusId"),
        "author": {
            "id": (node.get("author") or {}).get("id"),
            "screenName": (node.get("author") or {}).get("screenName"),
        },
    }
    replies = []
    for r in node.get("replies", []):
        replies.append(_strip_for_slim(r))
    if replies:
        keep["replies"] = replies
    return keep


def _assemble_ai_ready_output(username, user_id, fetched_at, raw_filename, pages, hydrated, missing_fetched):
    """
    Build a reduced, analysis-friendly JSON structure.
    """
    merged = dict(hydrated)
    merged.update(missing_fetched)

    tweets = list(merged.values())
    tweets.sort(key=_tweet_sort_key, reverse=True)
    minimal_tweets = [_build_minimal_tweet(t) for t in tweets]
    threads, orphan_replies = _build_threads(minimal_tweets)

    def sum_metric(key):
        total = 0
        for t in tweets:
            val = (t.get("engagement") or {}).get(key)
            if isinstance(val, (int, float)):
                total += val
        return total

    replies_count = sum(1 for t in minimal_tweets if t.get("inReplyToStatusId"))
    quotes_count = sum(1 for t in minimal_tweets if t.get("isQuoteStatus"))
    originals_count = len(minimal_tweets) - replies_count - quotes_count
    metrics = {
        "replyCountTotal": sum_metric("replyCount"),
        "retweetCountTotal": sum_metric("retweetCount"),
        "quoteCountTotal": sum_metric("quoteCount"),
        "likeCountTotal": sum_metric("likeCount"),
        "bookmarkCountTotal": sum_metric("bookmarkCount"),
        "viewCountTotal": sum_metric("viewCount"),
    }

    return {
        "username": username,
        "userId": user_id,
        "fetchedAt": fetched_at,
        "rawFile": raw_filename,
        "summary": {
            "pagesFetched": len(pages),
            "tweetsFromPagination": len(hydrated),
            "missingFetched": len(missing_fetched),
            "totalTweets": len(tweets),
            "missingIds": list(missing_fetched.keys()),
            "tweetBreakdown": {
                "originals": originals_count,
                "replies": replies_count,
                "quotes": quotes_count,
            },
            "engagementTotals": metrics,
            "orphanReplies": orphan_replies,
        },
        "tweetsFlat": minimal_tweets,
        "threads": threads,
    }


def _assemble_slim_output(username, user_id, fetched_at, raw_filename, threads, summary):
    slim_threads = [_strip_for_slim(t) for t in threads]
    return {
        "username": username,
        "userId": user_id,
        "fetchedAt": fetched_at,
        "rawFile": raw_filename,
        "summary": summary,
        "threads": slim_threads
    }


def _save_clean_output(filename, payload):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2)


async def fetch_tweet_detail(tweet_id):
    """
    Fetches a single tweet via TweetDetailv3.
    """
    options = {
        'method': 'GET',
        'url': f"{BASE_URL}/TweetDetailv3",
        'params': {'tweet_id': tweet_id},
        'headers': _build_headers()
    }
    response = await throttled_rapid_api_request(lambda: make_http_request(options))
    return response.get('data', {})


async def fetch_tweets_by_ids(tweet_ids):
    """
    Fetches up to 20 tweets via TweetResultsByRestIds.
    """
    ids_param = ','.join(tweet_ids)
    options = {
        'method': 'GET',
        'url': f"{BASE_URL}/TweetResultsByRestIds",
        'params': {'tweet_ids': ids_param},
        'headers': _build_headers()
    }
    max_attempts = 3
    attempt = 0
    while True:
        attempt += 1
        try:
            response = await throttled_rapid_api_request(lambda: make_http_request(options))
            return response.get('data', {})
        except requests.exceptions.HTTPError as error:
            response_obj = getattr(error, 'response', None)
            status = response_obj.status_code if response_obj is not None else None
            body_text = response_obj.text if response_obj is not None else ''
            logger.error(f"HTTP error on TweetResultsByRestIds attempt {attempt}/{max_attempts}: {error}")
            if body_text:
                try:
                    logger.error(f"Error response body: {response_obj.json()}")
                except Exception:
                    logger.error(f"Error response body (raw): {body_text[:500]}")
            if status == 400 and attempt < max_attempts:
                logger.warn(f"Retrying TweetResultsByRestIds (attempt {attempt + 1}/{max_attempts}) after 400...")
                await asyncio.sleep(0.5)
                continue
            logger.error("Giving up on this TweetResultsByRestIds batch after HTTP error.")
            return None


def _build_output_path(username, timestamp=None):
    """
    Prepares a timestamped output path for incremental JSON dumps.
    """
    os.makedirs(ANALYSIS_DIR, exist_ok=True)
    timestamp = timestamp or format_est_date_time()
    filename = os.path.join(ANALYSIS_DIR, f"{username}_{timestamp}.json")
    fetched_at = datetime.now(pytz.timezone('America/New_York')).isoformat()
    return filename, fetched_at, timestamp


def _build_clean_output_path(username, timestamp):
    filename = os.path.join(ANALYSIS_DIR, f"{username}_{timestamp}_clean.json")
    return filename


def _build_clean_slim_output_path(username, timestamp):
    filename = os.path.join(ANALYSIS_DIR, f"{username}_{timestamp}_clean_slim.json")
    return filename


def _write_partial_results(filename, username, user_id, user_payload, pages, fetched_at):
    """
    Writes the current collection state to disk. Intended to be called after each page fetch.
    """
    output = {
        "username": username,
        "userId": user_id,
        "fetchedAt": fetched_at,
        "user": user_payload,
        "tweetsAndReplies": {
            "pageCount": len(pages),
            "pages": pages
        }
    }

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)


def _load_raw_file(raw_path):
    with open(raw_path, 'r', encoding='utf-8') as f:
        payload = json.load(f)
    username = payload.get('username')
    user_id = payload.get('userId')
    fetched_at = payload.get('fetchedAt', datetime.now(pytz.timezone('America/New_York')).isoformat())
    pages = payload.get('tweetsAndReplies', {}).get('pages', [])
    user_payload = payload.get('user', {})
    return username, user_id, fetched_at, pages, user_payload


async def run(username):
    """
    Orchestrates lookup, pagination, backfill, and cleaning for a given username.
    """
    if not RAPID_API_KEY:
        raise ValueError('RAPID_API_KEY is not set in environment variables')

    clean_username = _clean_username(username)
    await run_full_flow(clean_username)


async def run_entry(args):
    """
    Entry point that supports modular execution:
    - pages-only: fetch pages and save raw
    - backfill-only: reuse raw file to backfill/clean
    - process-only: reuse raw file to build clean without backfill
    - default: full flow
    """
    if args.backfill_only:
        if not args.raw_file:
            raise ValueError("backfill-only requires --raw-file")
        username, user_id, fetched_at, pages, user_payload = _load_raw_file(args.raw_file)
        if not user_id or not username:
            raise ValueError("raw file missing username/userId")
        timestamp = os.path.basename(args.raw_file).rsplit('.', 1)[0].split('_', 1)[-1]
        clean_filename = _build_clean_output_path(username, timestamp)
        hydrated, missing_ids = _derive_missing_tweet_ids(user_id, pages)
        if missing_ids:
            logger.log(f"Backfilling {len(missing_ids)} referenced tweet(s) for @{username}")
        missing_fetched = await _fetch_and_filter_missing(user_id, missing_ids)
        clean_payload = _assemble_ai_ready_output(
            username=username,
            user_id=user_id,
            fetched_at=fetched_at,
            raw_filename=args.raw_file,
            pages=pages,
            hydrated=hydrated,
            missing_fetched=missing_fetched
        )
        _save_clean_output(clean_filename, clean_payload)
        clean_slim_filename = _build_clean_slim_output_path(username, format_est_date_time())
        slim_payload = _assemble_slim_output(
            username=username,
            user_id=user_id,
            fetched_at=fetched_at,
            raw_filename=args.raw_file,
            threads=clean_payload["threads"],
            summary=clean_payload["summary"],
        )
        _save_clean_output(clean_slim_filename, slim_payload)
        logger.log(f"Saved AI-friendly tweet data to {clean_filename}")
        logger.log(f"Saved slim tweet data to {clean_slim_filename}")
        return

    if args.process_only:
        if not args.raw_file:
            raise ValueError("process-only requires --raw-file")
        username, user_id, fetched_at, pages, user_payload = _load_raw_file(args.raw_file)
        timestamp = format_est_date_time()
        clean_filename = _build_clean_output_path(username, timestamp)
        # Skip backfill entirely; just process hydrated tweets
        hydrated = {}
        for page in pages:
            data = page.get('data', {})
            for tweet in _iter_tweets(data):
                sanitized = _sanitize_tweet(tweet)
                tid = sanitized.get('id')
                if tid:
                    hydrated[tid] = sanitized
        missing_fetched = {}
        clean_payload = _assemble_ai_ready_output(
            username=username,
            user_id=user_id,
            fetched_at=fetched_at,
            raw_filename=args.raw_file,
            pages=pages,
            hydrated=hydrated,
            missing_fetched=missing_fetched
        )
        _save_clean_output(clean_filename, clean_payload)
        clean_slim_filename = _build_clean_slim_output_path(username, format_est_date_time())
        slim_payload = _assemble_slim_output(
            username=username,
            user_id=user_id,
            fetched_at=fetched_at,
            raw_filename=args.raw_file,
            threads=clean_payload["threads"],
            summary=clean_payload["summary"],
        )
        _save_clean_output(clean_slim_filename, slim_payload)
        logger.log(f"Saved AI-friendly tweet data to {clean_filename}")
        logger.log(f"Saved slim tweet data to {clean_slim_filename}")
        return

    if args.pages_only and not args.username:
        raise ValueError("pages-only requires a username")

    if args.pages_only:
        await run_pages_only(_clean_username(args.username))
        return

    if args.raw_file:
        # Reuse raw file for backfill/clean after ensuring username if provided matches
        username, user_id, fetched_at, pages, user_payload = _load_raw_file(args.raw_file)
        if args.username and _clean_username(args.username) != _clean_username(username):
            raise ValueError("Provided username does not match raw file username")
        clean_filename = _build_clean_output_path(username, format_est_date_time())
        hydrated, missing_ids = _derive_missing_tweet_ids(user_id, pages)
        if missing_ids:
            logger.log(f"Backfilling {len(missing_ids)} referenced tweet(s) for @{username}")
        missing_fetched = await _fetch_and_filter_missing(user_id, missing_ids)
        clean_payload = _assemble_ai_ready_output(
            username=username,
            user_id=user_id,
            fetched_at=fetched_at,
            raw_filename=args.raw_file,
            pages=pages,
            hydrated=hydrated,
            missing_fetched=missing_fetched
        )
        _save_clean_output(clean_filename, clean_payload)
        logger.log(f"Saved AI-friendly tweet data to {clean_filename}")
        return

    await run_full_flow(_clean_username(args.username))


async def run_full_flow(clean_username):
    user_id, user_payload = await fetch_user_result(clean_username)
    raw_filename, fetched_at, timestamp = _build_output_path(clean_username)
    clean_filename = _build_clean_output_path(clean_username, timestamp)
    clean_slim_filename = _build_clean_slim_output_path(clean_username, timestamp)

    def persist(pages, page_record=None, final=False):
        _write_partial_results(raw_filename, clean_username, user_id, user_payload, pages, fetched_at)
        if page_record:
            used_cursor = page_record['requestedCursor'] or 'start'
            logger.log(f"Saved page {page_record['pageNumber']} (cursor used: {used_cursor}) to {raw_filename}")
        elif final:
            logger.log(f"Saved tweet and reply data to {raw_filename}")

    pages = await fetch_all_tweets_and_replies(user_id, on_page_saved=persist)
    persist(pages, final=True)

    hydrated, missing_ids = _derive_missing_tweet_ids(user_id, pages)
    if missing_ids:
        logger.log(f"Backfilling {len(missing_ids)} referenced tweet(s) for @{clean_username}")
    missing_fetched = await _fetch_and_filter_missing(user_id, missing_ids)

    clean_payload = _assemble_ai_ready_output(
        username=clean_username,
        user_id=user_id,
        fetched_at=fetched_at,
        raw_filename=raw_filename,
        pages=pages,
        hydrated=hydrated,
        missing_fetched=missing_fetched
    )
    _save_clean_output(clean_filename, clean_payload)
    logger.log(f"Saved AI-friendly tweet data to {clean_filename}")
    slim_payload = _assemble_slim_output(
        username=clean_username,
        user_id=user_id,
        fetched_at=fetched_at,
        raw_filename=raw_filename,
        threads=clean_payload["threads"],
        summary=clean_payload["summary"],
    )
    _save_clean_output(clean_slim_filename, slim_payload)
    logger.log(f"Saved slim tweet data to {clean_slim_filename}")


def parse_args():
    parser = argparse.ArgumentParser(description="Download tweets/replies and backfill missing tweets via RapidAPI.")
    parser.add_argument('username', nargs='?', help="Twitter username (with or without @). Not required for backfill-only.")
    parser.add_argument('--raw-file', help="Use an existing raw JSON file (skips pagination when provided).")
    parser.add_argument('--pages-only', action='store_true', help="Only fetch and save pages; skip backfill/clean output.")
    parser.add_argument('--backfill-only', action='store_true', help="Only run backfill/clean on an existing raw file.")
    parser.add_argument('--process-only', action='store_true', help="Only run cleaning on an existing raw file (no backfill).")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run_entry(args))
