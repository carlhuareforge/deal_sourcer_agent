# Twitter Following Discovery + AI Triage (Architecture)

This project watches a set of “source” X/Twitter accounts, detects when they follow new accounts, collects a small tweet sample for those newly-followed accounts, runs an LLM classification step, and (optionally) uploads the results to a Notion database. A SQLite DB is used to deduplicate profiles globally so the same discovered account is only processed once across all sources.

## Key Inputs / Outputs

- **Input**: `input_usernames.csv` (columns: `screen_name,user_id`)
- **Primary outputs**
  - `follower_tweets/*_tweets.json`: profile + extracted tweet text for each included discovered account
  - `follower_tweets/ai_tweets/*_ai_input.json` + `*_ai_response.json`: what was sent to OpenAI + the parsed response
  - `follower_counts/follower_counts_*.csv`: last-known “following” counts per source account (used for change detection)
  - `db/twitter_profiles.db`: global “processed profiles” + discovery source relationships (dedup + tracking)
  - `raw_api_responses/*.json`: raw Twitter API responses for debugging

## High-Level Flow

```mermaid
flowchart TB
  CSV[input_usernames.csv\n(screen_name,user_id)]
  ENV[.env + config.py]

  subgraph Main["main.py"]
    MAIN[main()]
    COUNTS[get_following_counts()]
    PREV[get_previous_follower_count()]
    PROC[process_username()]
    FILTER[Dedup + age/count filter]
    COLLECT[collect_tweets_for_new_followers()]
    AI[analyze_tweets_with_ai()]
  end

  subgraph External["External services"]
    RAPID[RapidAPI Twitter endpoints\nUsersByRestIds / FollowingLight / UserTweets]
    OPENAI[OpenAI Chat Completions]
    NOTION[Notion Database API]
    S3[S3 (optional)]
  end

  subgraph Storage["Local storage"]
    DB[(SQLite: db/twitter_profiles.db)]
    COUNTSCSV[(follower_counts/*.csv)]
    RAW[(raw_api_responses/*.json)]
    TWEETS[(follower_tweets/*_tweets.json)]
    AIIO[(follower_tweets/ai_tweets/*.json)]
  end

  CSV --> MAIN
  ENV --> MAIN

  MAIN --> COUNTS --> RAPID
  MAIN --> PREV --> COUNTSCSV

  MAIN --> PROC --> RAPID
  PROC --> FILTER
  FILTER <--> DB
  FILTER -->|included| COLLECT --> RAPID
  COLLECT --> RAW
  COLLECT --> TWEETS --> AI --> OPENAI
  AI -->|skip / error| DB
  AI -->|upload| NOTION --> DB

  MAIN --> COUNTSCSV
  MAIN -->|optional sync| S3
  S3 <--> DB
```

## Major Steps (What `main.py` Does)

1. **Boot + setup**
   - Loads environment/config (`config.py`) and ensures directories exist.
   - Optional S3 sync: downloads a newer `db/twitter_profiles.db` and latest follower counts before processing.
   - Optional recovery: if `recovery_state.json` exists, skips already-processed tweet files.

2. **Initialize Notion categories**
   - Fetches existing Notion “Category” options and injects them into the LLM prompt (replaces `{categories}` in `prompts/tweet_analysis_prompt.txt`).

3. **Read seed/source accounts**
   - Reads `input_usernames.csv` and limits to `MAX_PROFILES`.

4. **Fetch current following counts (per source)**
   - Batch calls RapidAPI `UsersByRestIds` and builds a map: `screen_name -> friends_count`.

5. **Detect “new following” events**
   - For each source account:
     - Loads the previous count from the newest `follower_counts/follower_counts_*.csv`.
     - If the source is new (no previous count): **baseline only** (no followings processed).
     - If `count_diff == 0`: skip (no processing).
     - Otherwise: fetches the most recent followings using RapidAPI `FollowingLight` with `fetch_count = count_diff` (or `3` if `count_diff <= 0`).

6. **Filter discovered accounts (dedup + age/size heuristics)** (detailed below)
   - Only the accounts that pass this filter become “work items” for tweet collection + AI processing.
   - Accounts filtered out here are still recorded in the dedup DB, which prevents repeated evaluation later.

7. **Collect tweets for included accounts**
   - For each included discovered account:
     - Calls `UserTweets(count=5)`, extracts tweet text; if empty or a retryable error occurs, falls back to `UserTweetsAndReplies(count=5)`.
     - Writes `follower_tweets/{screen_name}_tweets.json` with `{ profile, tweets, sourceUsername }`.
     - Writes raw responses to `raw_api_responses/` for debugging.

8. **AI analysis + Notion upload triage**
   - Sorts tweet files by “complexity” (size + text length) to reduce token risk.
   - For each tweet file:
     - Builds an LLM prompt from `prompts/tweet_analysis_prompt.txt` (with live Notion categories inserted).
     - Calls OpenAI with a strict “JSON object” response format.
     - **Skips Notion upload** if categories are `["Profile"]` only, or if meme/NFT categories are present; still records the handle in the dedup DB with `notion_page_id=None`.
     - Otherwise uploads a page to Notion and records `notion_page_id` in the dedup DB.

9. **Persist counts + summarize**
   - Writes a new dated follower counts CSV.
   - Logs run summary and skip breakdown (AI-triage skips).
   - Optional S3 sync: uploads the updated DB + newest follower counts file.

## Detailed Filtering: Account Age + “What Gets Processed”

This is the most important gate for deciding which discovered accounts turn into tweet collection + AI analysis work.

### Where this happens

- The filter lives inside `process_username()` in `main.py`, and runs **after** a global dedup check via `DeduplicationService.process_profile()`.

### Step 0: “Is this source eligible to produce work?”

- If the source account is **new** (no previous `follower_counts_*.csv` entry): it returns “Baseline” and produces **no work items**.
- If the source’s following count **did not change** (`count_diff == 0`): produces **no work items**.

Only sources with a nonzero diff proceed to fetch and filter recent followings.

### Step 1: Global dedup check (first gate)

For each candidate account returned by `FollowingLight`:

- If the handle already exists in `db/twitter_profiles.db`: **skip immediately** (no filtering, no tweet collection, no AI).
- If it has never been seen: it proceeds to the age/size filter.

### Step 2: Account age calculation (EST-normalized)

- Reads `created_at` from the Twitter API user object (string like `Mon Apr 29 00:00:00 +0000 2024`).
- If `created_at` is missing or cannot be parsed: `account_age_days` defaults to `181` (treated as “old”).
- Otherwise:
  - Parses with `datetime.strptime(..., '%a %b %d %H:%M:%S %z %Y')`
  - Converts `created_at` and `now` to `America/New_York`
  - Computes `account_age_days = (now_est - created_at_est).days`

### Step 3: “Should we process this account?”

Current rule in `main.py` (hard-coded thresholds):

```text
is_new_account = (account_age_days <= 180)
low_counts     = (followers_count < 1000) AND (friends_count < 1000)

should_process = is_new_account OR low_counts
```

- **Included (sent for processing)** if:
  - The account is **new** (≤ 180 days), even if it has high follower/following counts, OR
  - The account is **small-ish** (both followers and following under 1000), even if it is old
- **Excluded (not sent for processing)** if:
  - The account is **old** (> 180 days *or unknown age*) AND
  - It has **high** followers or following (either count ≥ 1000)

### Step 4: What happens on include vs exclude

- **If included**: the account is appended to the “new followings” list and later:
  - Tweet collection runs (`*_tweets.json` is written)
  - AI analysis runs (and may still be skipped at Notion upload time if classified as `Profile`/meme)
- **If excluded**: it is still recorded in the dedup DB via `record_new_profile(... notion_page_id=None ...)`.
  - This means excluded accounts are treated as “processed” and won’t be reconsidered later unless the DB is reset.

### Filtering decision diagram

```mermaid
flowchart TD
  Candidate[Candidate following user] --> Dedup{Exists in dedup DB?}
  Dedup -->|Yes| SkipDup[Skip\n(already processed)]
  Dedup -->|No| CreatedAt{created_at present\nand parseable?}
  CreatedAt -->|No| AgeDefault[account_age_days = 181\n(treated old)]
  CreatedAt -->|Yes| AgeCalc[account_age_days = (now_EST - created_at_EST).days]

  AgeDefault --> NewCheck{account_age_days <= 180?}
  AgeCalc --> NewCheck

  NewCheck -->|Yes| IncludeNew[Include\nNew account]
  NewCheck -->|No| CountsCheck{followers < 1000\nAND following < 1000?}
  CountsCheck -->|Yes| IncludeSmall[Include\nLow counts]
  CountsCheck -->|No| Exclude[Exclude\nOld/unknown age + high counts\nRecord in DB as processed]

  IncludeNew --> Enqueue[Enqueue for tweet fetch + AI]
  IncludeSmall --> Enqueue
```

### Note on configuration vs implementation

`config.py` defines `MAX_FOLLOWERS`, `MAX_FOLLOWING`, and `MAX_ACCOUNT_AGE_DAYS`, but `main.py` currently uses hard-coded values (`1000` and `180`) in the followings filter logic. If you intend these limits to be configurable, the filter needs to be wired to those config constants.
