# twitter_posts.py – Download, Backfill, and Process Tweets

This utility fetches a user's complete history of tweets/replies via RapidAPI, saves the raw pagination pages, optionally backfills missing referenced tweets, and produces two processed outputs:

- `<user>_<timestamp>_clean.json` – rich output with minimal per‑tweet fields, threading, and summary stats.
- `<user>_<timestamp>_clean_slim.json` – further reduced output (threads only, minimal fields).

The raw pagination is always saved as `<user>_<timestamp>.json`.

## Command Line Usage

Run from the project root:

```bash
python api/twitter_posts.py [options] [username]
```

Options:
- `--pages-only` – Fetch and save pagination pages only; skip backfill and processing.
- `--backfill-only --raw-file <path>` – Reuse an existing raw JSON; run backfill + processing to produce clean and slim files. `username` is optional in this mode.
- `--process-only --raw-file <path>` – Reuse an existing raw JSON; process it into clean and slim files without backfill (useful for testing formatting/sorting).

Positional:
- `username` – Twitter handle (with or without `@`). Required unless using `--backfill-only` or `--process-only` with `--raw-file`.

## What Each Mode Does
DAAHCgABG7shiru_9RoLAAIAAAATMTQ3MzczMjQ2NDkzMzgwMTk5OAgAAwAAAAIAAA
### Full run (default)
1. Resolves user id.
2. Paginates `/UserTweetsReplies`, saving each page to raw JSON as it arrives.
3. Detects referenced tweet IDs not present in the pages (including other authors).
4. Backfills missing IDs in batches of 20 (up to 10 concurrent calls) via `/TweetResultsByRestIds`, with retries on 400s.
5. Writes `_clean.json` and `_clean_slim.json` (newest→oldest roots; replies oldest→newest).

### Pages only
- Fetches pages and writes the raw file; no backfill or processed outputs.

### Backfill only
- Reads an existing raw file, runs missing-ID detection and backfill, writes clean and slim outputs. Raw is unchanged.

### Process only
- Reads an existing raw file, skips backfill, and writes clean and slim outputs from what’s already in the pages.

## Outputs

- **Raw**: `<user>_<timestamp>.json` – pagination pages as returned by the API.
- **Clean**: `<user>_<timestamp>_clean.json`
  - `tweetsFlat`: newest→oldest.
  - `threads`: roots newest→oldest; replies oldest→newest.
  - Minimal per-tweet fields: id, createdAt, text, conversationId, inReplyToStatusId, quotedStatusId, author (id, screenName), engagement, urls.
  - Summary: page counts, totals, missingIds (backfilled), tweet breakdown, engagement totals, orphanReplies.
- **Clean Slim**: `<user>_<timestamp>_clean_slim.json`
  - Threads only, stripped fields: id, createdAt, text, conversationId, inReplyToStatusId, quotedStatusId, author (id, screenName), replies.

## Logging and Persistence

- Raw pages are written incrementally during pagination.
- Backfill logs batch counts and retries; batches that keep failing are skipped, but processing continues.

## Notes

- Backfill includes tweets by other authors if they’re referenced but absent in the raw pages.
- Raw files are never mutated; backfill results live only in the clean outputs.

