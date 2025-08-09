import asyncio
import json
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from main import (
    setup_directories,
    read_input_usernames,
    load_prompt,
    prepare_analysis_prompt,
    analyze_tweets_with_ai,
    get_following_counts,
    process_username,
    collect_tweets_for_new_followers,
    main as main_workflow
)
from config import (
    INPUT_FILE, HISTORY_DIR, OUTPUT_DIR, TWEETS_DIR, INPUT_HISTORY_DIR,
    PROMPTS_DIR, FOLLOWER_COUNTS_DIR, LOGS_DIR, RAW_RESPONSES_DIR, AI_TWEETS_DIR,
    MAX_PROFILES, MAX_FOLLOWERS, MAX_FOLLOWING, MAX_ACCOUNT_AGE_DAYS
)

# Pytest marker for async tests
pytestmark = pytest.mark.asyncio

@pytest.fixture
def mock_env(monkeypatch):
    """Mock environment variables."""
    monkeypatch.setenv("RAPID_API_KEY", "test_rapid_api_key")
    monkeypatch.setenv("OPENAI_API_KEY", "test_openai_api_key")
    monkeypatch.setattr("config.DEBUG_MODE", True)
    monkeypatch.setattr("config.MAX_PROFILES", "5")
    monkeypatch.setattr("config.MAX_FOLLOWERS", "10000")
    monkeypatch.setattr("config.MAX_FOLLOWING", "5000")
    monkeypatch.setattr("config.MAX_ACCOUNT_AGE_DAYS", "30")
    monkeypatch.setattr("config.CONCURRENT_PROCESSES", "1")


@pytest.fixture
def fake_fs(fs, mock_env):
    """Setup a fake file system with necessary directories and files."""
    # Create directories
    dirs = [
        HISTORY_DIR, OUTPUT_DIR, TWEETS_DIR, INPUT_HISTORY_DIR,
        PROMPTS_DIR, FOLLOWER_COUNTS_DIR, LOGS_DIR, RAW_RESPONSES_DIR, AI_TWEETS_DIR
    ]
    for d in dirs:
        fs.create_dir(d)

    # Create input files
    fs.create_file(
        INPUT_FILE,
        contents="screen_name,user_id\njohndoe,123\njanedoe,456\n"
    )
    fs.create_file(
        os.path.join(PROMPTS_DIR, "tweet_analysis_prompt.txt"),
        contents="Analyze these tweets. Categories: {categories}"
    )
    return fs

# --- Test Individual Functions ---

async def test_setup_directories(fake_fs):
    """Test that all necessary directories are created."""
    await setup_directories()
    for directory in [HISTORY_DIR, OUTPUT_DIR, TWEETS_DIR, INPUT_HISTORY_DIR,
                      FOLLOWER_COUNTS_DIR, LOGS_DIR, RAW_RESPONSES_DIR, AI_TWEETS_DIR]:
        assert os.path.exists(directory)

async def test_read_input_usernames(fake_fs):
    """Test reading and parsing of the input CSV."""
    profiles = await read_input_usernames()
    assert len(profiles) == 2
    assert profiles[0]["screen_name"] == "johndoe"
    assert profiles[0]["user_id"] == "123"
    assert profiles[1]["screen_name"] == "janedoe"
    assert profiles[1]["user_id"] == "456"

async def test_read_input_usernames_respects_max_profiles(fake_fs, monkeypatch):
    """Test that MAX_PROFILES limit is respected."""
    monkeypatch.setattr("config.MAX_PROFILES", "1")
    profiles = await read_input_usernames()
    assert len(profiles) == 1
    assert profiles[0]["screen_name"] == "johndoe"

async def test_load_prompt(fake_fs):
    """Test that the prompt is loaded correctly."""
    prompt = await load_prompt("tweet_analysis_prompt.txt")
    assert prompt == "Analyze these tweets. Categories: {categories}"

async def test_prepare_analysis_prompt(fake_fs, monkeypatch):
    """Test that categories are correctly injected into the prompt."""
    monkeypatch.setattr("main.notion_categories", ["VC", "Profile"])
    prompt = await prepare_analysis_prompt()
    assert prompt == 'Analyze these tweets. Categories: ["VC", "Profile"]'

@patch("main.throttled_rapid_api_request")
async def test_get_following_counts(mock_throttled_request, fake_fs):
    """Test retrieval of following counts."""
    mock_throttled_request.return_value = {
        "data": {
            "users": [
                {"result": {"legacy": {"screen_name": "johndoe", "friends_count": 100}}},
                {"result": {"legacy": {"screen_name": "janedoe", "friends_count": 200}}},
            ]
        }
    }

    profiles = [{"screen_name": "johndoe", "user_id": "123"}, {"screen_name": "janedoe", "user_id": "456"}]
    counts = await get_following_counts(profiles)

    assert len(counts) == 2
    assert counts["johndoe"] == 100
    assert counts["janedoe"] == 200

@patch("main.throttled_rapid_api_request")
async def test_process_username_new_user(mock_throttled_request, fake_fs):
    """Test processing a user who is new (no previous data)."""
    result = await process_username("newuser", is_new_username=True, following_counts={"newuser": 50})
    assert result["total"] == 50
    assert result["new"] == 0  # No new followings detected for baseline
    assert not result["followings"]

@patch("main.get_previous_follower_count", new_callable=AsyncMock)
@patch("main.throttled_rapid_api_request")
async def test_process_username_no_change(mock_throttled_request, mock_get_prev_count, fake_fs):
    """Test processing a user with no change in following count."""
    mock_get_prev_count.return_value = 100
    result = await process_username("testuser", is_new_username=False, following_counts={"testuser": 100})
    assert result["new"] == 0
    assert not result["followings"]
    mock_throttled_request.assert_not_called()

@patch("main.get_previous_followings", new_callable=AsyncMock, return_value=[])
@patch("main.get_previous_follower_count", new_callable=AsyncMock, return_value=100)
@patch("main.throttled_rapid_api_request")
async def test_process_username_with_new_followings(mock_throttled_request, mock_get_prev_count, mock_get_prev_followings, fake_fs):
    """Test processing a user who has new followings."""
    mock_throttled_request.return_value = {
        "data": {
            "users": [
                {"screen_name": "new_friend_1", "followers_count": 100, "friends_count": 100, "created_at": "Mon Apr 29 00:00:00 +0000 2024"},
                {"screen_name": "new_friend_2_skipped", "followers_count": 99999, "friends_count": 100, "created_at": "Mon Apr 29 00:00:00 +0000 2020"},
            ]
        }
    }
    result = await process_username("testuser", is_new_username=False, following_counts={"testuser": 102})

    assert result["new"] == 1
    assert len(result["followings"]) == 1
    assert result["followings"][0]["screen_name"] == "new_friend_1"

@patch("services.deduplication_service.DeduplicationService.process_profile", new_callable=AsyncMock)
@patch("main.throttled_rapid_api_request")
async def test_collect_tweets_for_new_followers(mock_throttled_request, mock_process_profile, fake_fs):
    """Test collecting tweets for a list of new followers."""
    mock_process_profile.return_value = {"isNew": True}
    mock_throttled_request.return_value = {
        "data": {"tweets": [{"text": "a tweet"}]}
    }
    
    new_followings = [{"screen_name": "new_dev", "id_str": "789", "followers_count": 500, "friends_count": 500, "created_at": "Mon Apr 29 00:00:00 +0000 2024"}]
    await collect_tweets_for_new_followers(new_followings, "source_user")

    # Check that a tweet file was created
    expected_file = os.path.join(TWEETS_DIR, "new_dev_tweets.json")
    assert os.path.exists(expected_file)
    with open(expected_file, "r") as f:
        data = json.load(f)
        assert data["profile"]["screen_name"] == "new_dev"
        assert len(data["tweets"]) > 0

@patch("services.deduplication_service.DeduplicationService.record_new_profile", new_callable=AsyncMock)
@patch("main.add_notion_database_entry", new_callable=AsyncMock)
@patch("main.prepare_analysis_prompt", new_callable=AsyncMock, return_value="prompt")
@patch("main.create_throttler")
async def test_analyze_tweets_ai_skip_profile(mock_create_throttler, mock_prepare_prompt, mock_add_to_notion, mock_record_profile, fake_fs):
    """Test that AI analysis correctly skips a 'Profile' category and does not call Notion."""
    main.skipped_profiles = []
    # Mock throttler to just call the function
    mock_create_throttler.return_value = lambda fn: fn()

    # Mock OpenAI client
    mock_openai_completion = MagicMock()
    mock_openai_completion.choices[0].message.content = json.dumps({
        "name": "John Doe",
        "summary": "Just a person.",
        "categories": ["Profile"]
    })
    mock_openai_client = MagicMock()
    mock_openai_client.chat.completions.create = AsyncMock(return_value=mock_openai_completion)

    with patch("main.openai_client", mock_openai_client):
        # Create a dummy tweet file
        tweet_file_path = os.path.join(TWEETS_DIR, "johndoe_tweets.json")
        fake_fs.create_file(tweet_file_path, contents=json.dumps({
            "profile": {"screen_name": "johndoe", "name": "John Doe"},
            "tweets": ["a tweet"],
            "sourceUsername": "source_user"
        }))

        result = await analyze_tweets_with_ai(tweet_file_path)

        assert result is None
        mock_add_to_notion.assert_not_called()
        # Check that it was recorded for deduplication
        mock_record_profile.assert_called_once()
        assert "johndoe" in main.skipped_profiles[0]["username"]


@patch("services.deduplication_service.DeduplicationService.record_new_profile", new_callable=AsyncMock)
@patch("main.add_notion_database_entry", new_callable=AsyncMock)
@patch("main.prepare_analysis_prompt", new_callable=AsyncMock, return_value="prompt")
@patch("main.create_throttler")
async def test_analyze_tweets_ai_upload(mock_create_throttler, mock_prepare_prompt, mock_add_to_notion, mock_record_profile, fake_fs, monkeypatch):
    """Test that AI analysis correctly processes a valid profile and calls Notion."""
    monkeypatch.setattr("main.notion_categories", ["VC", "Startup"])
    mock_create_throttler.return_value = lambda fn: fn()
    mock_add_to_notion.return_value = {"id": "notion-123"}

    mock_openai_completion = MagicMock()
    mock_openai_completion.choices[0].message.content = json.dumps({
        "name": "VC Firm",
        "summary": "An investment firm.",
        "categories": ["VC"]
    })
    mock_openai_client = MagicMock()
    mock_openai_client.chat.completions.create = AsyncMock(return_value=mock_openai_completion)

    with patch("main.openai_client", mock_openai_client):
        tweet_file_path = os.path.join(TWEETS_DIR, "vcfirm_tweets.json")
        fake_fs.create_file(tweet_file_path, contents=json.dumps({
            "profile": {"screen_name": "vcfirm", "name": "VC Firm"},
            "tweets": ["a tweet about money"],
            "sourceUsername": "source_user"
        }))

        result = await analyze_tweets_with_ai(tweet_file_path)

        assert result is not None
        assert result["id"] == "notion-123"
        mock_add_to_notion.assert_called_once()
        mock_record_profile.assert_called_once()


# --- Test Main Workflow ---

@patch("main.send_completion_email", new_callable=AsyncMock)
@patch("main.add_notion_database_entry", new_callable=AsyncMock)
@patch("main.get_existing_categories", new_callable=AsyncMock, return_value=["VC", "Profile"])
@patch("api.openai_client.get_openai_client")
@patch("main.throttled_rapid_api_request")
async def test_main_workflow_end_to_end(mock_throttled_request, mock_openai_client_get, mock_get_cats, mock_add_notion, mock_send_email, fake_fs, mock_env):
    """Test the full main() workflow with mocks."""
    main.skipped_profiles = []
    # --- Mock Twitter Client ---
    def twitter_side_effect(func):
        if "get_users_by_rest_ids" in str(func):
            return {
                "data": {"users": [
                    {"result": {"legacy": {"screen_name": "johndoe", "friends_count": 102}}},
                    {"result": {"legacy": {"screen_name": "janedoe", "friends_count": 200}}},
                ]}
            }
        if "get_following" in str(func):
            return {
                "data": {"users": [
                    {"screen_name": "new_follow_vc", "id_str": "1001", "followers_count": 500, "friends_count": 500, "created_at": "Mon Apr 29 00:00:00 +0000 2024"},
                    {"screen_name": "new_follow_profile", "id_str": "1002", "followers_count": 500, "friends_count": 500, "created_at": "Mon Apr 29 00:00:00 +0000 2024"},
                ]}
            }
        if "get_user_tweets" in str(func):
            return {"data": {"tweets": [{"text": "a tweet"}]}}
        return {}
    
    mock_throttled_request.side_effect = lambda func: asyncio.Future()._result_or_cancel(twitter_side_effect(func))


    # --- Mock OpenAI Client ---
    mock_openai_instance = MagicMock()
    # Configure different responses based on user
    async def openai_side_effect(*args, **kwargs):
        user_content = json.loads(kwargs['messages'][1]['content'])
        screen_name = user_content['profile']['screen_name']
        if screen_name == 'new_follow_vc':
            response_content = json.dumps({"name": "VC Firm", "summary": "...", "categories": ["VC"]})
        else: # new_follow_profile
            response_content = json.dumps({"name": "A Person", "summary": "...", "categories": ["Profile"]})
        
        completion = MagicMock()
        completion.choices[0].message.content = response_content
        return completion

    mock_openai_instance.chat.completions.create = openai_side_effect
    mock_openai_client_get.return_value = mock_openai_instance

    # --- Mock File System for previous state ---
    fake_fs.create_file(
        os.path.join(FOLLOWER_COUNTS_DIR, "follower_counts_2025-01-01_00-00-00.csv"),
        contents="johndoe,100\njanedoe,200"
    )
    fake_fs.create_file(
        os.path.join(HISTORY_DIR, "johndoe_2025-01-01_00-00-00.json"),
        contents='[]'
    )

    # --- Run the workflow ---
    with patch("main.DeduplicationService.process_profile", new_callable=AsyncMock, return_value={"isNew": True}):
         with patch("main.DeduplicationService.record_new_profile", new_callable=AsyncMock):
            stats = await main_workflow()

    # --- Assertions ---
    assert stats["totalProcessed"] == 2
    assert stats["totalUploaded"] == 1
    assert stats["totalSkipped"] == 1

    # Notion should be called once for the VC profile
    mock_add_notion.assert_called_once()
    
    # Email should not be called since we are in test mode (or we can check its call)
    # For this test, let's assume it's not called as per instructions.
    # In main code, it's commented out. If it were active, we'd check:
    # mock_send_email.assert_called_once()

    # Check that tweet files were created for both new follows
    assert os.path.exists(os.path.join(TWEETS_DIR, "new_follow_vc_tweets.json"))
    assert os.path.exists(os.path.join(TWEETS_DIR, "new_follow_profile_tweets.json"))

    # Check that AI analysis files were created
    ai_tweets_dir = os.path.join(TWEETS_DIR, "ai_tweets")
    assert os.path.exists(os.path.join(ai_tweets_dir, "new_follow_vc_ai_input.json"))
    assert os.path.exists(os.path.join(ai_tweets_dir, "new_follow_vc_ai_response.json"))
    assert os.path.exists(os.path.join(ai_tweets_dir, "new_follow_profile_ai_input.json"))
    assert os.path.exists(os.path.join(ai_tweets_dir, "new_follow_profile_ai_response.json"))