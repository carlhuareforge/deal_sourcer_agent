import pytest
from unittest.mock import AsyncMock, patch
from datetime import datetime, timedelta, timezone


pytestmark = pytest.mark.asyncio


def _fmt_created_at(days_ago: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return dt.strftime('%a %b %d %H:%M:%S %z %Y')


async def _fake_following_response(users):
    return {
        "status": 200,
        "statusText": "OK",
        "headers": {},
        "data": {"users": users},
    }


async def test_get_following_builds_correct_request(monkeypatch):
    # Import inside to ensure patches work with module-level references
    from api.twitter_client import TwitterClient

    captured = {}

    async def fake_make_http_request(options):
        captured.update(options)
        # minimal valid wrapper
        return {"status": 200, "statusText": "OK", "headers": {}, "data": {"users": []}}

    # Patch the underlying HTTP call to capture options
    monkeypatch.setattr('api.twitter_client.make_http_request', fake_make_http_request)

    client = TwitterClient()
    await client.get_following('carlhua', 5)

    assert captured['url'] == 'https://twitter283.p.rapidapi.com/FollowingLight'
    assert captured['params'] == {"username": "carlhua", "count": "5"}
    # Host header must match provider host for RapidAPI
    assert captured['headers']['x-rapidapi-host'] == 'twitter283.p.rapidapi.com'


@patch('main.get_previous_follower_count', new_callable=AsyncMock, return_value=0)
@patch('main.throttled_rapid_api_request')
async def test_process_username_parses_users_from_new_endpoint(mock_throttled, _mock_prev):
    # Provide two users; one should pass filters, one should be skipped
    users = [
        {
            "screen_name": "new_friend_ok",
            "followers_count": 100,
            "friends_count": 50,
            "created_at": _fmt_created_at(30),
            "id_str": "111"
        },
        {
            "screen_name": "old_high_follower_skip",
            "followers_count": 99999,
            "friends_count": 10,
            "created_at": _fmt_created_at(365),
            "id_str": "222"
        },
    ]

    mock_throttled.return_value = await _fake_following_response(users)

    from main import process_username
    result = await process_username('testuser', is_new_username=False, following_counts={'testuser': 102})

    assert result['total'] == 102
    assert result['new'] >= 1  # at least one new following considered
    assert any(f['screen_name'] == 'new_friend_ok' for f in result['followings'])
    assert all(f['screen_name'] != 'old_high_follower_skip' for f in result['followings'])


@patch('main.get_previous_follower_count', new_callable=AsyncMock, return_value=0)
@patch('main.throttled_rapid_api_request')
async def test_process_username_private_account_short_circuit(mock_throttled, _mock_prev):
    mock_throttled.return_value = {"data": {"error": "Not authorized."}}

    from main import process_username
    result = await process_username('privateacct', is_new_username=False, following_counts={'privateacct': 10})

    assert result['total'] == 10
    assert result['new'] == 0
    assert result['followings'] == []


@patch('main.get_previous_follower_count', new_callable=AsyncMock, return_value=0)
@patch('main.throttled_rapid_api_request')
async def test_process_username_no_users_raises(mock_throttled, _mock_prev):
    mock_throttled.return_value = {"data": {"users": []}}

    from main import process_username
    with pytest.raises(ValueError):
        await process_username('empty', is_new_username=False, following_counts={'empty': 2})

