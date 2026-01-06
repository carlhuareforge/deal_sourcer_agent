import json

from api.twitter_posts import (
    _extract_next_cursor,
    _compute_page_signature,
    _write_partial_results,
)


def test_extract_next_cursor_prefers_bottom_snake_case():
    payload = {
        "data": {
            "timeline": {
                "instructions": [
                    {
                        "entries": [
                            {
                                "content": {
                                    "__typename": "TimelineTimelineCursor",
                                    "cursor_type": "Top",
                                    "value": "TOP_CURSOR_VALUE",
                                }
                            },
                            {
                                "content": {
                                    "__typename": "TimelineTimelineCursor",
                                    "cursor_info": {"autoload_on_min_distance_below_timeline_viewport": 2.5},
                                    "cursor_type": "Bottom",
                                    "value": "BOTTOM_CURSOR_VALUE",
                                }
                            },
                        ]
                    }
                ]
            }
        }
    }

    assert _extract_next_cursor(payload) == "BOTTOM_CURSOR_VALUE"


def test_extract_next_cursor_supports_camel_case():
    payload = {
        "timeline": {
            "instruction": {"some": "data"}
        },
        "content": {
            "cursorType": "Bottom",
            "value": "CAMEL_BOTTOM_VALUE",
        },
    }

    assert _extract_next_cursor(payload) == "CAMEL_BOTTOM_VALUE"


def test_extract_next_cursor_fallbacks_to_other_cursor_fields():
    payload = {
        "timeline": {
            "instructions": [
                {
                    "entries": [
                        {"content": {"some": "data"}},
                        {"cursor": "FALLBACK_CURSOR"},
                    ]
                }
            ]
        }
    }

    assert _extract_next_cursor(payload) == "FALLBACK_CURSOR"


def test_write_partial_results_includes_page_number(tmp_path):
    filename = tmp_path / "partial.json"
    pages = [
        {
            "pageNumber": 1,
            "cursor": None,
            "requestedCursor": None,
            "nextCursor": "NEXT_CURSOR",
            "data": {"hello": "world"},
        }
    ]

    _write_partial_results(
        filename=filename,
        username="example",
        user_id="123",
        user_payload={"id": "123"},
        pages=pages,
        fetched_at="2025-01-01T00:00:00-05:00",
    )

    with open(filename, "r", encoding="utf-8") as f:
        payload = json.load(f)

    assert payload["tweetsAndReplies"]["pageCount"] == 1
    assert payload["tweetsAndReplies"]["pages"][0]["pageNumber"] == 1


def test_compute_page_signature_stable_and_order_insensitive():
    data_a = {"b": 2, "a": 1, "nested": {"x": 10, "y": [1, 2, 3]}}
    data_b = {"nested": {"y": [1, 2, 3], "x": 10}, "a": 1, "b": 2}

    sig_a = _compute_page_signature(data_a)
    sig_b = _compute_page_signature(data_b)

    assert sig_a == sig_b


def test_compute_page_signature_prefers_tweet_ids_over_cursor_changes():
    page1 = {
        "timeline": {
            "instructions": [
                {
                    "entries": [
                        {
                            "content": {
                                "content": {
                                    "tweet_results": {"rest_id": "TWEET123", "result": {"rest_id": "TWEET123"}}
                                }
                            },
                            "entry_id": "tweet-1",
                            "sort_index": "AAA",
                        },
                        {
                            "content": {"__typename": "TimelineTimelineCursor", "cursor_type": "Bottom", "value": "CUR_A"}
                        },
                    ]
                }
            ]
        }
    }

    page2 = {
        "timeline": {
            "instructions": [
                {
                    "entries": [
                        {
                            "content": {
                                "content": {
                                    "tweet_results": {"rest_id": "TWEET123", "result": {"rest_id": "TWEET123"}}
                                }
                            },
                            "entry_id": "tweet-1",
                            "sort_index": "BBB",  # different sort index
                        },
                        {
                            "content": {"__typename": "TimelineTimelineCursor", "cursor_type": "Bottom", "value": "CUR_B"}
                        },
                    ]
                }
            ]
        }
    }

    sig1 = _compute_page_signature(page1)
    sig2 = _compute_page_signature(page2)

    assert sig1 == sig2
