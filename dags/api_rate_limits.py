"""Shared quota / rate-limit detection for YouTube and TikTok ingest."""

from __future__ import annotations

from airflow.exceptions import AirflowFailException  # pyright: ignore[reportMissingImports]

YOUTUBE_QUOTA_REASONS = frozenset(
    {
        "quotaExceeded",
        "dailyLimitExceeded",
        "rateLimitExceeded",
        "userRateLimitExceeded",
    }
)


class PlatformRateLimitError(AirflowFailException):
    """Platform API daily quota or rate limit exhausted."""


def youtube_quota_reason(response) -> str | None:
    if response is None:
        return None
    if response.status_code == 429:
        return "rateLimitExceeded"
    if response.status_code != 403:
        return None
    try:
        errors = response.json().get("error", {}).get("errors", [])
        for err in errors:
            reason = err.get("reason")
            if reason in YOUTUBE_QUOTA_REASONS:
                return reason
    except Exception:
        pass
    return None


def fail_if_youtube_quota(response, *, context: str) -> None:
    reason = youtube_quota_reason(response)
    if reason:
        raise PlatformRateLimitError(
            f"YouTube API quota/rate limit hit ({reason}) while {context}. "
            "Daily quota resets at midnight Pacific Time. Re-run after reset; "
            "completed records remain checkpointed in Mongo."
        )


def fail_if_tiktok_rate_limit(response, *, context: str) -> None:
    if response is not None and response.status_code == 429:
        raise PlatformRateLimitError(
            f"TikTok API rate limit hit (HTTP 429) while {context}. "
            "Daily quota resets at midnight UTC. Re-run after reset; "
            "completed records remain checkpointed in Mongo."
        )
