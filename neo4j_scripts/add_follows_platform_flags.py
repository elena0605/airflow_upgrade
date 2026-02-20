#!/usr/bin/env python3
"""
Add platform follow flags to follows.csv for manual review.

Creates:
  - follows_tiktok (True/False)
  - follows_youtube (True/False)
  - needs_manual_review (True/False)

By default, writes a new file next to follows.csv:
  follows_with_flags.csv
"""

import argparse
import difflib
import json
import os
import re
import sys
from typing import Dict, Set, Tuple

import pandas as pd


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT = os.path.join(SCRIPT_DIR, "follows.csv")
DEFAULT_OUTPUT = os.path.join(SCRIPT_DIR, "follows_with_flags.csv")
DEFAULT_INFLUENCERS_JSON = os.path.join(SCRIPT_DIR, "influencers.json")


def has_non_empty_value(value) -> bool:
    if pd.isna(value):
        return False
    s = str(value).strip()
    return s != "" and s.upper() != "NA"


def normalize_token(value: str) -> str:
    """Normalize text for robust equality checks."""
    if value is None:
        return ""
    token = str(value).strip().lower()
    if token.startswith("@"):
        token = token[1:]
    # Keep only letters/numbers for stable matching.
    token = re.sub(r"[^a-z0-9]+", "", token)
    return token


def parse_handle_tokens(value) -> Set[str]:
    """Parse comma-separated handles into normalized tokens."""
    if not has_non_empty_value(value):
        return set()
    tokens = set()
    for raw in str(value).split(","):
        token = normalize_token(raw)
        if token:
            tokens.add(token)
    return tokens


def is_similar_token(a: str, b: str) -> bool:
    """
    Robust similarity check for usernames/handles.
    - Exact match
    - Containment for longer tokens
    - High-ratio fuzzy match for close variants
    """
    if not a or not b:
        return False
    if a == b:
        return True

    shorter = a if len(a) <= len(b) else b
    longer = b if len(a) <= len(b) else a
    if len(shorter) >= 4 and shorter in longer:
        return True

    ratio = difflib.SequenceMatcher(None, a, b).ratio()
    return ratio >= 0.86


def any_similar_match(candidates: Set[str], handles: Set[str]) -> bool:
    """Return True if any candidate token is similar to any handle token."""
    if not candidates or not handles:
        return False
    for candidate in candidates:
        for handle in handles:
            if is_similar_token(candidate, handle):
                return True
    return False


def name_candidates(influencer_name: str) -> Set[str]:
    """
    Build normalized influencer-name tokens for fallback matching.
    Includes full name token and meaningful parts.
    """
    raw = str(influencer_name or "")
    candidates = set()
    full = normalize_token(raw)
    if full:
        candidates.add(full)

    for part in re.split(r"[\s\-/_.·]+", raw):
        token = normalize_token(part)
        if len(token) >= 3:
            candidates.add(token)
    return candidates


def load_influencer_lookup(influencers_json_path: str) -> Dict[str, Dict[str, Set[str]]]:
    """
    Load influencer name -> known platform username tokens from influencers.json.

    Expected shape:
      [
        {"n": {"properties": {"name": "...", "tiktok_username": "...", "youtube_username": "..."}}},
        ...
      ]
    """
    if not os.path.exists(influencers_json_path):
        return {}

    with open(influencers_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    lookup: Dict[str, Dict[str, Set[str]]] = {}
    for item in data:
        node = item.get("n", {}) if isinstance(item, dict) else {}
        props = node.get("properties", {}) if isinstance(node, dict) else {}
        name = str(props.get("name", "")).strip()
        if not name:
            continue

        key = normalize_token(name)
        if not key:
            continue

        tiktok_user = normalize_token(props.get("tiktok_username", ""))
        youtube_user = normalize_token(props.get("youtube_username", ""))

        if key not in lookup:
            lookup[key] = {"tiktok": set(), "youtube": set()}
        if tiktok_user:
            lookup[key]["tiktok"].add(tiktok_user)
        if youtube_user:
            lookup[key]["youtube"].add(youtube_user)

    return lookup


def match_with_lookup_only(
    influencer_name: str,
    tiktok_tokens: Set[str],
    youtube_tokens: Set[str],
    lookup: Dict[str, Dict[str, Set[str]]],
) -> Tuple[bool, bool, bool]:
    """
    Matching using influencer lookup:
      - find same influencer by normalized name in influencers.json
      - compare usernames against row handles using exact + similar-token matching
      - includes cross-fallback:
          tiktok_handle: try tiktok usernames, then youtube usernames
          youtube_handle: try youtube usernames, then tiktok usernames

    Returns:
      (is_tiktok, is_youtube, influencer_found_in_lookup)
    """
    key = normalize_token(influencer_name)
    platform_usernames = lookup.get(key)
    if not platform_usernames:
        return False, False, False

    known_tiktok = platform_usernames.get("tiktok", set())
    known_youtube = platform_usernames.get("youtube", set())
    influencer_name_tokens = name_candidates(influencer_name)

    # Primary platform match
    is_tiktok = any_similar_match(known_tiktok, tiktok_tokens)
    is_youtube = any_similar_match(known_youtube, youtube_tokens)

    # Cross-fallback when primary platform username is missing or doesn't match.
    if not is_tiktok:
        is_tiktok = any_similar_match(known_youtube, tiktok_tokens)
    if not is_youtube:
        is_youtube = any_similar_match(known_tiktok, youtube_tokens)

    # Name fallback when usernames are missing or still unmatched.
    if not is_tiktok:
        is_tiktok = any_similar_match(influencer_name_tokens, tiktok_tokens)
    if not is_youtube:
        is_youtube = any_similar_match(influencer_name_tokens, youtube_tokens)

    return is_tiktok, is_youtube, True


def add_flags(input_csv: str, output_csv: str, influencers_json: str) -> None:
    if not os.path.exists(input_csv):
        print(f"Input file not found: {input_csv}")
        sys.exit(1)

    df = pd.read_csv(input_csv, na_values=[], keep_default_na=False)
    required_cols = {"UserID", "Influencer", "tiktok_handle", "youtube_handle"}
    missing = required_cols.difference(df.columns)
    if missing:
        print(f"Missing required columns: {sorted(missing)}")
        sys.exit(1)

    lookup = load_influencer_lookup(influencers_json)
    print(f"Loaded influencer lookup entries: {len(lookup)}")

    follows_tiktok = []
    follows_youtube = []
    needs_review = []
    matched_with_lookup_count = 0
    missing_lookup_count = 0

    for _, row in df.iterrows():
        influencer = str(row.get("Influencer", "")).strip()
        tiktok_tokens = parse_handle_tokens(row.get("tiktok_handle", ""))
        youtube_tokens = parse_handle_tokens(row.get("youtube_handle", ""))

        is_tiktok, is_youtube, found_in_lookup = match_with_lookup_only(
            influencer, tiktok_tokens, youtube_tokens, lookup
        )

        if found_in_lookup:
            matched_with_lookup_count += 1
        else:
            missing_lookup_count += 1

        follows_tiktok.append(bool(is_tiktok))
        follows_youtube.append(bool(is_youtube))

        # Manual review only for high-risk rows:
        # - influencer missing in lookup, OR
        # - both platform matches failed while at least one handle list is present.
        tiktok_has_values = len(tiktok_tokens) > 0
        youtube_has_values = len(youtube_tokens) > 0
        review = (
            (not found_in_lookup)
            or (
                (tiktok_has_values or youtube_has_values)
                and (not is_tiktok)
                and (not is_youtube)
            )
        )
        needs_review.append(bool(review))

    df["follows_tiktok"] = follows_tiktok
    df["follows_youtube"] = follows_youtube
    df["needs_manual_review"] = needs_review

    df.to_csv(output_csv, index=False)

    total = len(df)
    t_count = int(df["follows_tiktok"].sum())
    y_count = int(df["follows_youtube"].sum())
    review_count = int(df["needs_manual_review"].sum())

    print("=" * 70)
    print(f"Input:  {input_csv}")
    print(f"Output: {output_csv}")
    print(f"Rows processed: {total}")
    print(f"Rows with influencer found in influencers.json: {matched_with_lookup_count}")
    print(f"Rows with influencer missing in influencers.json: {missing_lookup_count}")
    print(f"follows_tiktok=True: {t_count}")
    print(f"follows_youtube=True: {y_count}")
    print(f"needs_manual_review=True: {review_count}")
    print("=" * 70)


def parse_args():
    parser = argparse.ArgumentParser(description="Add follows_tiktok/follows_youtube flags to follows.csv")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Input follows CSV path")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output CSV path")
    parser.add_argument(
        "--influencers-json",
        default=DEFAULT_INFLUENCERS_JSON,
        help="Path to influencers.json exported from Neo4j",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    add_flags(args.input, args.output, args.influencers_json)

