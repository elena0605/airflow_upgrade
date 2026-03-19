#!/usr/bin/env python3
"""
Backfill missing TikTok topic categories in Mongo from saved OpenAI batch outputs.

This script:
- Loads Mongo connection string from `.env` (MONGODB_URI preferred).
- Reads TikTok batch records from progress JSON.
- Parses output JSONL files and extracts topic/category/weight.
- Updates ONLY videos missing `comments_frequent_topic_categories`.
- Ensures index on `tiktok_user_video.video_id` for efficient updates.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from dotenv import load_dotenv  # pyright: ignore[reportMissingImports]
from pymongo import MongoClient, UpdateOne  # pyright: ignore[reportMissingImports]
from pymongo.errors import BulkWriteError, OperationFailure  # pyright: ignore[reportMissingImports]


logger = logging.getLogger("backfill_tiktok_comment_categories")


def _extract_analysis_from_output_line(line_obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    body = (((line_obj.get("response") or {}).get("body") or {}))
    choices = body.get("choices") or []
    if not choices:
        return None
    msg = ((choices[0] or {}).get("message") or {})
    content = msg.get("content") or ""
    if isinstance(content, list):
        joined = []
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                joined.append(part.get("text", ""))
        content = "\n".join(joined)
    txt = str(content).strip()
    if txt.startswith("```"):
        parts = txt.split("```")
        if len(parts) >= 2:
            txt = parts[1].strip()
            if txt.lower().startswith("json"):
                txt = txt[4:].strip()
    try:
        return json.loads(txt)
    except Exception:
        start = txt.find("{")
        end = txt.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(txt[start : end + 1])
            except Exception:
                return None
    return None


def _is_mongo_throttled(exc: Exception) -> bool:
    code = getattr(exc, "code", None)
    if code in (16500, 429):
        return True
    msg = str(exc).lower()
    return (
        "requestratetoolarge" in msg
        or "toomanyrequests" in msg
        or "error=16500" in msg
        or " 429" in msg
    )


def _retry_after_seconds(exc: Exception, attempt: int, min_sleep: float) -> float:
    match = re.search(r"RetryAfterMs=(\d+)", str(exc))
    if match:
        try:
            return max(min_sleep, int(match.group(1)) / 1000.0)
        except ValueError:
            pass
    return max(min_sleep, 0.5 * (2 ** (attempt - 1)))


def _safe_bulk_write(
    coll,
    ops: List[UpdateOne],
    *,
    max_retries: int,
    min_sleep: float,
    dry_run: bool,
) -> Dict[str, int]:
    if not ops:
        return {"sent": 0, "matched": 0, "modified": 0}
    if dry_run:
        return {"sent": len(ops), "matched": 0, "modified": 0}

    for attempt in range(1, max_retries + 1):
        try:
            res = coll.bulk_write(ops, ordered=False)
            return {
                "sent": len(ops),
                "matched": int(getattr(res, "matched_count", 0) or 0),
                "modified": int(getattr(res, "modified_count", 0) or 0),
            }
        except (BulkWriteError, OperationFailure) as e:
            if (not _is_mongo_throttled(e)) or attempt == max_retries:
                raise
            sleep_s = _retry_after_seconds(e, attempt, min_sleep)
            logger.warning(
                "Mongo throttled during bulk_write (attempt %d/%d). Sleeping %.2fs.",
                attempt,
                max_retries,
                sleep_s,
            )
            time.sleep(sleep_s)
    raise RuntimeError("bulk_write retry loop exhausted")


def _iter_completed_records(progress_path: Path) -> Iterable[Dict[str, Any]]:
    data = json.loads(progress_path.read_text(encoding="utf-8"))
    completed = (data.get("completed") or {})
    if not isinstance(completed, dict):
        return []
    return list(completed.values())


def _resolve_recorded_path(recorded_path: str, repo_root: Path) -> Path:
    """
    Resolve paths stored by Airflow containers to local workspace paths.

    Progress entries typically store `/opt/airflow/dags/...`; on local workspace this
    should map to `<repo_root>/dags/...`.
    """
    p = Path(recorded_path)
    if p.exists():
        return p

    # Map container path -> local repo path.
    marker = "/opt/airflow/"
    if recorded_path.startswith(marker):
        rel = recorded_path[len(marker) :]
        mapped = repo_root / rel
        if mapped.exists():
            return mapped

    # Fallback for relative entries.
    mapped_relative = repo_root / recorded_path.lstrip("/")
    if mapped_relative.exists():
        return mapped_relative

    # Return best-effort path for logging visibility.
    return p


def _is_within(path: Path, base_dir: Path) -> bool:
    try:
        path.resolve().relative_to(base_dir.resolve())
        return True
    except Exception:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill TikTok topic categories from saved batch outputs.")
    parser.add_argument(
        "--progress-path",
        default="dags/tmp_openai_batches/tiktok_comments/tiktok_comments_analysis_progress.json",
        help="Path to TikTok comments progress JSON file.",
    )
    parser.add_argument("--mongo-db", default=None)
    parser.add_argument("--collection", default="tiktok_user_video")
    parser.add_argument(
        "--outputs-dir",
        default="/opt/airflow/dags/tmp_openai_batches/tiktok_comments",
        help="Only process output/meta files under this directory.",
    )
    parser.add_argument("--chunk-size", type=int, default=200)
    parser.add_argument("--max-retries", type=int, default=8)
    parser.add_argument("--min-sleep", type=float, default=1.0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env", override=True)
    mongo_uri = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI_PROD")
    if not mongo_uri:
        raise RuntimeError("Missing MONGODB_URI/MONGO_URI_PROD in environment.")
    mongo_db = args.mongo_db or os.getenv("MONGODB_DB", "rbl")
    allowed_dir_raw = Path(args.outputs_dir)
    allowed_dirs = {allowed_dir_raw}
    # Also allow mapped local workspace equivalent of container path.
    mapped_allowed = _resolve_recorded_path(str(allowed_dir_raw), repo_root)
    allowed_dirs.add(mapped_allowed)

    progress_path = Path(args.progress_path)
    if not progress_path.is_absolute():
        progress_path = repo_root / progress_path
    if not progress_path.exists():
        raise FileNotFoundError(f"Progress file not found: {progress_path}")

    client = MongoClient(mongo_uri)
    coll = client[mongo_db][args.collection]

    # Needed for performant point updates by video_id.
    if not args.dry_run:
        try:
            coll.create_index("video_id")
            logger.info("Ensured index: %s.%s(video_id)", mongo_db, args.collection)
        except Exception as e:
            logger.warning("Could not create index on video_id: %s", e)

    updates: List[UpdateOne] = []
    files_seen = files_skipped = rows_parsed = 0

    for rec in _iter_completed_records(progress_path):
        output_file = rec.get("output_file")
        meta_file = rec.get("meta_file")
        if not output_file or not meta_file:
            files_skipped += 1
            continue
        output_path = _resolve_recorded_path(str(output_file), repo_root)
        meta_path = _resolve_recorded_path(str(meta_file), repo_root)
        if not output_path.exists() or not meta_path.exists():
            logger.warning(
                "Skipping missing files: output=%s (raw=%s), meta=%s (raw=%s)",
                output_path,
                output_file,
                meta_path,
                meta_file,
            )
            files_skipped += 1
            continue
        if not any(_is_within(output_path, d) for d in allowed_dirs):
            logger.warning(
                "Skipping output outside allowed directory: %s (raw=%s). Allowed: %s",
                output_path,
                output_file,
                ", ".join(str(d) for d in allowed_dirs),
            )
            files_skipped += 1
            continue
        if not any(_is_within(meta_path, d) for d in allowed_dirs):
            logger.warning(
                "Skipping meta outside allowed directory: %s (raw=%s). Allowed: %s",
                meta_path,
                meta_file,
                ", ".join(str(d) for d in allowed_dirs),
            )
            files_skipped += 1
            continue

        meta_by_custom_id: Dict[str, Any] = {}
        try:
            meta_list = json.loads(meta_path.read_text(encoding="utf-8"))
            if isinstance(meta_list, list):
                for m in meta_list:
                    if isinstance(m, dict) and m.get("custom_id") is not None:
                        meta_by_custom_id[str(m.get("custom_id"))] = m.get("video_id")
        except Exception as e:
            logger.warning("Skipping meta parse failure %s: %s", meta_path, e)
            files_skipped += 1
            continue

        with output_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                custom_id = str(obj.get("custom_id", "")).strip()
                if not custom_id:
                    continue
                video_id = meta_by_custom_id.get(custom_id, custom_id)

                analysis = _extract_analysis_from_output_line(obj)
                if not analysis:
                    continue
                topics = analysis.get("topics") or []

                topic_names: List[str] = []
                topic_categories: List[str] = []
                topic_weights: List[float] = []

                for t in topics:
                    if not isinstance(t, dict):
                        continue
                    name = (t.get("topic") or "").strip() if isinstance(t.get("topic"), str) else ""
                    if not name:
                        continue
                    topic_names.append(name)
                    category = (t.get("category") or "").strip() if isinstance(t.get("category"), str) else ""
                    topic_categories.append(category)
                    try:
                        topic_weights.append(float(t.get("weight", 0.0)))
                    except Exception:
                        topic_weights.append(0.0)

                if not topic_names:
                    continue

                # Backfill only docs where categories are missing/empty.
                updates.append(
                    UpdateOne(
                        {
                            "video_id": video_id,
                            "$or": [
                                {"comments_frequent_topic_categories": {"$exists": False}},
                                {"comments_frequent_topic_categories": None},
                                {"comments_frequent_topic_categories": []},
                                {"comments_frequent_topic_categories": {"$size": 0}},
                            ],
                        },
                        {
                            "$set": {
                                "comments_frequent_topics": topic_names,
                                "comments_frequent_topic_categories": topic_categories,
                                "comments_frequent_topic_weights": topic_weights,
                                "comments_frequent_topics_json": json.dumps(
                                    [
                                        {"topic": n, "category": c, "weight": w}
                                        for n, c, w in zip(topic_names, topic_categories, topic_weights)
                                    ],
                                    ensure_ascii=False,
                                ),
                            }
                        },
                    )
                )
                rows_parsed += 1
        files_seen += 1

    logger.info(
        "Prepared updates from outputs: files_seen=%d files_skipped=%d parsed_rows=%d candidate_updates=%d",
        files_seen,
        files_skipped,
        rows_parsed,
        len(updates),
    )

    sent = matched = modified = 0
    for i in range(0, len(updates), max(1, args.chunk_size)):
        chunk = updates[i : i + args.chunk_size]
        stats = _safe_bulk_write(
            coll,
            chunk,
            max_retries=max(1, args.max_retries),
            min_sleep=max(0.1, args.min_sleep),
            dry_run=args.dry_run,
        )
        sent += stats["sent"]
        matched += stats["matched"]
        modified += stats["modified"]
        logger.info(
            "Chunk %d/%d: sent=%d matched=%d modified=%d",
            (i // args.chunk_size) + 1,
            (len(updates) + args.chunk_size - 1) // args.chunk_size if updates else 0,
            stats["sent"],
            stats["matched"],
            stats["modified"],
        )

    logger.info(
        "Backfill complete (dry_run=%s): total_sent=%d total_matched=%d total_modified=%d",
        args.dry_run,
        sent,
        matched,
        modified,
    )


if __name__ == "__main__":
    main()
