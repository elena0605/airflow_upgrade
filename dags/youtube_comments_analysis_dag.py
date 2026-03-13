"""
Batch AI analysis for YouTube top-level comments.

This DAG:
- Reads comments from MongoDB `youtube_video_comments` only (replies excluded)
- Groups comments by video and sends one OpenAI Batch request per video
- Uses OpenAI Batch API (Azure OpenAI compatible) for scalable analysis
- Updates analysis fields back into MongoDB `youtube_video_comments`
- Updates matching `:YouTubeVideo` nodes in Neo4j with topics + summary

No comment truncation is applied.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import json
import logging
import os
import re
import time
from typing import Any, Dict, List

from airflow import DAG  # pyright: ignore[reportMissingImports]
from airflow.operators.python import PythonOperator  # pyright: ignore[reportMissingImports]
from airflow.providers.mongo.hooks.mongo import MongoHook  # pyright: ignore[reportMissingImports]
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook  # pyright: ignore[reportMissingImports]
from openai import OpenAI  # pyright: ignore[reportMissingImports]
from openai import AzureOpenAI  # pyright: ignore[reportMissingImports]
from pymongo.operations import UpdateOne  # pyright: ignore[reportMissingImports]
from pymongo.errors import BulkWriteError, OperationFailure  # pyright: ignore[reportMissingImports]

from callbacks import task_failure_callback, task_success_callback

logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

airflow_env = os.getenv("AIRFLOW_ENV", "development")
mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
neo4j_conn_id = "neo4j_prod" if airflow_env == "production" else "neo4j_default"
mongo_db_name = "rbl" if airflow_env == "production" else "airflow_db"

CHUNK_SIZE = int(os.getenv("YT_COMMENT_ANALYSIS_CHUNK_SIZE", "100"))
MONGO_BATCH_WRITE_CHUNK = int(os.getenv("YT_COMMENT_MONGO_WRITE_CHUNK", "100"))
NEO4J_CHUNK = int(os.getenv("YT_COMMENT_NEO4J_CHUNK", "100"))
POLL_INTERVAL = int(os.getenv("OPENAI_POLL_INTERVAL", "10"))
MAX_WRITE_RETRIES = int(os.getenv("YT_COMMENT_MAX_WRITE_RETRIES", "3"))
# Guardrail for very large in-memory JSONL batches.
MAX_INPUT_FILE_BYTES = int(os.getenv("YT_COMMENT_BATCH_MAX_BYTES", str(20 * 1024 * 1024)))
MONGO_READ_MAX_RETRIES = int(os.getenv("YT_COMMENT_MONGO_READ_MAX_RETRIES", "8"))
MONGO_WRITE_MAX_RETRIES = int(os.getenv("YT_COMMENT_MONGO_WRITE_MAX_RETRIES", "8"))
MONGO_THROTTLE_MIN_SLEEP_SECONDS = float(os.getenv("YT_COMMENT_MONGO_THROTTLE_MIN_SLEEP_SECONDS", "1.0"))
OPENAI_SUBMIT_MAX_RETRIES = int(os.getenv("YT_COMMENT_OPENAI_SUBMIT_MAX_RETRIES", "60"))
OPENAI_SUBMIT_RETRY_BASE_SECONDS = int(os.getenv("YT_COMMENT_OPENAI_SUBMIT_RETRY_BASE_SECONDS", "15"))
OPENAI_SUBMIT_RETRY_MAX_SECONDS = int(os.getenv("YT_COMMENT_OPENAI_SUBMIT_RETRY_MAX_SECONDS", "300"))
OPENAI_BATCH_FILE_KEEP_COUNT = int(os.getenv("YT_COMMENT_OPENAI_BATCH_FILE_KEEP_COUNT", "50"))
STATE_COLLECTION_NAME = os.getenv("YT_COMMENT_STATE_COLLECTION", "youtube_comment_analysis_state")
USE_STATE_COLLECTION = os.getenv("YT_COMMENT_USE_STATE_COLLECTION", "true").lower() in ("1", "true", "yes", "y")
# Legacy mirror writes into youtube_video_comments are expensive on Cosmos and not required
# when state collection is enabled. Keep them opt-in.
WRITE_LEGACY_COMMENTS_PROCESSED = os.getenv("YT_COMMENT_WRITE_LEGACY_COMMENTS_PROCESSED", "false").lower() in (
    "1",
    "true",
    "yes",
    "y",
)

TMP_DIR = Path(os.getenv("OPENAI_BATCH_TMP", "/opt/airflow/dags/tmp_openai_batches"))
TMP_DIR.mkdir(parents=True, exist_ok=True)
PROGRESS_PATH = TMP_DIR / "youtube_comments_analysis_progress.json"


def resolve_openai_model() -> str:
    return os.getenv("AZURE_OPENAI_MODEL") or os.getenv("OPENAI_MODEL", "gpt-4o-mini")


def get_openai_client():
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    azure_api_key = os.getenv("AZURE_OPENAI_API_KEY")
    azure_api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-10-21")

    if azure_endpoint and azure_api_key:
        azure_model = os.getenv("AZURE_OPENAI_MODEL")
        if azure_model:
            logger.info("Using Azure OpenAI client with deployment: %s", azure_model)
        else:
            logger.info("Using Azure OpenAI client (no AZURE_OPENAI_MODEL set)")
        return AzureOpenAI(
            api_key=azure_api_key,
            api_version=azure_api_version,
            azure_endpoint=azure_endpoint,
        )

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("Missing OPENAI_API_KEY and Azure OpenAI is not configured.")
    logger.info("Using regular OpenAI client.")
    return OpenAI(api_key=api_key)


def get_mongo_collections():
    mongo = MongoHook(mongo_conn_id=mongo_conn_id).get_conn()
    db = mongo[mongo_db_name]
    return db.youtube_video_comments, db.youtube_channel_videos, db[STATE_COLLECTION_NAME]


def ensure_analysis_indexes(comments_coll, state_coll) -> None:
    comments_coll.create_index(
        [("analysis_processed", 1), ("video_id", 1), ("comment_id", 1)],
        background=True,
    )
    if USE_STATE_COLLECTION:
        try:
            state_coll.create_index([("comment_id", 1)], unique=True, background=True)
        except Exception as e:
            # CosmosDB often disallows creating unique indexes on non-empty collections.
            # Fall back to a regular index so the DAG can continue processing.
            logger.warning(
                "Could not create unique index on %s.comment_id (%s). "
                "Falling back to a regular index.",
                STATE_COLLECTION_NAME,
                e,
            )
            state_coll.create_index([("comment_id", 1)], background=True)
        state_coll.create_index(
            [("analysis_processed", 1), ("video_id", 1), ("comment_id", 1)],
            background=True,
        )


def get_neo4j_driver():
    return Neo4jHook(conn_id=neo4j_conn_id).get_conn()


def load_progress() -> Dict[str, Any]:
    if not PROGRESS_PATH.exists():
        return {"completed": {}, "parsed_outputs": {}}
    try:
        with PROGRESS_PATH.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict) and isinstance(data.get("completed"), dict):
            if not isinstance(data.get("parsed_outputs"), dict):
                data["parsed_outputs"] = {}
            return data
    except Exception as e:
        logger.warning("Failed to load progress file %s: %s", PROGRESS_PATH, e)
    return {"completed": {}, "parsed_outputs": {}}


def save_progress(progress: Dict[str, Any]) -> None:
    temp_path = PROGRESS_PATH.with_suffix(".tmp")
    with temp_path.open("w", encoding="utf-8") as fh:
        json.dump(progress, fh, ensure_ascii=False)
    temp_path.replace(PROGRESS_PATH)


def build_openai_request_body(combined_text: str) -> Dict[str, Any]:
    prompt = f"""Analyze the following Youtube video comments and provide:

1. **Frequent Topics**: Extract the most frequently discussed topics/themes. For each topic, provide:
   - The topic name (in English, even if comments are in other languages)
   - A weight (0.0 to 1.0) based on how frequently it appears relative to other topics
   - For each topic also assign a broader category label (1–2 words). Example: topic: AI Regulation, category: Policy, weight: 0.24

2. **Summary**: Write a comprehensive summary description of what the comments are discussing,
   the overall sentiment, key themes, and main points raised by viewers. The summary should be
   in English and be 2-4 sentences long.

The comments may be in different languages - analyze them in their original language but
provide the output in English.

Comments to analyze:
{combined_text}

Return your response as a JSON object with this exact structure:
{{
    "topics": [
        {{"topic": "Topic Name", "category": "Topic Category", "weight": 0.22}},
        {{"topic": "Another Topic", "category": "Another Topic Category", "weight": 0.18}}
    ],
    "summary": "Concise summary here."
}}

Only return the JSON object, no additional text."""

    return {
        "model": resolve_openai_model(),
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are an expert at analyzing social media comments and extracting meaningful "
                    "insights. Always respond with valid JSON only."
                ),
            },
            {"role": "user", "content": prompt},
        ],
    }


def stage_openai_batches(**context):
    logger.info("Cleaning old batch artifacts in %s", TMP_DIR)
    for p in TMP_DIR.glob("yt_comments_batch_*.jsonl"):
        p.unlink(missing_ok=True)
    for p in TMP_DIR.glob("yt_comments_batch_meta_*.json"):
        p.unlink(missing_ok=True)
    for p in TMP_DIR.glob("yt_comments_batch_output_*.jsonl"):
        p.unlink(missing_ok=True)
    for p in TMP_DIR.glob("yt_comments_batch_error_*.jsonl"):
        p.unlink(missing_ok=True)
    PROGRESS_PATH.unlink(missing_ok=True)

    comments_coll, videos_coll, state_coll = get_mongo_collections()
    ensure_analysis_indexes(comments_coll, state_coll)

    base_query = {
        "comment_id": {"$exists": True, "$nin": [None, ""]},
        "video_id": {"$exists": True, "$nin": [None, ""]},
        "orderByParameter": "relevance",
    }
    if not USE_STATE_COLLECTION:
        # Legacy behavior when state collection is disabled.
        base_query["analysis_processed"] = {"$ne": True}
    projection = {"comment_id": 1, "video_id": 1, "textOriginal": 1}

    def _is_throttled(err: Exception) -> bool:
        if isinstance(err, OperationFailure):
            if getattr(err, "code", None) == 16500:
                return True
            msg = str(err)
            return ("RequestRateTooLarge" in msg) or ("TooManyRequests (429)" in msg)
        return False

    def _retry_after_seconds(err: Exception, attempt: int) -> float:
        msg = str(err)
        match = re.search(r"RetryAfterMs=(\d+)", msg)
        if match:
            try:
                ms = int(match.group(1))
                return max(ms / 1000.0, 0.5)
            except Exception:
                pass
        # Fallback exponential backoff with cap
        return float(min(2 ** attempt, 30))

    def _get_candidate_video_ids() -> List[str]:
        # Read from the much smaller videos collection to avoid RU-heavy distinct on comments.
        q = {"video_id": {"$exists": True, "$nin": [None, ""]}}
        p = {"video_id": 1}
        last_error = None
        for attempt in range(1, MONGO_READ_MAX_RETRIES + 1):
            try:
                ids: List[str] = []
                for d in videos_coll.find(q, projection=p):
                    vid = d.get("video_id")
                    if vid is not None:
                        ids.append(str(vid))
                return ids
            except Exception as e:
                last_error = e
                if not _is_throttled(e) or attempt == MONGO_READ_MAX_RETRIES:
                    raise
                sleep_s = _retry_after_seconds(e, attempt)
                logger.warning(
                    "Mongo read throttled while listing videos (attempt %d/%d). Sleeping %.2fs.",
                    attempt,
                    MONGO_READ_MAX_RETRIES,
                    sleep_s,
                )
                time.sleep(sleep_s)
        raise RuntimeError("Unable to list candidate video ids from Mongo.") from last_error

    def _fetch_video_comments(video_id: str) -> tuple[List[str], List[str]]:
        video_query = dict(base_query)
        video_query["video_id"] = video_id
        last_error = None
        for attempt in range(1, MONGO_READ_MAX_RETRIES + 1):
            try:
                pairs: List[tuple[str, str]] = []
                for doc in comments_coll.find(video_query, projection=projection):
                    text = (doc.get("textOriginal") or "").strip()
                    if not text:
                        continue
                    pairs.append((str(doc["comment_id"]), text))

                if USE_STATE_COLLECTION and pairs:
                    processed_ids = set()
                    ids = [cid for cid, _ in pairs]
                    for i in range(0, len(ids), 500):
                        id_chunk = ids[i : i + 500]
                        state_query = {"comment_id": {"$in": id_chunk}, "analysis_processed": True}
                        for sdoc in state_coll.find(state_query, projection={"comment_id": 1}):
                            sid = sdoc.get("comment_id")
                            if sid is not None:
                                processed_ids.add(str(sid))
                    pairs = [(cid, txt) for cid, txt in pairs if cid not in processed_ids]

                comment_ids = [cid for cid, _ in pairs]
                texts = [txt for _, txt in pairs]
                return texts, comment_ids
            except Exception as e:
                last_error = e
                if not _is_throttled(e) or attempt == MONGO_READ_MAX_RETRIES:
                    raise
                sleep_s = _retry_after_seconds(e, attempt)
                logger.warning(
                    "Mongo read throttled for video_id=%s (attempt %d/%d). Sleeping %.2fs.",
                    video_id,
                    attempt,
                    MONGO_READ_MAX_RETRIES,
                    sleep_s,
                )
                time.sleep(sleep_s)
        raise RuntimeError(f"Unable to fetch comments for video_id={video_id}.") from last_error

    video_ids = _get_candidate_video_ids()

    # Stream by video_id to avoid holding all comments for all videos in memory.
    timestamp = int(time.time())
    batch_idx = 0
    file_paths: List[str] = []
    rows: List[str] = []
    meta_rows: List[Dict[str, Any]] = []
    current_batch_bytes = 0
    videos_staged = 0
    comments_staged = 0

    def flush_batch_if_needed():
        nonlocal batch_idx, rows, meta_rows, current_batch_bytes
        if len(rows) < CHUNK_SIZE and current_batch_bytes < MAX_INPUT_FILE_BYTES:
            return
        batch_idx += 1
        input_path = TMP_DIR / f"yt_comments_batch_{batch_idx}_{timestamp}.jsonl"
        meta_path = TMP_DIR / f"yt_comments_batch_meta_{batch_idx}_{timestamp}.json"
        input_path.write_text("\n".join(rows), encoding="utf-8")
        meta_path.write_text(json.dumps(meta_rows, ensure_ascii=False), encoding="utf-8")
        file_paths.append(str(input_path))
        rows = []
        meta_rows = []
        current_batch_bytes = 0

    for video_id in video_ids:
        video_texts, video_comment_ids = _fetch_video_comments(video_id)

        if not video_texts:
            continue

        combined_text = "\n\n".join(video_texts)
        req = {
            "custom_id": video_id,
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": build_openai_request_body(combined_text),
        }
        req_line = json.dumps(req, ensure_ascii=False)
        rows.append(req_line)
        current_batch_bytes += len(req_line.encode("utf-8")) + 1
        meta_rows.append(
            {
                "video_id": video_id,
                "comment_ids": video_comment_ids,
                "comment_count": len(video_texts),
            }
        )
        videos_staged += 1
        comments_staged += len(video_texts)
        flush_batch_if_needed()

    if rows:
        batch_idx += 1
        input_path = TMP_DIR / f"yt_comments_batch_{batch_idx}_{timestamp}.jsonl"
        meta_path = TMP_DIR / f"yt_comments_batch_meta_{batch_idx}_{timestamp}.json"
        input_path.write_text("\n".join(rows), encoding="utf-8")
        meta_path.write_text(json.dumps(meta_rows, ensure_ascii=False), encoding="utf-8")
        file_paths.append(str(input_path))

    logger.info(
        "Staged %d videos (%d comments) into %d input files.",
        videos_staged,
        comments_staged,
        len(file_paths),
    )
    context["ti"].xcom_push(key="openai_batch_files", value=file_paths)
    return file_paths


def submit_and_poll_batches(**context):
    client = get_openai_client()
    file_paths = context["ti"].xcom_pull(key="openai_batch_files", task_ids="stage_openai_batches") or []

    if not file_paths:
        logger.info("No XCom batch files found -- scanning %s for input files.", TMP_DIR)
        input_files = sorted(
            p for p in TMP_DIR.glob("yt_comments_batch_*.jsonl")
            if "output" not in p.stem and "error" not in p.stem
        )
        file_paths = [str(p) for p in input_files]

    if not file_paths:
        logger.info("No files to submit.")
        return []

    progress = load_progress()
    completed_map = progress.get("completed", {})
    submitted_info = []

    def _is_token_limit_error(exc: Exception) -> bool:
        txt = str(exc)
        return ("token_limit_exceeded" in txt) or ("enqueued tokens has surpassed the configured limit" in txt)

    def _is_file_quota_error(exc: Exception) -> bool:
        txt = str(exc)
        return ("quotaExceeded" in txt) and ("files without expiry" in txt)

    def _cleanup_batch_files(keep_latest: int) -> int:
        try:
            listed = client.files.list()
            data = list(getattr(listed, "data", []) or [])
        except Exception as e:
            logger.warning("Failed to list files for quota cleanup: %s", e)
            return 0

        # Keep newest files and delete older batch-purpose files first.
        batch_files = [f for f in data if getattr(f, "purpose", None) == "batch"]
        batch_files.sort(key=lambda f: int(getattr(f, "created_at", 0) or 0), reverse=True)
        to_delete = batch_files[max(keep_latest, 0):]
        deleted = 0
        for f in to_delete:
            fid = getattr(f, "id", None)
            if not fid:
                continue
            try:
                client.files.delete(fid)
                deleted += 1
            except Exception as e:
                logger.warning("Failed deleting old batch file %s: %s", fid, e)
        return deleted

    for fp in file_paths:
        prev = completed_map.get(fp)
        if (
            prev
            and prev.get("output_path")
            and prev.get("metadata_file")
            and Path(prev["output_path"]).exists()
            and Path(prev["metadata_file"]).exists()
        ):
            submitted_info.append(prev)
            continue

        uploaded = None
        for attempt in range(1, 4):
            try:
                with open(fp, "rb") as fh:
                    uploaded = client.files.create(file=fh, purpose="batch")
                break
            except Exception as e:
                if _is_file_quota_error(e):
                    deleted = _cleanup_batch_files(OPENAI_BATCH_FILE_KEEP_COUNT)
                    logger.warning(
                        "OpenAI file quota reached while uploading %s. Deleted %d old batch files "
                        "(keep_latest=%d) and retrying (%d/3).",
                        fp,
                        deleted,
                        OPENAI_BATCH_FILE_KEEP_COUNT,
                        attempt,
                    )
                    if attempt < 3:
                        time.sleep(5)
                        continue
                raise
        if uploaded is None:
            raise RuntimeError(f"Failed to upload file for batch submission: {fp}")

        batch = None
        for attempt in range(1, OPENAI_SUBMIT_MAX_RETRIES + 1):
            try:
                batch = client.batches.create(
                    input_file_id=uploaded.id,
                    endpoint="/v1/chat/completions",
                    completion_window="24h",
                )
                break
            except Exception as e:
                if _is_token_limit_error(e):
                    sleep_s = min(
                        OPENAI_SUBMIT_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
                        OPENAI_SUBMIT_RETRY_MAX_SECONDS,
                    )
                    logger.warning(
                        "OpenAI batch submission hit token queue limit for %s (attempt %d/%d). "
                        "Retrying in %ds. Error: %s",
                        fp,
                        attempt,
                        OPENAI_SUBMIT_MAX_RETRIES,
                        sleep_s,
                        e,
                    )
                    if attempt == OPENAI_SUBMIT_MAX_RETRIES:
                        try:
                            client.files.delete(uploaded.id)
                        except Exception:
                            pass
                        raise RuntimeError(
                            f"OpenAI batch submission exceeded retry budget ({OPENAI_SUBMIT_MAX_RETRIES}) "
                            f"for file {fp} due to token queue limits."
                        ) from e
                    time.sleep(sleep_s)
                    continue
                try:
                    client.files.delete(uploaded.id)
                except Exception:
                    pass
                raise
        if batch is None:
            try:
                client.files.delete(uploaded.id)
            except Exception:
                pass
            raise RuntimeError(f"Failed to submit batch for file {fp}: empty response after retries.")
        batch_id = batch.id
        logger.info("Submitted batch %s for file %s", batch_id, fp)

        last_state = None
        poll_count = 0
        while True:
            status = client.batches.retrieve(batch_id)
            state = getattr(status, "status", None)
            poll_count += 1
            if state != last_state:
                logger.info("Batch %s status -> %s", batch_id, state)
                last_state = state
            elif poll_count % 12 == 0:
                # Heartbeat every ~2 minutes with default POLL_INTERVAL=10s.
                logger.info("Batch %s still %s (poll #%d)", batch_id, state, poll_count)
            if state == "completed":
                break
            if state in ("failed", "cancelled", "expired"):
                raise RuntimeError(f"Batch {batch_id} ended in status {state}")
            time.sleep(POLL_INTERVAL)

        output_file_id = getattr(status, "output_file_id", None)
        error_file_id = getattr(status, "error_file_id", None)

        output_path = None
        if output_file_id:
            output_content = client.files.content(output_file_id).content
            output_path = TMP_DIR / f"yt_comments_batch_output_{batch_id}.jsonl"
            output_path.write_bytes(output_content)

        error_path = None
        if error_file_id:
            error_content = client.files.content(error_file_id).content
            error_path = TMP_DIR / f"yt_comments_batch_error_{batch_id}.jsonl"
            error_path.write_bytes(error_content)

        meta_file = Path(fp).with_name(
            Path(fp).name.replace("yt_comments_batch_", "yt_comments_batch_meta_").replace(".jsonl", ".json")
        )
        record = {
            "input_file": fp,
            "batch_id": batch_id,
            "output_path": str(output_path) if output_path else None,
            "error_path": str(error_path) if error_path else None,
            "metadata_file": str(meta_file) if meta_file.exists() else None,
        }
        submitted_info.append(record)
        completed_map[fp] = record
        progress["completed"] = completed_map
        save_progress(progress)
        try:
            client.files.delete(uploaded.id)
        except Exception as e:
            logger.warning("Failed deleting uploaded batch input file %s: %s", uploaded.id, e)

    context["ti"].xcom_push(key="openai_submitted_batches", value=submitted_info)
    return submitted_info


def parse_and_update_results(**context):
    overall_started_at = time.perf_counter()
    progress = load_progress()
    parsed_outputs = progress.get("parsed_outputs", {})
    submitted_info = context["ti"].xcom_pull(
        key="openai_submitted_batches",
        task_ids="submit_and_poll_batches",
    ) or []

    if not submitted_info:
        logger.warning("No XCom batch info found -- rebuilding from progress file.")
        progress = load_progress()
        completed_map = progress.get("completed", {})

        for _, record in completed_map.items():
            output_path = record.get("output_path")
            meta_path = record.get("metadata_file")
            if (
                output_path
                and meta_path
                and Path(output_path).exists()
                and Path(meta_path).exists()
            ):
                submitted_info.append(record)

    if not submitted_info:
        logger.warning("No output files found anywhere. Nothing to parse.")
        return

    comments_coll, videos_coll, state_coll = get_mongo_collections()
    ensure_analysis_indexes(comments_coll, state_coll)
    driver = get_neo4j_driver()
    model_name = resolve_openai_model()

    total_videos_updated = 0
    total_comments_marked = 0
    partition_full_skip_stats: Dict[str, Dict[str, int]] = {}

    def _is_mongo_write_throttled(err: Exception) -> bool:
        if isinstance(err, OperationFailure):
            if getattr(err, "code", None) == 16500:
                return True
            msg = str(err)
            return ("RequestRateTooLarge" in msg) or ("TooManyRequests (429)" in msg)
        if isinstance(err, BulkWriteError):
            details = getattr(err, "details", {}) or {}
            write_errors = details.get("writeErrors", [])
            if write_errors and all(int(we.get("code", -1)) == 16500 for we in write_errors):
                return True
            msg = str(err)
            return ("RequestRateTooLarge" in msg) or ("TooManyRequests (429)" in msg)
        return False

    def _is_partition_full_message(msg: str) -> bool:
        return ("Substatus: 1014" in msg) or ("Partition key reached maximum size of 20 GB" in msg)

    def _classify_partition_full(err: Exception) -> Dict[str, int]:
        """
        Return counts for partition-full write errors.
        {
          "partition_full": <count>,
          "non_partition_full": <count>,
          "total": <count>
        }
        """
        if isinstance(err, OperationFailure):
            msg = str(err)
            total = 1
            pf = 1 if (_is_partition_full_message(msg) and int(getattr(err, "code", -1)) == 13) else 0
            return {"partition_full": pf, "non_partition_full": total - pf, "total": total}

        if isinstance(err, BulkWriteError):
            details = getattr(err, "details", {}) or {}
            write_errors = details.get("writeErrors", []) or []
            if not write_errors:
                return {"partition_full": 0, "non_partition_full": 1, "total": 1}
            pf = 0
            non_pf = 0
            for we in write_errors:
                code = int(we.get("code", -1))
                msg = str(we.get("errmsg", ""))
                if code == 13 and _is_partition_full_message(msg):
                    pf += 1
                else:
                    non_pf += 1
            return {"partition_full": pf, "non_partition_full": non_pf, "total": len(write_errors)}

        return {"partition_full": 0, "non_partition_full": 1, "total": 1}

    def _retry_after_seconds(err: Exception, attempt: int) -> float:
        msg = str(err)
        match = re.search(r"RetryAfterMs=(\d+)", msg)
        if match:
            try:
                ms = int(match.group(1))
                return max(ms / 1000.0, MONGO_THROTTLE_MIN_SLEEP_SECONDS)
            except Exception:
                pass
        return float(min(max(2 ** attempt, MONGO_THROTTLE_MIN_SLEEP_SECONDS), 30))

    def _record_partition_full_skip(label: str, ops_count: int, partition_full_errors: int) -> None:
        stats = partition_full_skip_stats.setdefault(label, {"chunks": 0, "ops": 0, "errors": 0})
        stats["chunks"] += 1
        stats["ops"] += ops_count
        stats["errors"] += max(partition_full_errors, 1)

        skipped_chunks = stats["chunks"]
        # Keep early visibility, then summarize periodically to avoid log flooding.
        if skipped_chunks <= 3 or skipped_chunks % 25 == 0:
            logger.error(
                "Mongo bulk write hit Cosmos partition-full limit for %s; skipping this chunk "
                "(ops=%d, partition_full_errors=%d, skipped_chunks_for_label=%d, skipped_ops_for_label=%d).",
                label,
                ops_count,
                max(partition_full_errors, 1),
                skipped_chunks,
                stats["ops"],
            )
        elif skipped_chunks == 4:
            logger.error(
                "Additional partition-full skips for %s are being suppressed; "
                "a summary will be logged every 25 chunks and at task end.",
                label,
            )

    def _bulk_write_with_retry(coll, ops: List[UpdateOne], label: str) -> Dict[str, int]:
        if not ops:
            return {"attempted": 0, "matched": 0, "modified": 0, "upserted": 0, "skipped": 0}
        last_error = None
        for attempt in range(1, MONGO_WRITE_MAX_RETRIES + 1):
            try:
                res = coll.bulk_write(ops, ordered=False)
                return {
                    "attempted": len(ops),
                    "matched": int(getattr(res, "matched_count", 0) or 0),
                    "modified": int(getattr(res, "modified_count", 0) or 0),
                    "upserted": int(getattr(res, "upserted_count", 0) or 0),
                    "skipped": 0,
                }
            except Exception as e:
                last_error = e
                # Cosmos hard limit: logical partition reached 20 GB (Substatus 1014).
                # This is non-retriable; skip this chunk so the task can continue.
                if _is_partition_full_message(str(e)):
                    partition_info = _classify_partition_full(e)
                    _record_partition_full_skip(label, len(ops), partition_info.get("partition_full", 0))
                    return {
                        "attempted": len(ops),
                        "matched": 0,
                        "modified": 0,
                        "upserted": 0,
                        "skipped": len(ops),
                    }
                partition_info = _classify_partition_full(e)
                if partition_info["partition_full"] > 0 and partition_info["non_partition_full"] == 0:
                    _record_partition_full_skip(label, len(ops), partition_info["partition_full"])
                    return {
                        "attempted": len(ops),
                        "matched": 0,
                        "modified": 0,
                        "upserted": 0,
                        "skipped": len(ops),
                    }
                is_throttled = _is_mongo_write_throttled(e)
                if is_throttled and attempt == MONGO_WRITE_MAX_RETRIES and label.startswith(f"{STATE_COLLECTION_NAME}."):
                    logger.error(
                        "Mongo bulk write remained throttled for %s after %d attempts; "
                        "skipping this state chunk (ops=%d) so task can continue.",
                        label,
                        MONGO_WRITE_MAX_RETRIES,
                        len(ops),
                    )
                    return {
                        "attempted": len(ops),
                        "matched": 0,
                        "modified": 0,
                        "upserted": 0,
                        "skipped": len(ops),
                    }
                if not is_throttled or attempt == MONGO_WRITE_MAX_RETRIES:
                    raise
                sleep_s = _retry_after_seconds(e, attempt)
                logger.warning(
                    "Mongo bulk write throttled for %s (attempt %d/%d). Sleeping %.2fs.",
                    label,
                    attempt,
                    MONGO_WRITE_MAX_RETRIES,
                    sleep_s,
                )
                time.sleep(sleep_s)
        raise RuntimeError(f"Mongo bulk write failed for {label}") from last_error

    logger.info("Starting parse_and_update_results for %d submitted batch records.", len(submitted_info))

    for batch_idx, batch in enumerate(submitted_info, start=1):
        batch_started_at = time.perf_counter()
        parse_key = str(batch.get("batch_id") or batch.get("output_path") or "")
        if parse_key and parsed_outputs.get(parse_key, {}).get("status") == "completed":
            logger.info(
                "Skipping already parsed batch %d/%d (key=%s).",
                batch_idx,
                len(submitted_info),
                parse_key,
            )
            continue
        output_path = batch.get("output_path")
        meta_path = batch.get("metadata_file")
        if not output_path or not Path(output_path).exists():
            continue
        if not meta_path or not Path(meta_path).exists():
            logger.warning("Skipping output without metadata: %s", output_path)
            continue

        meta_list = json.loads(Path(meta_path).read_text(encoding="utf-8"))
        meta_by_video = {m["video_id"]: m for m in meta_list if isinstance(m, dict) and m.get("video_id")}

        mongo_ops = []
        state_ops = []
        neo4j_rows = []
        parse_started_at = time.perf_counter()

        for line in Path(output_path).read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue

            video_id = rec.get("custom_id")
            if not video_id or video_id not in meta_by_video:
                continue
            response = rec.get("response", {})
            if response.get("status_code") != 200:
                continue
            body = response.get("body", {})
            choices = body.get("choices") or []
            if not choices:
                continue
            content = ((choices[0].get("message") or {}).get("content") or "").strip()
            if not content:
                continue

            # Strip code fences if present
            txt = content
            if txt.startswith("```"):
                parts = txt.split("```")
                if len(parts) >= 2:
                    txt = parts[1].strip()

            try:
                analysis = json.loads(txt)
            except json.JSONDecodeError:
                start = txt.find("{")
                end = txt.rfind("}")
                if start != -1 and end != -1:
                    try:
                        analysis = json.loads(txt[start:end + 1])
                    except json.JSONDecodeError:
                        logger.warning("Could not parse AI output for video %s", video_id)
                        continue
                else:
                    logger.warning("Could not parse AI output for video %s", video_id)
                    continue

            topics_raw = analysis.get("topics") or []
            topics = []
            topic_names = []
            topic_categories = []
            topic_weights = []
            if isinstance(topics_raw, list):
                for t in topics_raw:
                    if not isinstance(t, dict):
                        continue
                    name = str(t.get("topic", "")).strip()
                    if not name:
                        continue
                    cat = str(t.get("category", "")).strip()
                    try:
                        weight = round(float(t.get("weight", 0.0)), 3)
                    except Exception:
                        weight = 0.0
                    topics.append({"topic": name, "category": cat, "weight": weight})
                    topic_names.append(name)
                    topic_categories.append(cat)
                    topic_weights.append(weight)

            summary = str(analysis.get("summary", "")).strip()
            topics_json = json.dumps(topics, ensure_ascii=False)
            now = datetime.utcnow()

            comment_ids = meta_by_video[video_id].get("comment_ids", [])
            for cid in comment_ids:
                state_ops.append(
                    UpdateOne(
                        {
                            "comment_id": cid,
                            "analysis_processed": {"$ne": True},
                        },
                        {
                            "$set": {
                                "comment_id": cid,
                                "video_id": video_id,
                                "analysis_processed": True,
                                "analysis_processed_at": now,
                                "source": "youtube_comments_analysis_dag",
                            }
                        },
                        upsert=True,
                    )
                )
                if WRITE_LEGACY_COMMENTS_PROCESSED:
                    mongo_ops.append(
                        UpdateOne(
                            {
                                "comment_id": cid,
                                "analysis_processed": {"$ne": True},
                            },
                            {"$set": {"analysis_processed": True, "analysis_processed_at": now}},
                        )
                    )
            total_comments_marked += len(comment_ids)

            neo4j_rows.append({
                "video_id": video_id,
                "topic_names": topic_names,
                "topic_categories": topic_categories,
                "topic_weights": topic_weights,
                "topics_json": topics_json,
                "summary": summary,
                "model": model_name,
            })
            total_videos_updated += 1
        parse_elapsed = time.perf_counter() - parse_started_at
        logger.info(
            "Batch %d/%d parsed in %.2fs (videos=%d, state_ops=%d, comment_ops=%d).",
            batch_idx,
            len(submitted_info),
            parse_elapsed,
            len(neo4j_rows),
            len(state_ops),
            len(mongo_ops),
        )

        # Mongo state collection: authoritative processed status (used by staging).
        state_write_started_at = time.perf_counter()
        for i in range(0, len(state_ops), MONGO_BATCH_WRITE_CHUNK):
            chunk = state_ops[i : i + MONGO_BATCH_WRITE_CHUNK]
            if chunk:
                _bulk_write_with_retry(state_coll, chunk, f"{STATE_COLLECTION_NAME}.analysis_processed")
        logger.info(
            "Batch %d/%d state writes completed in %.2fs (ops=%d, chunk_size=%d).",
            batch_idx,
            len(submitted_info),
            time.perf_counter() - state_write_started_at,
            len(state_ops),
            MONGO_BATCH_WRITE_CHUNK,
        )

        # Mongo: mark comments as processed
        comments_write_started_at = time.perf_counter()
        if WRITE_LEGACY_COMMENTS_PROCESSED:
            for i in range(0, len(mongo_ops), MONGO_BATCH_WRITE_CHUNK):
                chunk = mongo_ops[i : i + MONGO_BATCH_WRITE_CHUNK]
                if chunk:
                    _bulk_write_with_retry(comments_coll, chunk, "youtube_video_comments.analysis_processed")
            logger.info(
                "Batch %d/%d comment writes completed in %.2fs (ops=%d, chunk_size=%d).",
                batch_idx,
                len(submitted_info),
                time.perf_counter() - comments_write_started_at,
                len(mongo_ops),
                MONGO_BATCH_WRITE_CHUNK,
            )
        else:
            logger.info(
                "Batch %d/%d legacy comment writes skipped (YT_COMMENT_WRITE_LEGACY_COMMENTS_PROCESSED=false).",
                batch_idx,
                len(submitted_info),
            )

        # Neo4j: update YouTubeVideo nodes with comment summary only
        neo4j_video_query = """
        UNWIND $rows AS r
        MATCH (v:YouTubeVideo {video_id: r.video_id})
        WHERE v.comments_analyzed_at IS NULL
        SET
          v.comment_summary_description = r.summary,
          v.comments_analyzed_at = datetime()
        RETURN count(v) AS matched
        """
        neo4j_write_started_at = time.perf_counter()
        neo4j_sent = 0
        neo4j_matched = 0
        with driver.session() as session:
            for i in range(0, len(neo4j_rows), NEO4J_CHUNK):
                chunk = neo4j_rows[i : i + NEO4J_CHUNK]
                if not chunk:
                    continue
                neo4j_sent += len(chunk)
                last_error = None
                for attempt in range(1, MAX_WRITE_RETRIES + 1):
                    try:
                        if hasattr(session, "execute_write"):
                            matched = session.execute_write(
                                lambda tx: int((tx.run(neo4j_video_query, rows=chunk).single() or {}).get("matched", 0))
                            )
                        else:
                            matched = session.write_transaction(
                                lambda tx: int((tx.run(neo4j_video_query, rows=chunk).single() or {}).get("matched", 0))
                            )
                        neo4j_matched += int(matched or 0)
                        last_error = None
                        break
                    except Exception as e:
                        last_error = e
                        logger.warning("Neo4j write failed (attempt %d/%d): %s", attempt, MAX_WRITE_RETRIES, e)
                        if attempt < MAX_WRITE_RETRIES:
                            time.sleep(min(2 ** attempt, 8))
                if last_error:
                    raise RuntimeError("Neo4j write failed after retries") from last_error
        logger.info(
            "Batch %d/%d Neo4j writes completed in %.2fs (sent=%d, matched=%d, missing=%d, chunk_size=%d).",
            batch_idx,
            len(submitted_info),
            time.perf_counter() - neo4j_write_started_at,
            neo4j_sent,
            neo4j_matched,
            max(neo4j_sent - neo4j_matched, 0),
            NEO4J_CHUNK,
        )
        logger.info("Updated %d videos in Neo4j (YouTubeVideo).", neo4j_matched)

        # Mongo: update video documents with analysis results
        videos_write_started_at = time.perf_counter()
        if neo4j_rows:
            now = datetime.utcnow()
            video_ops = []
            video_write_stats = {"attempted": 0, "matched": 0, "modified": 0, "upserted": 0, "skipped": 0}
            for row in neo4j_rows:
                video_ops.append(
                    UpdateOne(
                        {
                            "video_id": row["video_id"],
                            "$or": [
                                {"comments_analyzed_at": {"$exists": False}},
                                {"comments_analyzed_at": None},
                            ],
                        },
                        {
                            "$set": {
                                "comments_frequent_topics": row["topic_names"],
                                "comments_frequent_topic_categories": row["topic_categories"],
                                "comments_frequent_topic_weights": row["topic_weights"],
                                "comments_frequent_topics_json": row["topics_json"],
                                "comment_summary_description": row["summary"],
                                "comments_analysis_model": row["model"],
                                "comments_analyzed_at": now,
                            }
                        },
                    )
                )
            for i in range(0, len(video_ops), MONGO_BATCH_WRITE_CHUNK):
                chunk = video_ops[i : i + MONGO_BATCH_WRITE_CHUNK]
                if chunk:
                    stats = _bulk_write_with_retry(videos_coll, chunk, "youtube_channel_videos.analysis_fields")
                    for key in video_write_stats:
                        video_write_stats[key] += stats.get(key, 0)
            logger.info(
                "youtube_channel_videos writes: sent=%d matched=%d modified=%d upserted=%d skipped=%d.",
                video_write_stats["attempted"],
                video_write_stats["matched"],
                video_write_stats["modified"],
                video_write_stats["upserted"],
                video_write_stats["skipped"],
            )
        logger.info(
            "Batch %d/%d video-doc writes completed in %.2fs.",
            batch_idx,
            len(submitted_info),
            time.perf_counter() - videos_write_started_at,
        )
        logger.info(
            "Batch %d/%d total processing time: %.2fs.",
            batch_idx,
            len(submitted_info),
            time.perf_counter() - batch_started_at,
        )
        if parse_key:
            parsed_outputs[parse_key] = {
                "status": "completed",
                "batch_id": batch.get("batch_id"),
                "output_path": output_path,
                "metadata_file": meta_path,
                "completed_at": datetime.utcnow().isoformat(),
            }
            progress["parsed_outputs"] = parsed_outputs
            save_progress(progress)

    driver.close()
    if partition_full_skip_stats:
        for label, stats in partition_full_skip_stats.items():
            logger.error(
                "Cosmos partition-full summary for %s: skipped_chunks=%d, skipped_ops=%d, "
                "detected_partition_full_errors=%d. Data in those full partitions was not updated.",
                label,
                stats["chunks"],
                stats["ops"],
                stats["errors"],
            )
    logger.info(
        "parse_and_update_results total elapsed time: %.2fs across %d batch records.",
        time.perf_counter() - overall_started_at,
        len(submitted_info),
    )
    logger.info("Updated %d videos, marked %d comments as processed.", total_videos_updated, total_comments_marked)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_callback,
    "on_success_callback": task_success_callback,
}


with DAG(
    dag_id="youtube_comments_analysis_dag",
    default_args=default_args,
    description="Batch AI analysis for top-level YouTube comments (Mongo + Neo4j)",
    schedule=None,
    start_date=datetime(2025, 1, 21),
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "comments", "analysis", "batch"],
) as dag:
    t1 = PythonOperator(
        task_id="stage_openai_batches",
        python_callable=stage_openai_batches,
    )
    t2 = PythonOperator(
        task_id="submit_and_poll_batches",
        python_callable=submit_and_poll_batches,
    )
    t3 = PythonOperator(
        task_id="parse_and_update_results",
        python_callable=parse_and_update_results,
    )

    t1 >> t2 >> t3
    #t2 >> t3
    #t3
  
