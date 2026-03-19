"""
Batch AI analysis for TikTok comments.

This DAG:
- Reads TikTok comments from MongoDB (`tiktok_video_comments`) by video_id
- Builds OpenAI Batch input files (Azure OpenAI compatible)
- Submits and polls batch jobs
- Parses outputs and updates:
  - MongoDB `tiktok_user_video` with full analysis fields
  - Neo4j `TikTokVideo` with summary only
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional

import pendulum  # pyright: ignore[reportMissingImports]
from airflow import DAG  # pyright: ignore[reportMissingImports]
from airflow.providers.standard.operators.python import PythonOperator  # pyright: ignore[reportMissingImports]
from airflow.providers.mongo.hooks.mongo import MongoHook  # pyright: ignore[reportMissingImports]
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook  # pyright: ignore[reportMissingImports]
from callbacks import task_failure_callback, task_success_callback
from openai import AzureOpenAI  # pyright: ignore[reportMissingImports]
from openai import OpenAI  # pyright: ignore[reportMissingImports]
from pymongo.operations import UpdateOne  # pyright: ignore[reportMissingImports]
from pymongo.errors import BulkWriteError, OperationFailure  # pyright: ignore[reportMissingImports]

logger = logging.getLogger("airflow.task")
local_tz = pendulum.timezone("Europe/Amsterdam")

airflow_env = os.getenv("AIRFLOW_ENV", "development")
mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
mongo_db_name = "rbl" if airflow_env == "production" else "airflow_db"
neo4j_conn_id = "neo4j_prod" if airflow_env == "production" else "neo4j_default"

CHUNK_SIZE = int(os.getenv("TTK_COMMENT_ANALYSIS_CHUNK_SIZE", "100"))
MONGO_BATCH_WRITE_CHUNK = int(os.getenv("TTK_COMMENT_MONGO_WRITE_CHUNK", "100"))
NEO4J_CHUNK = int(os.getenv("TTK_COMMENT_NEO4J_CHUNK", "100"))
POLL_INTERVAL = int(os.getenv("OPENAI_POLL_INTERVAL", "10"))
MAX_COMMENTS_PER_VIDEO = int(os.getenv("TTK_COMMENT_MAX_COMMENTS_PER_VIDEO", "1000"))
MAX_INPUT_FILE_BYTES = int(os.getenv("TTK_COMMENT_BATCH_MAX_BYTES", str(15 * 1024 * 1024)))
OPENAI_SUBMIT_MAX_RETRIES = int(os.getenv("TTK_COMMENT_OPENAI_SUBMIT_MAX_RETRIES", "60"))
OPENAI_SUBMIT_RETRY_BASE_SECONDS = int(os.getenv("TTK_COMMENT_OPENAI_SUBMIT_RETRY_BASE_SECONDS", "15"))
OPENAI_SUBMIT_RETRY_MAX_SECONDS = int(os.getenv("TTK_COMMENT_OPENAI_SUBMIT_RETRY_MAX_SECONDS", "300"))
OPENAI_BATCH_FILE_KEEP_COUNT = int(os.getenv("TTK_COMMENT_OPENAI_BATCH_FILE_KEEP_COUNT", "200"))
MONGO_READ_MAX_RETRIES = int(os.getenv("TTK_COMMENT_MONGO_READ_MAX_RETRIES", "8"))
MONGO_WRITE_MAX_RETRIES = int(os.getenv("TTK_COMMENT_MONGO_WRITE_MAX_RETRIES", "8"))
MONGO_THROTTLE_MIN_SLEEP_SECONDS = float(os.getenv("TTK_COMMENT_MONGO_THROTTLE_MIN_SLEEP_SECONDS", "1.0"))

TMP_DIR = Path(os.getenv("OPENAI_BATCH_TMP", "/opt/airflow/dags/tmp_openai_batches")) / "tiktok_comments"
TMP_DIR.mkdir(parents=True, exist_ok=True)
PROGRESS_PATH = TMP_DIR / "tiktok_comments_analysis_progress.json"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": task_failure_callback,
    "on_success_callback": task_success_callback,
}


def resolve_openai_model() -> str:
    return os.getenv("AZURE_OPENAI_MODEL") or os.getenv("OPENAI_MODEL", "gpt-4o-mini")


def get_openai_client():
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    azure_api_key = os.getenv("AZURE_OPENAI_API_KEY")
    azure_api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-10-21")
    if azure_endpoint and azure_api_key:
        logger.info("Using Azure OpenAI client with deployment: %s", resolve_openai_model())
        return AzureOpenAI(api_key=azure_api_key, api_version=azure_api_version, azure_endpoint=azure_endpoint)

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("Missing OPENAI_API_KEY and Azure OpenAI is not configured.")
    logger.info("Using regular OpenAI client.")
    return OpenAI(api_key=api_key)


def get_neo4j_driver():
    return Neo4jHook(conn_id=neo4j_conn_id).get_conn()


def get_mongo_collections():
    mongo = MongoHook(mongo_conn_id=mongo_conn_id).get_conn()
    db = mongo[mongo_db_name]
    return db.tiktok_video_comments, db.tiktok_user_video


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
    tmp = PROGRESS_PATH.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(progress, fh, ensure_ascii=False)
    tmp.replace(PROGRESS_PATH)


def _is_mongo_throttled(err: Exception) -> bool:
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


def _parse_video_ids_param(video_ids_param: Any) -> Optional[List[int|str]]:
    if not video_ids_param:
        return None
    if isinstance(video_ids_param, str):
        out = []
        for x in video_ids_param.split(","):
            x = x.strip()
            if not x:
                continue
            out.append(int(x) if x.isdigit() else x)
        return out
    if isinstance(video_ids_param, list):
        out = []
        for x in video_ids_param:
            sx = str(x).strip()
            if not sx:
                continue
            out.append(int(sx) if sx.isdigit() else sx)
        return out
    return None


def get_videos_with_comments(
    videos_coll,
    limit: Optional[int],
    offset: int,
    video_ids: Optional[List[int|str]],
    skip_analyzed: bool,
) -> List[int|str]:
    if video_ids:
        return video_ids

    query: Dict[str, Any] = {"video_id": {"$exists": True, "$nin": [None, ""]}}
    if skip_analyzed:
        query["$or"] = [
            {"comments_analyzed_at": {"$exists": False}},
            {"comments_analyzed_at": None},
            {"comment_summary_description": {"$exists": False}},
            {"comment_summary_description": None},
            {"comment_summary_description": ""},
        ]

    last_error = None
    for attempt in range(1, MONGO_READ_MAX_RETRIES + 1):
        try:
            cursor = videos_coll.find(query, {"video_id": 1, "_id": 0}).sort("video_id", 1)
            if offset > 0:
                cursor = cursor.skip(offset)
            if limit:
                cursor = cursor.limit(int(limit))
            return [r.get("video_id") for r in cursor if r.get("video_id") is not None]
        except Exception as e:
            last_error = e
            if not _is_mongo_throttled(e) or attempt == MONGO_READ_MAX_RETRIES:
                raise
            sleep_s = _retry_after_seconds(e, attempt)
            logger.warning(
                "Mongo read throttled while listing TikTok candidate videos (attempt %d/%d). Sleeping %.2fs.",
                attempt,
                MONGO_READ_MAX_RETRIES,
                sleep_s,
            )
            time.sleep(sleep_s)
    raise RuntimeError("Unable to list TikTok candidate video ids from Mongo.") from last_error


def get_comments_for_video(comments_coll, video_id: str) -> List[str]:
    query = {"video_id": video_id, "text": {"$exists": True, "$ne": ""}}
    last_error = None
    for attempt in range(1, MONGO_READ_MAX_RETRIES + 1):
        try:
            comments: List[str] = []
            cursor = comments_coll.find(query, {"text": 1, "_id": 0}).limit(MAX_COMMENTS_PER_VIDEO)
            for r in cursor:
                txt = str(r.get("text") or "").strip()
                if txt:
                    comments.append(txt)
            return comments
        except Exception as e:
            last_error = e
            if not _is_mongo_throttled(e) or attempt == MONGO_READ_MAX_RETRIES:
                raise
            sleep_s = _retry_after_seconds(e, attempt)
            logger.warning(
                "Mongo read throttled for TikTok comments video_id=%s (attempt %d/%d). Sleeping %.2fs.",
                video_id,
                attempt,
                MONGO_READ_MAX_RETRIES,
                sleep_s,
            )
            time.sleep(sleep_s)
    raise RuntimeError(f"Unable to fetch TikTok comments for video_id={video_id}.") from last_error


def build_openai_request_body(combined_text: str) -> Dict[str, Any]:
    prompt = f"""Analyze the following TikTok video comments and provide:

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
    logger.info("Cleaning old TikTok comments batch artifacts in %s", TMP_DIR)
    for p in TMP_DIR.glob("ttk_comments_batch_*.jsonl"):
        p.unlink(missing_ok=True)
    for p in TMP_DIR.glob("ttk_comments_batch_meta_*.json"):
        p.unlink(missing_ok=True)
    for p in TMP_DIR.glob("ttk_comments_batch_output_*.jsonl"):
        p.unlink(missing_ok=True)
    for p in TMP_DIR.glob("ttk_comments_batch_error_*.jsonl"):
        p.unlink(missing_ok=True)
    PROGRESS_PATH.unlink(missing_ok=True)

    params = context.get("params", {})
    limit = params.get("limit")
    offset = int(params.get("offset", 0) or 0)
    skip_analyzed = bool(params.get("skip_analyzed", True))
    video_ids = _parse_video_ids_param(params.get("video_ids"))

    comments_coll, videos_coll = get_mongo_collections()
    all_video_ids = get_videos_with_comments(videos_coll, limit=limit, offset=offset, video_ids=video_ids, skip_analyzed=skip_analyzed)
    logger.info("Found %d TikTok videos to stage.", len(all_video_ids))
    rows: List[str] = []
    meta_rows: List[Dict[str, Any]] = []
    current_batch_bytes = 0
    file_paths: List[str] = []
    timestamp = int(time.time())
    batch_idx = 0
    staged_videos = 0

    def flush():
        nonlocal batch_idx, rows, meta_rows, current_batch_bytes
        if len(rows) < CHUNK_SIZE and current_batch_bytes < MAX_INPUT_FILE_BYTES:
            return
        batch_idx += 1
        input_path = TMP_DIR / f"ttk_comments_batch_{batch_idx}_{timestamp}.jsonl"
        meta_path = TMP_DIR / f"ttk_comments_batch_meta_{batch_idx}_{timestamp}.json"
        input_path.write_text("\n".join(rows), encoding="utf-8")
        meta_path.write_text(json.dumps(meta_rows, ensure_ascii=False), encoding="utf-8")
        file_paths.append(str(input_path))
        rows = []
        meta_rows = []
        current_batch_bytes = 0

    for video_id in all_video_ids:
        texts = get_comments_for_video(comments_coll, video_id)
        if not texts:
            continue
        req = {
            "custom_id": str(video_id),
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": build_openai_request_body("\n\n".join(texts)),
        }
        line = json.dumps(req, ensure_ascii=False)
        rows.append(line)
        meta_rows.append({"custom_id": str(video_id), "video_id": video_id, "comment_count": len(texts)})
        current_batch_bytes += len(line.encode("utf-8")) + 1
        staged_videos += 1
        flush()

    if rows:
        batch_idx += 1
        input_path = TMP_DIR / f"ttk_comments_batch_{batch_idx}_{timestamp}.jsonl"
        meta_path = TMP_DIR / f"ttk_comments_batch_meta_{batch_idx}_{timestamp}.json"
        input_path.write_text("\n".join(rows), encoding="utf-8")
        meta_path.write_text(json.dumps(meta_rows, ensure_ascii=False), encoding="utf-8")
        file_paths.append(str(input_path))

    logger.info("Staged %d videos into %d input files.", staged_videos, len(file_paths))
    context["ti"].xcom_push(key="openai_batch_files", value=file_paths)
    return file_paths


def submit_and_poll_batches(**context):
    client = get_openai_client()
    file_paths = context["ti"].xcom_pull(key="openai_batch_files", task_ids="stage_openai_batches") or []
    if not file_paths:
        file_paths = [str(p) for p in sorted(TMP_DIR.glob("ttk_comments_batch_*.jsonl")) if "output" not in p.stem and "error" not in p.stem]
    if not file_paths:
        logger.info("No files to submit.")
        return []

    progress = load_progress()
    completed_map = progress.get("completed", {})
    submitted_info: List[Dict[str, str]] = []

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

        batch_files = [f for f in data if getattr(f, "purpose", None) == "batch"]
        batch_files.sort(key=lambda f: int(getattr(f, "created_at", 0) or 0), reverse=True)
        to_delete = batch_files[max(keep_latest, 0) :]
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

    for input_file in file_paths:
        done = completed_map.get(input_file)
        if done and done.get("status") == "completed" and done.get("output_file") and Path(done["output_file"]).exists():
            submitted_info.append(done)
            logger.info("Skipping already completed input %s.", input_file)
            continue

        uploaded = None
        for attempt in range(1, 4):
            try:
                with open(input_file, "rb") as fh:
                    uploaded = client.files.create(file=fh, purpose="batch")
                break
            except Exception as e:
                if _is_file_quota_error(e):
                    deleted = _cleanup_batch_files(OPENAI_BATCH_FILE_KEEP_COUNT)
                    logger.warning(
                        "OpenAI file quota reached while uploading %s. Deleted %d old batch files "
                        "(keep_latest=%d) and retrying (%d/3).",
                        input_file,
                        deleted,
                        OPENAI_BATCH_FILE_KEEP_COUNT,
                        attempt,
                    )
                    if attempt < 3:
                        time.sleep(5)
                        continue
                raise
        if uploaded is None:
            raise RuntimeError(f"Failed to upload batch input file: {input_file}")
        file_id = uploaded.id

        batch = None
        for attempt in range(1, OPENAI_SUBMIT_MAX_RETRIES + 1):
            try:
                batch = client.batches.create(
                    input_file_id=file_id,
                    endpoint="/v1/chat/completions",
                    completion_window="24h",
                )
                break
            except Exception:
                if attempt >= OPENAI_SUBMIT_MAX_RETRIES:
                    try:
                        client.files.delete(file_id)
                    except Exception:
                        pass
                    raise
                sleep_s = min(OPENAI_SUBMIT_RETRY_BASE_SECONDS * (2 ** (attempt - 1)), OPENAI_SUBMIT_RETRY_MAX_SECONDS)
                time.sleep(sleep_s)
        if batch is None:
            try:
                client.files.delete(file_id)
            except Exception:
                pass
            raise RuntimeError(f"Failed creating batch for {input_file}")

        while True:
            b = client.batches.retrieve(batch.id)
            state = b.status
            if state in ("completed", "failed", "cancelled", "expired"):
                if state != "completed":
                    try:
                        client.files.delete(file_id)
                    except Exception:
                        pass
                    raise RuntimeError(f"Batch {batch.id} ended in {state}")
                break
            time.sleep(POLL_INTERVAL)

        output_file_id = b.output_file_id
        if not output_file_id:
            raise RuntimeError(f"Batch {batch.id} completed without output_file_id")
        output_path = TMP_DIR / f"ttk_comments_batch_output_{batch.id}.jsonl"
        output_resp = client.files.content(output_file_id)
        output_text = output_resp.text if hasattr(output_resp, "text") else output_resp.content.decode("utf-8")
        output_path.write_text(output_text, encoding="utf-8")

        if getattr(b, "error_file_id", None):
            error_resp = client.files.content(b.error_file_id)
            error_text = error_resp.text if hasattr(error_resp, "text") else error_resp.content.decode("utf-8")
            error_path = TMP_DIR / f"ttk_comments_batch_error_{batch.id}.jsonl"
            error_path.write_text(error_text, encoding="utf-8")

        idx_part = Path(input_file).name.replace("ttk_comments_batch_", "").replace(".jsonl", "")
        meta_path = TMP_DIR / f"ttk_comments_batch_meta_{idx_part}.json"

        rec = {
            "input_file": input_file,
            "batch_id": batch.id,
            "output_file": str(output_path),
            "meta_file": str(meta_path),
            "status": "completed",
        }
        submitted_info.append(rec)
        completed_map[input_file] = rec
        progress["completed"] = completed_map
        save_progress(progress)
        try:
            client.files.delete(file_id)
        except Exception as e:
            logger.warning("Failed deleting uploaded batch input file %s: %s", file_id, e)

    context["ti"].xcom_push(key="submitted_batches", value=submitted_info)
    return submitted_info


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


def _bulk_update_neo4j(driver, rows: List[Dict[str, Any]], chunk_size: int) -> Dict[str, int]:
    if not rows:
        return {"sent": 0, "matched": 0, "missing": 0}

    cypher = """
    UNWIND $rows AS r
    MATCH (v:TikTokVideo {video_id: r.video_id})
    WHERE v.comments_analyzed_at IS NULL OR v.comment_summary_description IS NULL OR v.comment_summary_description = ''
    SET v.comment_summary_description = r.summary,
        v.comments_analyzed_at = datetime()
    RETURN count(v) AS matched
    """

    sent = 0
    matched = 0
    with driver.session() as session:
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            rec = session.run(cypher, rows=chunk).single()
            sent += len(chunk)
            matched += int((rec or {}).get("matched", 0))
    return {"sent": sent, "matched": matched, "missing": max(sent - matched, 0)}


def _bulk_update_mongo(videos_coll, ops: List[UpdateOne], chunk_size: int) -> Dict[str, int]:
    if not ops:
        return {"sent": 0, "matched": 0, "modified": 0, "upserted": 0}
    sent = matched = modified = upserted = 0
    for i in range(0, len(ops), chunk_size):
        chunk = ops[i : i + chunk_size]
        last_error = None
        for attempt in range(1, MONGO_WRITE_MAX_RETRIES + 1):
            try:
                res = videos_coll.bulk_write(chunk, ordered=False)
                sent += len(chunk)
                matched += int(getattr(res, "matched_count", 0) or 0)
                modified += int(getattr(res, "modified_count", 0) or 0)
                upserted += int(getattr(res, "upserted_count", 0) or 0)
                break
            except Exception as e:
                last_error = e
                if not _is_mongo_throttled(e) or attempt == MONGO_WRITE_MAX_RETRIES:
                    raise
                sleep_s = _retry_after_seconds(e, attempt)
                logger.warning(
                    "Mongo bulk write throttled for tiktok_user_video (attempt %d/%d). Sleeping %.2fs.",
                    attempt,
                    MONGO_WRITE_MAX_RETRIES,
                    sleep_s,
                )
                time.sleep(sleep_s)
        if last_error is not None and not _is_mongo_throttled(last_error):
            raise last_error
    return {"sent": sent, "matched": matched, "modified": modified, "upserted": upserted}


def parse_and_update_results(**context):
    submitted = context["ti"].xcom_pull(key="submitted_batches", task_ids="submit_and_poll_batches") or []
    if not submitted:
        progress = load_progress()
        submitted = list((progress.get("completed") or {}).values())
    if not submitted:
        logger.info("No submitted batches to parse.")
        return

    progress = load_progress()
    parsed_outputs = progress.get("parsed_outputs", {})
    _, videos_coll = get_mongo_collections()
    driver = get_neo4j_driver()
    try:
        updated = 0
        skipped = 0
        for i, rec in enumerate(submitted, start=1):
            batch_started_at = time.perf_counter()
            output_file = rec.get("output_file")
            if not output_file or not Path(output_file).exists():
                continue
            if parsed_outputs.get(output_file, {}).get("status") == "completed":
                logger.info("Skipping already parsed output %s", output_file)
                skipped += 1
                continue

            neo4j_rows: List[Dict[str, Any]] = []
            mongo_ops: List[UpdateOne] = []
            parse_started_at = time.perf_counter()
            meta_file = rec.get("meta_file")
            meta_by_custom_id: Dict[str, Any] = {}
            if meta_file and Path(meta_file).exists():
                try:
                    meta_list = json.loads(Path(meta_file).read_text(encoding="utf-8"))
                    if isinstance(meta_list, list):
                        for m in meta_list:
                            if isinstance(m, dict) and m.get("custom_id") is not None:
                                meta_by_custom_id[str(m.get("custom_id"))] = m.get("video_id")
                except Exception as e:
                    logger.warning("Failed reading meta file %s: %s", meta_file, e)
            with open(output_file, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    custom_id = str(obj.get("custom_id", "")).strip()
                    if not custom_id:
                        continue
                    # Keep original Mongo type (e.g., NumberLong/int) via metadata mapping.
                    video_id = meta_by_custom_id.get(custom_id, custom_id)
                    analysis = _extract_analysis_from_output_line(obj)
                    if not analysis:
                        continue
                    topics = analysis.get("topics") or []
                    summary = str(analysis.get("summary", "")).strip()
                    if not summary:
                        continue

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
                        category = (
                            (t.get("category") or "").strip()
                            if isinstance(t.get("category"), str)
                            else ""
                        )
                        topic_categories.append(category)
                        try:
                            topic_weights.append(float(t.get("weight", 0.0)))
                        except Exception:
                            topic_weights.append(0.0)
                    topics_json = json.dumps(
                        [
                            {"topic": n, "category": c, "weight": w}
                            for n, c, w in zip(topic_names, topic_categories, topic_weights)
                        ],
                        ensure_ascii=False,
                    )
                    neo4j_rows.append(
                        {
                            "video_id": video_id,
                            "summary": summary,
                        }
                    )
                    now = datetime.utcnow()
                    mongo_ops.append(
                        UpdateOne(
                            {
                                "video_id": video_id,
                                "$or": [
                                    {"comments_analyzed_at": {"$exists": False}},
                                    {"comments_analyzed_at": None},
                                ],
                            },
                            {
                                "$set": {
                                    "comments_frequent_topics": topic_names,
                                    "comments_frequent_topic_categories": topic_categories,
                                    "comments_frequent_topic_weights": topic_weights,
                                    "comments_frequent_topics_json": topics_json,
                                    "comment_summary_description": summary,
                                    "comments_analyzed_at": now,
                                }
                            },
                        )
                    )

            logger.info(
                "Batch %d/%d parsed in %.2fs (videos=%d).",
                i,
                len(submitted),
                time.perf_counter() - parse_started_at,
                len(neo4j_rows),
            )

            mongo_write_started_at = time.perf_counter()
            mongo_stats = _bulk_update_mongo(videos_coll, mongo_ops, MONGO_BATCH_WRITE_CHUNK)
            logger.info(
                "Batch %d/%d tiktok_user_video writes completed in %.2fs (sent=%d matched=%d modified=%d upserted=%d chunk_size=%d).",
                i,
                len(submitted),
                time.perf_counter() - mongo_write_started_at,
                mongo_stats["sent"],
                mongo_stats["matched"],
                mongo_stats["modified"],
                mongo_stats["upserted"],
                MONGO_BATCH_WRITE_CHUNK,
            )

            write_started_at = time.perf_counter()
            write_stats = _bulk_update_neo4j(driver, neo4j_rows, NEO4J_CHUNK)
            updated += write_stats["matched"]
            logger.info(
                "Batch %d/%d Neo4j writes completed in %.2fs (sent=%d, matched=%d, missing=%d, chunk_size=%d).",
                i,
                len(submitted),
                time.perf_counter() - write_started_at,
                write_stats["sent"],
                write_stats["matched"],
                write_stats["missing"],
                NEO4J_CHUNK,
            )

            parsed_outputs[output_file] = {
                "status": "completed",
                "parsed_at": datetime.utcnow().isoformat(),
                "updated_videos": max(write_stats["matched"], mongo_stats["modified"]),
            }
            progress["parsed_outputs"] = parsed_outputs
            save_progress(progress)
            logger.info(
                "Batch %d/%d total processing time: %.2fs.",
                i,
                len(submitted),
                time.perf_counter() - batch_started_at,
            )

        logger.info("Parse/update completed: updated=%d, skipped_already_parsed=%d", updated, skipped)
    finally:
        driver.close()


with DAG(
    "tiktok_comments_analysis_dag",
    default_args=default_args,
    description="Batch AI analysis for TikTok comments -> Neo4j updates",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 15, tz=local_tz),
    catchup=False,
    tags=["tiktok", "comments", "ai_analysis", "neo4j", "batch"],
    params={
        "limit": None,
        "offset": 0,
        "video_ids": None,
        "skip_analyzed": True,
    },
) as dag:
    # stage_task = PythonOperator(task_id="stage_openai_batches", python_callable=stage_openai_batches)
    submit_task = PythonOperator(task_id="submit_and_poll_batches", python_callable=submit_and_poll_batches)
    parse_task = PythonOperator(task_id="parse_and_update_results", python_callable=parse_and_update_results)
    #stage_task >> submit_task >> parse_task
    submit_task >> parse_task

