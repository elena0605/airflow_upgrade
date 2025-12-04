# /opt/airflow/dags/youtube_thumbnail_analysis_dag.py
from __future__ import annotations
import os
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any

from pymongo import errors as pymongo_errors
from pymongo.operations import UpdateOne
from openai import OpenAI
from openai import AzureOpenAI

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook

logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

# ---------------------------
# Tunables - change as needed
# ---------------------------
CHUNK_SIZE = int(os.getenv("OPENAI_CHUNK_SIZE", "1000"))          # docs per OpenAI Batch job
MONGO_BATCH_WRITE_CHUNK = int(os.getenv("MONGO_BATCH_WRITE_CHUNK", "100"))  # docs per bulk write to Mongo
NEO4J_CHUNK = int(os.getenv("NEO4J_CHUNK", "100"))                # nodes per transaction to Neo4j
POLL_INTERVAL = int(os.getenv("OPENAI_POLL_INTERVAL", "10"))     # seconds between polling batch status
TMP_DIR = Path(os.getenv("OPENAI_BATCH_TMP", "/opt/airflow/dags/tmp_openai_batches"))
TMP_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------
# Environment / connections
# ---------------------------
# Get environment variable
AIRFLOW_ENV = os.getenv("AIRFLOW_ENV", "development")

DB_NAME = "rbl" if AIRFLOW_ENV == "production" else "airflow_db"
COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "youtube_channel_videos")

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "o4-mini")

PROMPT = os.getenv("YOUTUBE_PROMPT") or '''Task: You are an expert in media psychology, visual communication, and public-health behaviour. Analyze the thumbnail image and output ONLY valid JSON with the following fields:

1. Description (up to 1000 characters)
Provide a clear, objective, non-judgmental description of everything visible in the thumbnail, including:
- People (age group, gender presentation, facial expressions — avoid guessing identity)
- Objects
- Setting/background
- Text or symbols
- Colors, composition, or visual emphasis
- Any suggestive emotions conveyed (e.g., excitement, fear, curiosity)

2. Keywords (5 to 10)
Provide a list of high-level keywords that capture:
- Visual elements
- Themes
- Possible emotions evoked
- Behaviour- or health-related aspects (if any)

Return ONLY JSON. Do not include any additional text.
'''

# ---------------------------
# Helpers
# ---------------------------
def get_mongo():
    """Get MongoDB collection using MongoHook based on environment."""
    mongo_conn_id = "mongo_prod" if AIRFLOW_ENV == "production" else "mongo_default"
    mongo_hook = MongoHook(mongo_conn_id=mongo_conn_id)
    mongo_client = mongo_hook.get_conn()
    db = mongo_client[DB_NAME]
    return db[COLLECTION_NAME]

def get_neo4j_driver():
    """Get Neo4j driver using Neo4jHook based on environment."""
    neo4j_conn_id = "neo4j_prod" if AIRFLOW_ENV == "production" else "neo4j_default"
    neo4j_hook = Neo4jHook(conn_id=neo4j_conn_id)
    return neo4j_hook.get_conn()

def get_openai_client():
    """Get OpenAI or Azure OpenAI client from Airflow Variables.
    If Azure variables are set, use AzureOpenAI, otherwise use regular OpenAI.
    """
    try:
        # Check for Azure OpenAI variables first
        try:
            azure_endpoint = Variable.get("AZURE_OPENAI_ENDPOINT", default_var=None)
            azure_api_key = Variable.get("AZURE_OPENAI_API_KEY", default_var=None)
            azure_api_version = Variable.get("AZURE_OPENAI_API_VERSION", default_var="2024-10-21")
            
            if azure_endpoint and azure_api_key:
                logger.info("Using Azure OpenAI client")
                # Update OPENAI_MODEL if Azure model is specified
                try:
                    azure_model = Variable.get("AZURE_OPENAI_MODEL", default_var=None)
                    if azure_model:
                        global OPENAI_MODEL
                        OPENAI_MODEL = azure_model
                        logger.info("Using Azure OpenAI model: %s", azure_model)
                except Exception:
                    pass
                
                return AzureOpenAI(
                    api_key=azure_api_key,
                    api_version=azure_api_version,
                    azure_endpoint=azure_endpoint
                )
        except Exception as e:
            logger.debug("Azure OpenAI variables not found, trying regular OpenAI: %s", e)
        
        # Fallback to regular OpenAI
        api_key = Variable.get("OPENAI_API_KEY")
        logger.info("Using regular OpenAI client")
        return OpenAI(api_key=api_key)
    except Exception as e:
        logger.error(f"Failed to get OpenAI client from Airflow Variables: {e}")
        raise

def build_openai_request_body(thumbnail_url: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Return the JSON structure per-line required by the OpenAI Batch format."""
    # custom_id must be <= 512 chars, so only store essential fields
    # Full metadata is saved in a separate mapping file
    minimal_meta = {
        "_id": metadata.get("_id"),
        "video_id": metadata.get("video_id")
    }
    custom_id_str = json.dumps(minimal_meta, ensure_ascii=False)
    
    # Validate length (should be well under 512, but check anyway)
    if len(custom_id_str) > 512:
        logger.warning("custom_id too long (%d chars), truncating metadata", len(custom_id_str))
        # Fallback: just use _id if even minimal metadata is too long
        custom_id_str = json.dumps({"_id": metadata.get("_id")}, ensure_ascii=False)
    
    body = {
        "custom_id": custom_id_str,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": OPENAI_MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": PROMPT},
                        {"type": "image_url", "image_url": {"url": thumbnail_url, "detail": "high"}}
                    ]
                }
            ]
        }
    }
    return body

# ---------------------------
# Task 1: Stage JSONL chunks for OpenAI Batch
# ---------------------------
def stage_openai_batches(**context):
    """
    Query Mongo for docs that don't have thumbnail_description OR thumbnail_keywords,
    create chunked jsonl files ready to upload to OpenAI Batch API.
    Push list of file paths to xcom.
    """
    # Clean up old batch files before creating new ones
    logger.info("Cleaning up old batch files in %s", TMP_DIR)
    old_input_files = list(TMP_DIR.glob("youtube_batch_*.jsonl"))
    old_meta_files = list(TMP_DIR.glob("youtube_batch_meta_*.json"))
    old_output_files = list(TMP_DIR.glob("youtube_batch_output_*.jsonl"))
    
    deleted_input = 0
    deleted_meta = 0
    deleted_output = 0
    
    for f in old_input_files:
        try:
            f.unlink()
            deleted_input += 1
        except Exception as e:
            logger.warning("Failed to delete %s: %s", f, e)
    
    for f in old_meta_files:
        try:
            f.unlink()
            deleted_meta += 1
        except Exception as e:
            logger.warning("Failed to delete %s: %s", f, e)
    
    # Optionally delete output files too (uncomment if you want to clean everything)
    # for f in old_output_files:
    #     try:
    #         f.unlink()
    #         deleted_output += 1
    #     except Exception as e:
    #         logger.warning("Failed to delete %s: %s", f, e)
    
    logger.info("Cleanup complete: deleted %d input files, %d metadata files, %d output files (kept)", 
                deleted_input, deleted_meta, deleted_output)
    
    coll = get_mongo()
    query = {"$or": [{"thumbnail_description": {"$exists": False}}, {"thumbnail_keywords": {"$exists": False}}]}
    projection = {"video_id": 1, "thumbnail_url": 1, "video_url": 1}
    cursor = coll.find(query, projection=projection).sort("_id", 1)

    file_paths: List[str] = []
    batch_num = 0
    buffer: List[Dict[str, Any]] = []
    metadata_buffer: List[Dict[str, Any]] = []  # Store full metadata for mapping file
    
    # Track statistics
    total_queried = 0
    skipped_no_video_id = 0
    skipped_no_thumbnail = 0
    processed = 0

    for doc in cursor:
        total_queried += 1
        video_id = doc.get("video_id")
        thumbnail_url = doc.get("thumbnail_url")
        video_url = doc.get("video_url")
        
        if not video_id:
            skipped_no_video_id += 1
            logger.debug("Skipping doc %s: missing video_id", doc.get("_id"))
            continue
        
        # Skip if no thumbnail URL (already stored in MongoDB)
        if not thumbnail_url:
            skipped_no_thumbnail += 1
            logger.info("Skipping (no thumbnail_url) doc %s", doc.get("_id"))
            continue
        
        processed += 1

        meta = {
            "_id": str(doc["_id"]), 
            "video_url": video_url or f"https://www.youtube.com/watch?v={video_id}",
            "thumbnail_url": thumbnail_url, 
            "video_id": str(video_id)
        }
        buffer.append(build_openai_request_body(thumbnail_url, meta))
        metadata_buffer.append(meta)  # Store full metadata

        if len(buffer) >= CHUNK_SIZE:
            batch_num += 1
            timestamp = int(time.time())
            out = TMP_DIR / f"youtube_batch_{batch_num}_{timestamp}.jsonl"
            meta_file = TMP_DIR / f"youtube_batch_meta_{batch_num}_{timestamp}.json"
            
            # Write batch file
            with out.open("w", encoding="utf-8") as fh:
                for item in buffer:
                    fh.write(json.dumps(item, ensure_ascii=False) + "\n")
            
            # Write metadata mapping file (maps _id to full metadata)
            with meta_file.open("w", encoding="utf-8") as fh:
                json.dump({m["_id"]: m for m in metadata_buffer}, fh, ensure_ascii=False)
            
            file_paths.append(str(out))
            logger.info("Wrote batch file %s (%d items) and metadata file %s", out, len(buffer), meta_file)
            buffer = []
            metadata_buffer = []

    # leftover
    if buffer:
        batch_num += 1
        timestamp = int(time.time())
        out = TMP_DIR / f"youtube_batch_{batch_num}_{timestamp}.jsonl"
        meta_file = TMP_DIR / f"youtube_batch_meta_{batch_num}_{timestamp}.json"
        
        # Write batch file
        with out.open("w", encoding="utf-8") as fh:
            for item in buffer:
                fh.write(json.dumps(item, ensure_ascii=False) + "\n")
        
        # Write metadata mapping file
        with meta_file.open("w", encoding="utf-8") as fh:
            json.dump({m["_id"]: m for m in metadata_buffer}, fh, ensure_ascii=False)
        
        file_paths.append(str(out))
        logger.info("Wrote final batch file %s (%d items) and metadata file %s", out, len(buffer), meta_file)

    logger.info("Total batch files staged: %d", len(file_paths))
    logger.info("Processing summary: queried=%d, processed=%d, skipped_no_video_id=%d, skipped_no_thumbnail=%d", 
                total_queried, processed, skipped_no_video_id, skipped_no_thumbnail)
    # Also store metadata file paths for later use
    meta_file_paths = sorted(TMP_DIR.glob("youtube_batch_meta_*.json"))
    context["ti"].xcom_push(key="openai_batch_files", value=file_paths)
    context["ti"].xcom_push(key="openai_batch_meta_files", value=[str(p) for p in meta_file_paths])
    return file_paths

# ---------------------------
# Task 2: Upload batches to OpenAI + poll until completed
# ---------------------------
def submit_and_poll_batches(**context):
    client = get_openai_client()
    
    # Try to get file paths from xcom first, but fallback to scanning directory
    file_paths: List[str] = context["ti"].xcom_pull(key="openai_batch_files", task_ids="stage_openai_batches")
    
    # If xcom is empty or None, scan the tmp directory for batch files
    if not file_paths:
        logger.info("No batch files from xcom, scanning tmp directory: %s", TMP_DIR)
        # Find all jsonl files that match the batch pattern
        batch_files = sorted(TMP_DIR.glob("youtube_batch_*.jsonl"))
        file_paths = [str(f) for f in batch_files]
        logger.info("Found %d batch files in tmp directory", len(file_paths))
    
    if not file_paths:
        logger.info("No batch files to submit.")
        return []

    submitted_info = []  # each item: {file_path, upload_id, batch_id, output_path}

    for fp in file_paths:
        logger.info("Uploading %s to OpenAI files API", fp)
        with open(fp, "rb") as fh:
            uploaded = client.files.create(file=fh, purpose="batch")
        input_file_id = uploaded.id
        logger.info("Uploaded -> file_id=%s", input_file_id)

        # Create the batch
        batch = client.batches.create(input_file_id=input_file_id, endpoint="/v1/chat/completions", completion_window="24h")
        batch_id = batch.id
        logger.info("Created batch id %s for file %s", batch_id, fp)

        # Poll until done
        status = None
        while True:
            time.sleep(POLL_INTERVAL)
            batch_status = client.batches.retrieve(batch_id)
            status = getattr(batch_status, "status", None)
            
            # Log request counts if available
            request_counts = None
            try:
                if hasattr(batch_status, "request_counts"):
                    request_counts = batch_status.request_counts
                
                if request_counts:
                    total = getattr(request_counts, "total", None) or (request_counts.get("total") if isinstance(request_counts, dict) else None)
                    completed = getattr(request_counts, "completed", None) or (request_counts.get("completed") if isinstance(request_counts, dict) else None)
                    failed = getattr(request_counts, "failed", None) or (request_counts.get("failed") if isinstance(request_counts, dict) else None)
                    logger.info("Batch %s status: %s (total: %s, completed: %s, failed: %s)", 
                              batch_id, status, total, completed, failed)
                else:
                    logger.info("Batch %s status: %s", batch_id, status)
            except Exception as e:
                logger.warning("Could not extract request counts: %s", e)
            
            if status in ("completed", "succeeded"):
                # Log final counts before breaking
                if request_counts:
                    total = getattr(request_counts, "total", None) or (request_counts.get("total") if isinstance(request_counts, dict) else None)
                    completed = getattr(request_counts, "completed", None) or (request_counts.get("completed") if isinstance(request_counts, dict) else None)
                    failed = getattr(request_counts, "failed", None) or (request_counts.get("failed") if isinstance(request_counts, dict) else None)
                    logger.info("Batch %s completed: total=%s, completed=%s, failed=%s", batch_id, total, completed, failed)
                break
            if status in ("failed", "cancelled", "expired"):
                # Get error details if available
                error_info = ""
                try:
                    if hasattr(batch_status, "errors") and batch_status.errors:
                        error_info = f" Errors: {batch_status.errors}"
                    if request_counts:
                        error_info += f" Request counts: {request_counts}"
                except Exception as e:
                    logger.warning("Could not extract error details: %s", e)
                
                logger.error("Batch %s ended with status %s%s", batch_id, status, error_info)
                raise RuntimeError(f"OpenAI batch {batch_id} failed: {status}{error_info}")
            # else continue polling

        # download output (even if all requests failed, we still get an output file with error details)
        output_file_id = getattr(batch_status, "output_file_id", None)
        
        # If no output_file_id, check for error_file_id (sometimes used when all requests fail)
        if not output_file_id:
            error_file_id = getattr(batch_status, "error_file_id", None)
            if error_file_id:
                logger.info("No output_file_id, but found error_file_id: %s", error_file_id)
                output_file_id = error_file_id
        
        # If still no file ID, try to get errors directly from batch_status
        if not output_file_id:
            logger.warning("Batch %s completed but no output_file_id. Status: %s, Failed: %s", 
                        batch_id, status, failed if request_counts else "unknown")
            
            # Log all available attributes for debugging
            logger.info("Available batch_status attributes: %s", [attr for attr in dir(batch_status) if not attr.startswith('_')])
            
            # Try to get error details directly from batch_status
            error_details = None
            sample_error = None
            try:
                # Check various possible error fields
                if hasattr(batch_status, "errors") and batch_status.errors:
                    error_details = batch_status.errors
                    logger.error("Batch errors (from batch_status.errors): %s", error_details)
                elif hasattr(batch_status, "error") and batch_status.error:
                    error_details = batch_status.error
                    logger.error("Batch error (from batch_status.error): %s", error_details)
                
                # Try to extract a sample error message
                if error_details:
                    try:
                        if hasattr(error_details, "data") and error_details.data:
                            errors_list = error_details.data
                            if errors_list and len(errors_list) > 0:
                                first_error = errors_list[0]
                                if hasattr(first_error, "message"):
                                    sample_error = first_error.message
                                elif isinstance(first_error, dict):
                                    sample_error = first_error.get("message", str(first_error))
                                else:
                                    sample_error = str(first_error)
                        elif isinstance(error_details, dict) and error_details.get("data"):
                            errors_list = error_details.get("data", [])
                            if errors_list and len(errors_list) > 0:
                                first_error = errors_list[0]
                                if isinstance(first_error, dict):
                                    sample_error = first_error.get("message", str(first_error))
                                else:
                                    sample_error = str(first_error)
                        elif isinstance(error_details, list) and len(error_details) > 0:
                            sample_error = str(error_details[0])
                        else:
                            sample_error = str(error_details)
                    except Exception as e:
                        logger.warning("Could not parse error details: %s", e)
                        sample_error = str(error_details)[:500]  # First 500 chars
                
                if sample_error:
                    logger.error("Sample error from batch: %s", sample_error[:500])
            except Exception as e:
                logger.warning("Could not retrieve error details: %s", e)
            
            # Raise error with context
            error_msg = f"Batch {batch_id} has no output file id. All requests failed."
            if sample_error:
                error_msg += f" Sample error: {sample_error[:300]}"
            raise RuntimeError(error_msg)
        # fetch contents
        logger.info("Fetching batch output file id %s", output_file_id)
        content_bytes = client.files.content(output_file_id).content
        outpath = TMP_DIR / f"youtube_batch_output_{batch_id}.jsonl"
        with outpath.open("wb") as f:
            f.write(content_bytes)
        
        # Count lines in output file to verify
        output_lines = content_bytes.decode('utf-8').strip().split('\n')
        non_empty_lines = [line for line in output_lines if line.strip()]
        logger.info("Saved output to %s (%d non-empty lines)", outpath, len(non_empty_lines))
        
        # Check for errors in the output file
        error_count = 0
        sample_errors = []
        for line in non_empty_lines[:10]:  # Check first 10 lines for errors
            try:
                rec = json.loads(line)
                if rec.get("error"):
                    error_count += 1
                    if len(sample_errors) < 3:
                        error_info = rec.get("error", {})
                        error_msg = error_info.get("message", "Unknown error") if isinstance(error_info, dict) else str(error_info)
                        sample_errors.append(error_msg)
            except Exception:
                pass
        
        if error_count > 0:
            logger.warning("Found %d errors in output file (checked first 10 lines). Sample errors: %s", 
                         error_count, sample_errors)
        
        # Log if there's a mismatch
        if request_counts:
            expected = getattr(request_counts, "total", None) or (request_counts.get("total") if isinstance(request_counts, dict) else None)
            if expected and len(non_empty_lines) < expected:
                logger.warning("Output file has %d lines but batch had %d total requests. Some requests may have failed.", 
                             len(non_empty_lines), expected)

        # Try to find corresponding metadata file for this batch
        # Input file: youtube_batch_{batch_num}_{timestamp}.jsonl
        # Extract timestamp from input file name
        input_path = Path(fp)
        input_stem = input_path.stem  # e.g., "youtube_batch_1_1764283236"
        # Try to find matching metadata file
        meta_file_path = None
        if "_" in input_stem:
            parts = input_stem.split("_")
            if len(parts) >= 4:  # youtube_batch_{num}_{timestamp}
                batch_num = parts[2]
                timestamp = parts[3]
                meta_pattern = f"youtube_batch_meta_{batch_num}_{timestamp}.json"
                meta_file = TMP_DIR / meta_pattern
                if meta_file.exists():
                    meta_file_path = str(meta_file)
        
        submitted_info.append({
            "input_file": fp,
            "input_file_id": input_file_id,
            "batch_id": batch_id,
            "output_path": str(outpath),
            "metadata_file": meta_file_path
        })

    context["ti"].xcom_push(key="openai_submitted_batches", value=submitted_info)
    return submitted_info

# ---------------------------
# Helpers for resilient writes
# ---------------------------
def backoff_sleep_from_retry_after(retry_after_ms: int | None, attempt: int = 1):
    if retry_after_ms:
        t = max(retry_after_ms / 1000.0, 0.5)
    else:
        # exponential fallback
        t = min(2 ** attempt, 30)
    logger.info("Sleeping %.2fs before retry (attempt %d)", t, attempt)
    time.sleep(t)

def mongo_safe_bulk_update(collection, operations: List[Dict[str, Any]]):
    """
    operations: list of {"filter": {...}, "update": {...}}
    Will run in chunks, use unordered bulk_write, and retry on 429 (throttling).
    """
    if not operations:
        return
    # chunk ops
    attempt = 0
    for i in range(0, len(operations), MONGO_BATCH_WRITE_CHUNK):
        chunk = operations[i:i+MONGO_BATCH_WRITE_CHUNK]
        attempt = 0
        while True:
            try:
                requests_bulk = []
                for op in chunk:
                    requests_bulk.append(
                        UpdateOne(op["filter"], {"$set": op["update"]}, upsert=False)
                    )
                res = collection.bulk_write(requests_bulk, ordered=False)
                logger.info("Mongo bulk write result: matched=%s modified=%s", res.matched_count, res.modified_count)
                break
            except pymongo_errors.BulkWriteError as bwe:
                # check for 429 like errors in details
                logger.warning("BulkWriteError: %s", bwe.details)
                # attempt best-effort to extract RetryAfterMs
                retry_after = None
                try:
                    # Some Azure Cosmos responses include retry info in the first writeError detail
                    details = bwe.details or {}
                    write_errors = details.get("writeErrors", [])
                    if write_errors and isinstance(write_errors, list):
                        err0 = write_errors[0]
                        # sometimes message contains RetryAfterMs
                        if "RetryAfterMs" in err0:
                            retry_after = int(err0["RetryAfterMs"])
                except Exception:
                    retry_after = None
                attempt += 1
                if attempt > 6:
                    logger.error("Too many attempts for mongo bulk chunk, aborting chunk")
                    raise
                backoff_sleep_from_retry_after(retry_after, attempt)
            except pymongo_errors.OperationFailure as ofe:
                logger.warning("Mongo OperationFailure: %s", ofe)
                attempt += 1
                if attempt > 6:
                    raise
                backoff_sleep_from_retry_after(None, attempt)
            except Exception as e:
                logger.exception("Unexpected error during mongo bulk write: %s", e)
                attempt += 1
                if attempt > 6:
                    raise
                backoff_sleep_from_retry_after(None, attempt)

# ---------------------------
# Task 3: Parse outputs and bulk update Mongo & Neo4j
# ---------------------------
def parse_outputs_and_update(**context):
    """
    Reads OpenAI batch output files (.jsonl), extracts thumbnail descriptions + keywords,
    and updates MongoDB + Neo4j. Handles malformed JSON gracefully.
    """
    submitted_info: List[Dict[str, Any]] = context["ti"].xcom_pull(
        key="openai_submitted_batches",
        task_ids="submit_and_poll_batches"
    )

    # If Op1 was never run or the worker restarted → scan tmp dir as fallback
    if not submitted_info:
        logger.warning("No XCom batch info found — scanning %s for output files.", TMP_DIR)
        submitted_info = []
        for p in sorted(TMP_DIR.glob("youtube_batch_output_*.jsonl")):
            submitted_info.append({
                "output_path": str(p),
                "batch_id": p.stem.replace("youtube_batch_output_", "")
            })

    if not submitted_info:
        logger.error("No output files found — nothing to parse.")
        return False

    mongo = get_mongo()
    neo4j = get_neo4j_driver()

    for batch in submitted_info:
        fpath = Path(batch["output_path"])
        if not fpath.exists():
            logger.error("Missing output file → %s", fpath)
            continue

        logger.info("Processing batch output → %s", fpath.name)

        # --- Load metadata mapping file ----------------------------------------
        metadata_map: Dict[str, Dict[str, Any]] = {}
        batch_id = batch.get("batch_id") or fpath.stem.replace("youtube_batch_output_", "")
        
        # First try to use metadata file path from batch info
        meta_file_path = batch.get("metadata_file")
        if meta_file_path and Path(meta_file_path).exists():
            try:
                with open(meta_file_path, "r", encoding="utf-8") as fh:
                    metadata_map = json.load(fh)
                logger.info("Loaded metadata from %s (%d entries)", meta_file_path, len(metadata_map))
            except Exception as e:
                logger.warning("Failed to load metadata file %s: %s", meta_file_path, e)
        
        # Fallback: load all metadata files and merge (works for directory scan case)
        if not metadata_map:
            meta_files = sorted(TMP_DIR.glob("youtube_batch_meta_*.json"))
            if meta_files:
                logger.info("Loading all metadata files as fallback (%d files)", len(meta_files))
                for meta_file in meta_files:
                    try:
                        with meta_file.open("r", encoding="utf-8") as fh:
                            file_meta = json.load(fh)
                            metadata_map.update(file_meta)
                    except Exception as e:
                        logger.warning("Failed to load metadata file %s: %s", meta_file, e)
        
        if not metadata_map:
            logger.warning("No metadata mapping file found for batch %s - video_url may be missing", batch_id)

        mongo_ops = []
        neo4j_rows = []

        with fpath.open("r", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue

                try:
                    rec = json.loads(line)   # one JSON per line
                except Exception:
                    logger.warning("Skipping INVALID JSON line.")
                    continue

                # --- 1) Extract minimal metadata from custom_id -----------------
                cid = rec.get("custom_id")
                try:
                    minimal_meta = json.loads(cid) if isinstance(cid, str) else {}
                except json.JSONDecodeError:
                    logger.error("Bad custom_id JSON → %s", cid)
                    continue

                doc_id = minimal_meta.get("_id") or minimal_meta.get("id")
                if not doc_id:
                    logger.warning("Skipping record — no _id inside custom_id")
                    continue

                # --- Get full metadata from mapping file -----------------------
                full_meta = metadata_map.get(doc_id, {})
                video_url = full_meta.get("video_url")
                video_id = full_meta.get("video_id") or minimal_meta.get("video_id") or minimal_meta.get("videoId")

                # --- 2) Extract AI JSON output ---------------------------------
                body = rec.get("response", {}).get("body", {})
                choices = body.get("choices", [])

                content = ""
                if choices and isinstance(choices, list) and len(choices) > 0:
                    choice = choices[0]
                    if isinstance(choice, dict) and "message" in choice:
                        message = choice["message"]
                        if isinstance(message, dict) and "content" in message:
                            msg_content = message["content"]
                            if isinstance(msg_content, str):
                                content = msg_content
                            elif isinstance(msg_content, list):
                                # Handle array of content blocks (text + image_url)
                                for block in msg_content:
                                    if isinstance(block, dict) and block.get("type") == "text":
                                        content += block.get("text", "") + "\n"

                if not content:
                    logger.warning("No model content found for _id=%s", doc_id)
                    continue

                # Convert AI JSON string → dict (with code fence stripping and fallback)
                parsed = None
                txt = content.strip()
                # Strip code fences if present (OpenAI sometimes wraps JSON in ```)
                if txt.startswith("```"):
                    parts = txt.split("```")
                    if len(parts) >= 2:
                        txt = parts[1].strip()
                
                try:
                    parsed = json.loads(txt)
                except json.JSONDecodeError:
                    # Fallback: try extracting JSON substring
                    start = txt.find("{")
                    end = txt.rfind("}")
                    if start != -1 and end != -1:
                        try:
                            parsed = json.loads(txt[start:end+1])
                        except json.JSONDecodeError:
                            logger.error("Model output is not valid JSON → storing raw text only.")
                            parsed = None
                    else:
                        logger.error("Model output is not valid JSON → storing raw text only.")
                        parsed = None

                if parsed and isinstance(parsed, dict):
                    desc = parsed.get("Description") or parsed.get("description") or ""
                    kw = parsed.get("Keywords") or parsed.get("keywords") or []
                    if isinstance(kw, str):
                        kw = [k.strip() for k in kw.split(",") if k.strip()]
                else:
                    desc, kw = None, None

                # --- 3) MongoDB update structure -------------------------------
                update = {"processed_at": datetime.utcnow().isoformat()}
                if desc is not None:
                    update["thumbnail_description"] = desc
                if kw is not None:
                    update["thumbnail_keywords"] = kw
                if video_url:
                    update["video_url"] = video_url
                # Note: thumbnail_url is already stored, so we don't update it

                mongo_ops.append({
                    "filter": {"_id": __import__("bson").ObjectId(doc_id)},
                    "update": update
                })

                # Only add to Neo4j if we have a valid video_id
                if video_id:
                    neo4j_rows.append({
                        "video_id": video_id,
                        "description": desc or "",
                        "keywords": kw or [],
                        "video_url": video_url or ""
                    })

        # --- MongoDB write ---------------------------------------------------
        if mongo_ops:
            logger.info("Mongo bulk update → %d docs", len(mongo_ops))
            mongo_safe_bulk_update(mongo, mongo_ops)

        # --- Neo4j write -----------------------------------------------------
        if neo4j_rows:
            logger.info("Neo4j total rows to insert: %d", len(neo4j_rows))

            # Filter out rows with empty video_id, but log them
            valid_rows = []
            for r in neo4j_rows:
                if not r.get("video_id"):
                    logger.warning("Skipping Neo4j row with missing video_id: %s", r)
                else:
                    # Ensure video_id is string
                    r["video_id"] = str(r["video_id"])
                    # Ensure keywords is always a list (not string or None)
                    if isinstance(r.get("keywords"), str):
                        r["keywords"] = [k.strip() for k in r["keywords"].split(",") if k.strip()]
                    elif not isinstance(r.get("keywords"), list):
                        r["keywords"] = []
                    # Ensure description and video_url are strings (not None)
                    r["description"] = r.get("description") or ""
                    r["video_url"] = r.get("video_url") or ""
                    valid_rows.append(r)

            if not valid_rows:
                logger.warning("No valid rows to insert into Neo4j. Skipping.")
            else:
                logger.info("Neo4j valid rows after filtering: %d", len(valid_rows))
                # Log sample video_ids for debugging
                sample_ids = [r["video_id"] for r in valid_rows[:5]]
                logger.info("Sample video_ids being sent to Neo4j (first 5): %s (types: %s)", 
                          sample_ids, [type(vid).__name__ for vid in sample_ids])
                
                with neo4j.session() as session:
                    # Check what type video_id is in existing Neo4j nodes (optional debugging)
                    def check_video_id_type_tx(tx):
                        result = tx.run("""
                            MATCH (v:YouTubeVideo)
                            RETURN v.video_id AS video_id
                            LIMIT 5
                        """)
                        return list(result)
                    
                    try:
                        sample_nodes = session.read_transaction(check_video_id_type_tx)
                        if sample_nodes:
                            sample_ids = [str(record["video_id"]) for record in sample_nodes]
                            logger.info("Sample existing Neo4j video_ids (first 5): %s", sample_ids)
                        else:
                            logger.warning("No YouTubeVideo nodes found in Neo4j - cannot update")
                    except Exception as e:
                        logger.warning("Could not check Neo4j node types: %s", e)
                    
                    # Ensure unique constraint exists on video_id
                    def create_constraint_tx(tx):
                        tx.run("""
                            CREATE CONSTRAINT youtube_video_id_unique IF NOT EXISTS
                            FOR (v:YouTubeVideo) REQUIRE v.video_id IS UNIQUE
                        """)
                    
                    try:
                        session.write_transaction(create_constraint_tx)
                        logger.info("Neo4j unique constraint on video_id verified/created")
                    except Exception as e:
                        logger.warning("Could not create Neo4j constraint (may already exist): %s", e)
                    
                    # Process chunks - only update existing nodes, don't create new ones
                    # Use toString() for type-agnostic matching (handles both string and number video_ids)
                    for i in range(0, len(valid_rows), NEO4J_CHUNK):
                        chunk = valid_rows[i:i+NEO4J_CHUNK]
                        cypher = """
                        UNWIND $rows AS r
                        MATCH (v:YouTubeVideo)
                        WHERE toString(v.video_id) = toString(r.video_id)
                        SET v.thumbnail_description = r.description,
                            v.thumbnail_keywords = r.keywords,
                            v.video_url = r.video_url
                        """
                        try:
                            def write_tx(tx):
                                result = tx.run(cypher, rows=chunk)
                                return result.consume()
                            
                            summary = session.write_transaction(write_tx)
                            counters = summary.counters if hasattr(summary, 'counters') else None
                            properties_set = counters.properties_set if counters else 0
                            # Count how many nodes were actually matched (properties_set > 0 means node was found and updated)
                            logger.info("Neo4j chunk %d/%d processed: %d nodes attempted, properties_set: %d", 
                                      i//NEO4J_CHUNK + 1, 
                                      (len(valid_rows) + NEO4J_CHUNK - 1) // NEO4J_CHUNK,
                                      len(chunk),
                                      properties_set)
                        except Exception as e:
                            logger.exception("Neo4j batch failed for chunk %d: %s", i//NEO4J_CHUNK + 1, e)

    logger.info("✓ All batch outputs parsed & databases updated.")
    return True

# ---------------------------
# DAG definition
# ---------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="youtube_openai_batch_pipeline",
    default_args=default_args,
    description="Stage YouTube thumbnails -> OpenAI Batch -> parse -> bulk update Mongo + Neo4j",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "openai", "batch"],
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
        task_id="parse_outputs_and_update",
        python_callable=parse_outputs_and_update,
    )

    # Full pipeline: clean up old files -> create batches -> submit to OpenAI -> parse and update DBs
    t1 >> t2 >> t3

