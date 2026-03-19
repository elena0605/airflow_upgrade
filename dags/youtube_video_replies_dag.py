from datetime import datetime, timedelta
from collections import defaultdict
from airflow import DAG  # pyright: ignore[reportMissingImports]
from airflow.providers.standard.operators.python import PythonOperator  # pyright: ignore[reportMissingImports]
from airflow.providers.mongo.hooks.mongo import MongoHook  # pyright: ignore[reportMissingImports]
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook  # pyright: ignore[reportMissingImports]
from callbacks import task_failure_callback, task_success_callback
from airflow.exceptions import AirflowFailException  # pyright: ignore[reportMissingImports]
import logging
from pymongo.errors import BulkWriteError  # pyright: ignore[reportMissingImports]
from pymongo.operations import UpdateOne  # pyright: ignore[reportMissingImports]
import youtube_etl as ye
import os
import time
import re
from typing import List, Dict, Any

# Set up logging
logger = logging.getLogger("airflow.task")

# Get environment variables 
airflow_env = os.getenv("AIRFLOW_ENV", "development")

# Configuration constants - tunable for performance
COMMENT_BATCH_SIZE = int(os.getenv("REPLY_COMMENT_BATCH_SIZE", "1000"))  # Comments to process per batch
MONGO_BULK_WRITE_SIZE = int(os.getenv("REPLY_MONGO_BULK_SIZE", "200"))  # MongoDB bulk write batch size
STATE_BULK_WRITE_SIZE = int(os.getenv("REPLY_STATE_BULK_SIZE", "50"))  # Lower default for CosmosDB state writes
REPLY_FETCH_BATCH_SIZE = int(os.getenv("REPLY_FETCH_BATCH_SIZE", "50"))  # Comments to fetch replies for in parallel
API_RATE_LIMIT_DELAY = float(os.getenv("YOUTUBE_API_DELAY", "0.1"))  # Delay between API calls (seconds)
MAX_RETRIES = int(os.getenv("REPLY_MAX_RETRIES", "3"))  # Max retries for failed API calls
STATE_WRITE_MAX_RETRIES = int(os.getenv("REPLY_STATE_WRITE_MAX_RETRIES", "6"))  # Keep retries bounded to avoid long stalls
READ_MAX_RETRIES = int(os.getenv("REPLY_MONGO_READ_MAX_RETRIES", "6"))
READ_MIN_SLEEP_SECONDS = float(os.getenv("REPLY_MONGO_READ_MIN_SLEEP_SECONDS", "0.2"))
MAX_REPLIES_PER_VIDEO = int(os.getenv("REPLY_MAX_REPLIES_PER_VIDEO", "1000"))  # Global replies cap per video in a DAG run
USE_REPLIES_STATE_COLLECTION = os.getenv("YT_REPLIES_USE_STATE_COLLECTION", "true").lower() in ("1", "true", "yes", "y")
REPLIES_STATE_COLLECTION_NAME = os.getenv("YT_REPLIES_STATE_COLLECTION", "youtube_replies_fetch_state")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": task_failure_callback,
    "on_success_callback": task_success_callback,
}

with DAG(
    "youtube_video_replies",
    default_args=default_args,
    description='A DAG to fetch, store, and transform YouTube video replies',
    schedule=None,
    start_date=datetime(2025, 1, 30),
    catchup=False,
    tags=['youtube_video_replies'],
) as dag:

    def fetch_and_store_video_replies(**context):
        """
        Optimized function to fetch and store YouTube video replies in batches.
        Processes comments in batches, uses bulk MongoDB operations, and handles errors efficiently.
        """
        try:
            # Choose the connection ID based on your environment (development or production)
            mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
            hook = MongoHook(mongo_conn_id=mongo_conn_id)
            client = hook.get_conn()
            # Dynamically choose the database based on the environment
            db_name = "rbl" if airflow_env == "production" else "airflow_db"
            db = client[db_name]

            comment_collection = db.youtube_video_comments
            replies_collection = db.youtube_video_replies
            state_collection = db[REPLIES_STATE_COLLECTION_NAME]

            # Create indexes if they don't exist (idempotent)
            try:
                replies_collection.create_index("comment_id", unique=True)
                replies_collection.create_index("parent_id")
                replies_collection.create_index("transformed_to_neo4j")
                if USE_REPLIES_STATE_COLLECTION:
                    try:
                        state_collection.create_index("comment_id", unique=True)
                    except Exception as e:
                        logger.warning(
                            "Could not create unique index on %s.comment_id (%s). Falling back to regular index.",
                            REPLIES_STATE_COLLECTION_NAME,
                            e,
                        )
                        state_collection.create_index("comment_id")
                    state_collection.create_index("replies_fetched")
                else:
                    comment_collection.create_index("replies_fetched")
            except Exception as e:
                logger.warning(f"Index creation warning (may already exist): {e}")

            # Base query for candidate comments.
            cursor_filter = {"comment_id": {"$exists": True, "$ne": None}}
            if not USE_REPLIES_STATE_COLLECTION:
                cursor_filter["replies_fetched"] = {"$ne": True}

            comments_processed = 0
            total_replies_found = 0
            total_replies_stored = 0
            batch_processed_updates = []  # Bulk updates for processed-state tracking
            batch_reply_operations = []  # Bulk operations for replies
            current_batch_comments = []  # Current batch of comments being processed
            replies_count_by_video = defaultdict(int)  # Track fetched replies count per video
            skipped_already_processed = 0
            fetched_at = datetime.now()
            last_seen_id = None

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

            def _retry_after_seconds(exc: Exception, attempt: int) -> float:
                msg = str(exc)
                match = re.search(r"RetryAfterMs=(\d+)", msg)
                if match:
                    try:
                        return max(READ_MIN_SLEEP_SECONDS, int(match.group(1)) / 1000.0)
                    except ValueError:
                        pass
                return max(READ_MIN_SLEEP_SECONDS, 0.5 * (2 ** attempt))

            def _next_comments_page() -> List[Dict[str, Any]]:
                """Read comments in _id-ordered pages to avoid long-lived cursors timing out."""
                nonlocal last_seen_id
                page_filter = dict(cursor_filter)
                if last_seen_id is not None:
                    page_filter["_id"] = {"$gt": last_seen_id}
                docs = []
                for attempt in range(READ_MAX_RETRIES):
                    try:
                        docs = list(
                            comment_collection.find(
                                page_filter,
                                {"_id": 1, "comment_id": 1, "channel_id": 1, "video_id": 1},
                            ).sort("_id", 1).limit(COMMENT_BATCH_SIZE)
                        )
                        break
                    except Exception as e:
                        if not _is_mongo_throttled(e) or attempt == READ_MAX_RETRIES - 1:
                            raise
                        wait = _retry_after_seconds(e, attempt)
                        logger.warning(
                            "State/read throttled while fetching comments page (attempt %d/%d). Retrying in %.2fs...",
                            attempt + 1,
                            READ_MAX_RETRIES,
                            wait,
                        )
                        time.sleep(wait)
                if docs:
                    last_seen_id = docs[-1].get("_id")
                return docs

            def _filter_unprocessed_comments(comments_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
                """Skip comments already marked processed in state collection."""
                nonlocal skipped_already_processed
                if not USE_REPLIES_STATE_COLLECTION or not comments_batch:
                    return comments_batch
                ids = [c.get("comment_id") for c in comments_batch if c.get("comment_id")]
                if not ids:
                    return []
                processed_ids = set()
                state_docs = []
                for attempt in range(READ_MAX_RETRIES):
                    try:
                        state_docs = list(
                            state_collection.find(
                                {"comment_id": {"$in": ids}, "replies_fetched": True},
                                {"comment_id": 1, "_id": 0},
                            )
                        )
                        break
                    except Exception as e:
                        if not _is_mongo_throttled(e) or attempt == READ_MAX_RETRIES - 1:
                            raise
                        wait = _retry_after_seconds(e, attempt)
                        logger.warning(
                            "State/read throttled while checking processed comment ids (attempt %d/%d). Retrying in %.2fs...",
                            attempt + 1,
                            READ_MAX_RETRIES,
                            wait,
                        )
                        time.sleep(wait)

                for doc in state_docs:
                    cid = doc.get("comment_id")
                    if cid:
                        processed_ids.add(cid)
                if not processed_ids:
                    return comments_batch
                filtered = [c for c in comments_batch if c.get("comment_id") not in processed_ids]
                skipped_already_processed += (len(comments_batch) - len(filtered))
                return filtered

            def process_reply_batch(replies: list[dict], channel_id: str) -> int:
                """Process a batch of replies and add to bulk operations."""
                stored_count = 0
                for reply in replies:
                    reply["fetched_at"] = fetched_at
                    reply["transformed_to_neo4j"] = False
                    if not reply.get("channel_id"):
                        reply["channel_id"] = channel_id

                    # Use upsert operation
                    batch_reply_operations.append(
                        UpdateOne(
                            {"comment_id": reply['comment_id']},
                            {"$setOnInsert": reply},
                            upsert=True
                        )
                    )
                    stored_count += 1
                return stored_count

            def flush_mongo_operations():
                """Flush accumulated MongoDB operations."""
                nonlocal total_replies_stored

                # Flush reply operations if batch_reply_operations has data
                if batch_reply_operations:
                    try:
                        result = ye.safe_bulk_write(
                            replies_collection,
                            batch_reply_operations,
                            max_retries=MAX_RETRIES
                        )
                        inserted = getattr(result, 'upserted_count', 0) + getattr(result, 'inserted_count', 0)
                        modified = getattr(result, 'modified_count', 0)
                        total_replies_stored += (inserted + modified)
                        logger.info(
                            f"Flushed {len(batch_reply_operations)} reply operations "
                            f"(upserted/inserted: {inserted}, modified: {modified})"
                        )
                        batch_reply_operations.clear()
                    except Exception as e:
                        logger.error(f"Error in bulk write for replies: {e}")
                        batch_reply_operations.clear()

                # Flush processed-state updates
                if batch_processed_updates:
                    try:
                        target_collection = state_collection if USE_REPLIES_STATE_COLLECTION else comment_collection
                        target_name = REPLIES_STATE_COLLECTION_NAME if USE_REPLIES_STATE_COLLECTION else "youtube_video_comments"
                        modified = 0
                        matched = 0
                        upserted = 0
                        remaining = []
                        for i in range(0, len(batch_processed_updates), STATE_BULK_WRITE_SIZE):
                            chunk = batch_processed_updates[i:i + STATE_BULK_WRITE_SIZE]
                            try:
                                result = ye.safe_bulk_write(
                                    target_collection,
                                    chunk,
                                    max_retries=STATE_WRITE_MAX_RETRIES,
                                )
                                modified += getattr(result, "modified_count", 0)
                                matched += getattr(result, "matched_count", 0)
                                upserted += getattr(result, "upserted_count", 0)
                            except Exception as e:
                                logger.error(
                                    "State write chunk failed for %s (chunk_size=%d): %s",
                                    target_name,
                                    len(chunk),
                                    e,
                                )
                                remaining.extend(chunk)

                        logger.info(
                            "Updated %d rows in %s as replies-processed (modified: %d, matched: %d, upserted: %d, deferred: %d)",
                            len(batch_processed_updates) - len(remaining),
                            target_name,
                            modified,
                            matched,
                            upserted,
                            len(remaining),
                        )
                        batch_processed_updates.clear()
                        batch_processed_updates.extend(remaining)
                    except Exception as e:
                        logger.error("Error in bulk write for replies processed-state: %s", e, exc_info=True)
                        # Don't clear on error - will retry on next flush
                        # Only clear if it's a non-retryable error
                        if not isinstance(e, BulkWriteError):
                            batch_processed_updates.clear()

            # Main processing loop (paged by _id, no long-lived read cursor).
            while True:
                page_docs = _next_comments_page()
                if not page_docs:
                    break
                for comment_doc in page_docs:
                    comment_id = comment_doc.get("comment_id")
                    channel_id = comment_doc.get("channel_id")
                    video_id = comment_doc.get("video_id")
                    if not comment_id:
                        continue

                    current_batch_comments.append({
                        "comment_id": comment_id,
                        "channel_id": channel_id,
                        "video_id": video_id
                    })

                    if len(current_batch_comments) >= REPLY_FETCH_BATCH_SIZE:
                        filtered_comments = _filter_unprocessed_comments(current_batch_comments)
                        for comment_info in filtered_comments:
                            cid = comment_info["comment_id"]
                            cvid = comment_info["video_id"]
                            cchid = comment_info["channel_id"]

                            # Fetch replies with retry
                            replies = None
                            for retry in range(MAX_RETRIES):
                                try:
                                    replies = ye.get_replies(
                                        cid,
                                        cvid,
                                        max_replies_per_video=MAX_REPLIES_PER_VIDEO,
                                        already_fetched_for_video=replies_count_by_video[cvid]
                                    )
                                    time.sleep(API_RATE_LIMIT_DELAY)
                                    break
                                except Exception as e:
                                    if retry < MAX_RETRIES - 1:
                                        wait_time = (retry + 1) * 2
                                        logger.warning(f"Retry {retry + 1}/{MAX_RETRIES} for comment {cid}: {e}")
                                        time.sleep(wait_time)
                                    else:
                                        logger.error(f"Failed to fetch replies for comment {cid} after {MAX_RETRIES} retries: {e}")

                            if replies:
                                stored = process_reply_batch(replies, cchid)
                                total_replies_found += len(replies)
                                replies_count_by_video[cvid] += len(replies)
                                logger.debug(f"Fetched {len(replies)} replies for comment {cid}")

                            if USE_REPLIES_STATE_COLLECTION:
                                batch_processed_updates.append(
                                    UpdateOne(
                                        {"comment_id": cid},
                                        {"$set": {
                                            "comment_id": cid,
                                            "video_id": cvid,
                                            "channel_id": cchid,
                                            "replies_fetched": True,
                                            "replies_fetched_at": fetched_at,
                                            "replies_count": len(replies) if replies else 0,
                                            "source": "youtube_video_replies_dag",
                                        }},
                                        upsert=True,
                                    )
                                )
                            else:
                                batch_processed_updates.append(
                                    UpdateOne(
                                        {"comment_id": cid},
                                        {"$set": {
                                            "replies_fetched": True,
                                            "replies_fetched_at": fetched_at,
                                            "replies_count": len(replies) if replies else 0
                                        }}
                                    )
                                )
                            comments_processed += 1

                        if len(batch_reply_operations) >= MONGO_BULK_WRITE_SIZE:
                            flush_mongo_operations()
                        current_batch_comments.clear()

                        if comments_processed > 0 and comments_processed % 1000 == 0:
                            logger.info(
                                f"Progress: {comments_processed} comments processed, "
                                f"{total_replies_found} replies found, "
                                f"{total_replies_stored} replies stored"
                            )

            # Process remaining comments
            if current_batch_comments:
                filtered_comments = _filter_unprocessed_comments(current_batch_comments)
                for comment_info in filtered_comments:
                    cid = comment_info["comment_id"]
                    cvid = comment_info["video_id"]
                    cchid = comment_info["channel_id"]

                    replies = None
                    for retry in range(MAX_RETRIES):
                        try:
                            replies = ye.get_replies(
                                cid,
                                cvid,
                                max_replies_per_video=MAX_REPLIES_PER_VIDEO,
                                already_fetched_for_video=replies_count_by_video[cvid]
                            )
                            time.sleep(API_RATE_LIMIT_DELAY)
                            break
                        except Exception as e:
                            if retry < MAX_RETRIES - 1:
                                wait_time = (retry + 1) * 2
                                logger.warning(f"Retry {retry + 1}/{MAX_RETRIES} for comment {cid}: {e}")
                                time.sleep(wait_time)
                            else:
                                logger.error(f"Failed to fetch replies for comment {cid} after {MAX_RETRIES} retries: {e}")

                    if replies:
                        stored = process_reply_batch(replies, cchid)
                        total_replies_found += len(replies)
                        replies_count_by_video[cvid] += len(replies)

                    if USE_REPLIES_STATE_COLLECTION:
                        batch_processed_updates.append(
                            UpdateOne(
                                {"comment_id": cid},
                                {"$set": {
                                    "comment_id": cid,
                                    "video_id": cvid,
                                    "channel_id": cchid,
                                    "replies_fetched": True,
                                    "replies_fetched_at": fetched_at,
                                    "replies_count": len(replies) if replies else 0,
                                    "source": "youtube_video_replies_dag",
                                }},
                                upsert=True,
                            )
                        )
                    else:
                        batch_processed_updates.append(
                            UpdateOne(
                                {"comment_id": cid},
                                {"$set": {
                                    "replies_fetched": True,
                                    "replies_fetched_at": fetched_at,
                                    "replies_count": len(replies) if replies else 0
                                }}
                            )
                        )
                    comments_processed += 1

            # Final flush
            flush_mongo_operations()

            logger.info(
                f"Processing complete: {comments_processed} comments processed, "
                f"{total_replies_found} replies found, "
                f"{total_replies_stored} replies stored in MongoDB, "
                f"{skipped_already_processed} comments skipped (already processed state)"
            )

        except Exception as e:
            logger.error(f"Error in fetch_and_store_replies: {e}", exc_info=True)
            raise

    # def transform_to_graph(**context):
    #     try:
    #         mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
    #         mongo_hook = MongoHook(mongo_conn_id=mongo_conn_id)
    #         mongo_client = mongo_hook.get_conn()
    #         db_name = "rbl" if airflow_env == "production" else "airflow_db"
    #         db = mongo_client[db_name]
    #         collection = db.youtube_video_replies
    #
    #         neo4j_conn_id = "neo4j_prod" if airflow_env == "production" else "neo4j_default"
    #         hook = Neo4jHook(conn_id=neo4j_conn_id)
    #         driver = hook.get_conn()
    #
    #         with driver.session() as session:
    #             # Only fetch untransformed replies
    #             documents = collection.find({
    #                 "transformed_to_neo4j": False
    #             })
    #
    #             replies_processed = 0
    #
    #             for doc in documents:
    #                 try:
    #                     session.run(
    #                         """
    #                         MERGE (r:YouTubeVideoReply {reply_id: $reply_id})
    #                         MERGE (c:YouTubeVideoComment {comment_id: $parent_id})
    #                         SET
    #                           r.reply_id = $reply_id,
    #                           r.channel_id = $channel_id,
    #                           r.parent_id = $parent_id,
    #                           r.text = $text,
    #                           r.authorDisplayName = $authorDisplayName,
    #                           r.authorProfileImageUrl = $authorProfileImageUrl,
    #                           r.authorChannelUrl = $authorChannelUrl,
    #                           r.authorChannelId = $authorChannelId,
    #                           r.canRate = $canRate,
    #                           r.viewerRating = $viewerRating,
    #                           r.likeCount = $likeCount,
    #                           r.publishedAt = $publishedAt,
    #                           r.updatedAt = $updatedAt
    #
    #                         MERGE (r)-[:REPLY_TO_YOUTUBE_COMMENT]->(c)
    #                         """,
    #                         reply_id=doc.get("reply_id"),
    #                         channel_id=doc.get("channel_id"),
    #                         parent_id=doc.get("parent_id"),
    #                         text=doc.get("text"),
    #                         authorDisplayName=doc.get("authorDisplayName"),
    #                         authorProfileImageUrl=doc.get("authorProfileImageUrl"),
    #                         authorChannelUrl=doc.get("authorChannelUrl"),
    #                         authorChannelId=doc.get("authorChannelId"),
    #                         canRate=doc.get("canRate"),
    #                         viewerRating=doc.get("viewerRating"),
    #                         likeCount=doc.get("likeCount"),
    #                         publishedAt=doc.get("publishedAt"),
    #                         updatedAt=doc.get("updatedAt")
    #                     )
    #
    #                     # Mark as transformed in MongoDB
    #                     collection.update_one(
    #                         {"reply_id": doc["reply_id"]},
    #                         {"$set": {"transformed_to_neo4j": True}}
    #                     )
    #
    #                     replies_processed += 1
    #                     logger.info(f"Transformed reply {doc.get('reply_id')} for comment {doc.get('parent_id')}")
    #
    #                 except Exception as e:
    #                     logger.error(f"Error transforming reply {doc.get('reply_id')}: {e}")
    #                     continue
    #
    #             logger.info(f"Successfully transformed {replies_processed} replies to Neo4j")
    #
    #     except Exception as e:
    #         logger.error(f"Error in transforming replies to Neo4j: {e}")
    #         raise

    fetch_and_store_video_replies_task = PythonOperator(
        task_id='fetch_and_store_video_replies',
        python_callable=fetch_and_store_video_replies,
    )
    # transform_to_graph_task = PythonOperator(
    #     task_id='transform_to_graph',
    #     python_callable=transform_to_graph,
    # )

    # fetch_and_store_video_replies_task >> transform_to_graph_task
    fetch_and_store_video_replies_task