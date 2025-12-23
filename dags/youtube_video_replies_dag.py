from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from callbacks import task_failure_callback, task_success_callback
from airflow.exceptions import AirflowFailException
import logging
from pymongo.errors import BulkWriteError
from pymongo.operations import UpdateOne
import youtube_etl as ye
import os
import time
from typing import List, Dict, Any

# Set up logging
logger = logging.getLogger("airflow.task")

# Get environment variables 
airflow_env = os.getenv("AIRFLOW_ENV", "development")

# Configuration constants - tunable for performance
COMMENT_BATCH_SIZE = int(os.getenv("REPLY_COMMENT_BATCH_SIZE", "1000"))  # Comments to process per batch
MONGO_BULK_WRITE_SIZE = int(os.getenv("REPLY_MONGO_BULK_SIZE", "500"))  # MongoDB bulk write batch size
REPLY_FETCH_BATCH_SIZE = int(os.getenv("REPLY_FETCH_BATCH_SIZE", "50"))  # Comments to fetch replies for in parallel
API_RATE_LIMIT_DELAY = float(os.getenv("YOUTUBE_API_DELAY", "0.1"))  # Delay between API calls (seconds)
MAX_RETRIES = int(os.getenv("REPLY_MAX_RETRIES", "3"))  # Max retries for failed API calls

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

            # Create indexes if they don't exist (idempotent)
            try:
                replies_collection.create_index("comment_id", unique=True)
                replies_collection.create_index("parent_id")
                replies_collection.create_index("transformed_to_neo4j")
                comment_collection.create_index("replies_fetched")
            except Exception as e:
                logger.warning(f"Index creation warning (may already exist): {e}")

            # Use cursor with batch_size for efficient pagination
            cursor = comment_collection.find(
                {"replies_fetched": {"$ne": True}},
                {"comment_id": 1, "channel_id": 1, "video_id": 1, "_id": 0}
            ).batch_size(COMMENT_BATCH_SIZE)

            comments_processed = 0
            total_replies_found = 0
            total_replies_stored = 0
            batch_comment_updates = []  # Bulk updates for comments
            batch_reply_operations = []  # Bulk operations for replies
            current_batch_comments = []  # Current batch of comments being processed
            fetched_at = datetime.now()

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

                # Flush comment updates
                # CRITICAL: Use safe_bulk_write for CosmosDB throttling handling
                if batch_comment_updates:
                    try:
                        result = ye.safe_bulk_write(
                            comment_collection,
                            batch_comment_updates,
                            max_retries=10  # Use higher retries for CosmosDB
                        )
                        modified = getattr(result, 'modified_count', 0)
                        matched = getattr(result, 'matched_count', 0)
                        logger.info(f"Updated {len(batch_comment_updates)} comments as processed "
                                  f"(modified: {modified}, matched: {matched})")
                        batch_comment_updates.clear()
                    except Exception as e:
                        logger.error(f"Error in bulk write for comments: {e}", exc_info=True)
                        # Don't clear on error - will retry on next flush
                        # Only clear if it's a non-retryable error
                        if not isinstance(e, BulkWriteError):
                            batch_comment_updates.clear()

            # Main processing loop
            try:
                for comment_doc in cursor:
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
                        for comment_info in current_batch_comments:
                            cid = comment_info["comment_id"]
                            cvid = comment_info["video_id"]
                            cchid = comment_info["channel_id"]

                            # Fetch replies with retry
                            replies = None
                            for retry in range(MAX_RETRIES):
                                try:
                                    replies = ye.get_replies(cid, cvid)
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
                                logger.debug(f"Fetched {len(replies)} replies for comment {cid}")

                            batch_comment_updates.append(
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

                        if comments_processed % 1000 == 0:
                            logger.info(
                                f"Progress: {comments_processed} comments processed, "
                                f"{total_replies_found} replies found, "
                                f"{total_replies_stored} replies stored"
                            )

                # Process remaining comments
                if current_batch_comments:
                    for comment_info in current_batch_comments:
                        cid = comment_info["comment_id"]
                        cvid = comment_info["video_id"]
                        cchid = comment_info["channel_id"]

                        replies = None
                        for retry in range(MAX_RETRIES):
                            try:
                                replies = ye.get_replies(cid, cvid)
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

                        batch_comment_updates.append(
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

            finally:
                cursor.close()

            logger.info(
                f"Processing complete: {comments_processed} comments processed, "
                f"{total_replies_found} replies found, "
                f"{total_replies_stored} replies stored in MongoDB"
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