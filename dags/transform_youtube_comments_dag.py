from airflow import DAG  # pyright: ignore[reportMissingImports]
from airflow.providers.standard.operators.python import PythonOperator  # pyright: ignore[reportMissingImports]
from airflow.providers.mongo.hooks.mongo import MongoHook  # pyright: ignore[reportMissingImports]
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook  # pyright: ignore[reportMissingImports]
import logging
from callbacks import task_failure_callback, task_success_callback
import os
import time
from datetime import datetime, timedelta
from pymongo import UpdateOne  # pyright: ignore[reportMissingImports]

# Get environment variables 
airflow_env = os.getenv("AIRFLOW_ENV", "development")

# Set up logging
logger = logging.getLogger("airflow.task")

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

def transform_comments_unified_task(**context):
    driver = None
    try:
        # MongoDB connection
        mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
        mongo_hook = MongoHook(mongo_conn_id=mongo_conn_id)
        mongo_client = mongo_hook.get_conn()
        db_name = "rbl" if airflow_env == "production" else "airflow_db"
        db = mongo_client[db_name]

        # Collections for comments and replies
        comments_collection = db.youtube_video_comments
        replies_collection = db.youtube_video_replies
        collections = [comments_collection, replies_collection]

        # Neo4j connection
        neo4j_conn_id = "neo4j_prod" if airflow_env == "production" else "neo4j_default"
        hook = Neo4jHook(conn_id=neo4j_conn_id)
        driver = hook.get_conn()

        # Create unique constraints if they don't exist
        with driver.session() as constraint_session:
            # Create unique constraint on comment_id for YouTubeComment nodes
            constraint_session.run("""
                CREATE CONSTRAINT youtube_comment_id_unique IF NOT EXISTS
                FOR (c:YouTubeComment) REQUIRE c.comment_id IS UNIQUE
            """)
            # Create unique constraint on video_id for YouTubeVideo nodes
            constraint_session.run("""
                CREATE CONSTRAINT youtube_video_id_unique IF NOT EXISTS
                FOR (v:YouTubeVideo) REQUIRE v.video_id IS UNIQUE
            """)
            logger.info("Unique constraints created/verified for YouTubeComment and YouTubeVideo nodes")

        # Batch processing configuration
        batch_size = 700
        max_write_retries = 3

        # Optimized Neo4j query - all operations in one transaction
        neo4j_query = """
        UNWIND $comments AS c
        MERGE (comment:YouTubeComment {comment_id: c.comment_id})
        SET
            comment.parent_id = coalesce(c.parent_id, "TOP_LEVEL"),
            comment.video_id = c.video_id,
            comment.channel_id = c.channel_id,
            comment.authorDisplayName = c.authorDisplayName,
            comment.authorProfileImageUrl = c.authorProfileImageUrl,
            comment.authorChannelUrl = c.authorChannelUrl,
            comment.authorChannelId = c.authorChannelId,
            comment.canReply = coalesce(c.canReply, false),
            comment.totalReplyCount = coalesce(c.totalReplyCount, 0),
            comment.textDisplay = c.textDisplay,
            comment.textOriginal = c.textOriginal,
            comment.canRate = c.canRate,
            comment.viewerRating = c.viewerRating,
            comment.likeCount = c.likeCount,
            comment.publishedAt = c.publishedAt,
            comment.updatedAt = c.updatedAt
        MERGE (v:YouTubeVideo {video_id: c.video_id})
        MERGE (v)-[r:HAS_COMMENT {platform: 'YouTube'}]->(comment)
        WITH comment, c
        WHERE c.parent_id IS NOT NULL
        MERGE (parent:YouTubeComment {comment_id: c.parent_id})
        MERGE (comment)-[:REPLY_TO_COMMENT]->(parent)
        """

        total_processed = 0
        total_batches = 0

        # Keep compatibility across Neo4j driver versions.
        def run_write(session_obj, tx_func):
            if hasattr(session_obj, "execute_write"):
                return session_obj.execute_write(tx_func)
            return session_obj.write_transaction(tx_func)

        # Reuse Neo4j session for all collections to reduce connection overhead
        with driver.session() as neo4j_session:
            for collection in collections:
                # Use MongoDB session for better consistency
                with mongo_client.start_session() as mongo_session:
                    # Include docs where the flag is missing, null, or false.
                    query = {"transformed_to_neo4j": {"$ne": True}}
                    # Only transform the "relevance" comment set from youtube_video_comments
                    if collection.name == comments_collection.name:
                        query["orderByParameter"] = "relevance"

                    cursor = collection.find(
                        query,
                        no_cursor_timeout=True,
                        session=mongo_session
                    ).batch_size(batch_size)

                    try:
                        batch_docs = []

                        for doc in cursor:
                            batch_docs.append(doc)

                            # Process when chunk reaches batch size.
                            if len(batch_docs) >= batch_size:
                                valid_docs = []
                                batch_comments = []
                                for d in batch_docs:
                                    comment_id = d.get("comment_id")
                                    video_id = d.get("video_id")
                                    if not comment_id or not video_id:
                                        logger.warning(
                                            "Skipping document with missing comment_id or video_id: %s",
                                            d.get("_id"),
                                        )
                                        continue
                                    valid_docs.append(d)
                                    batch_comments.append({
                                        "comment_id": comment_id,
                                        "parent_id": d.get("parent_id"),
                                        "video_id": video_id,
                                        "channel_id": d.get("channel_id"),
                                        "authorDisplayName": d.get("authorDisplayName"),
                                        "authorProfileImageUrl": d.get("authorProfileImageUrl"),
                                        "authorChannelUrl": d.get("authorChannelUrl"),
                                        "authorChannelId": d.get("authorChannelId"),
                                        "canReply": d.get("canReply"),
                                        "totalReplyCount": d.get("totalReplyCount"),
                                        "textDisplay": d.get("textDisplay"),
                                        "textOriginal": d.get("textOriginal"),
                                        "canRate": d.get("canRate"),
                                        "viewerRating": d.get("viewerRating"),
                                        "likeCount": d.get("likeCount"),
                                        "publishedAt": d.get("publishedAt"),
                                        "updatedAt": d.get("updatedAt")
                                    })

                                if batch_comments:
                                    last_error = None
                                    for attempt in range(1, max_write_retries + 1):
                                        try:
                                            run_write(neo4j_session, lambda tx: tx.run(neo4j_query, comments=batch_comments))
                                            break
                                        except Exception as e:
                                            last_error = e
                                            logger.warning(
                                                "Neo4j chunk write failed (attempt %d/%d): %s",
                                                attempt,
                                                max_write_retries,
                                                e,
                                            )
                                            if attempt < max_write_retries:
                                                time.sleep(min(2 ** attempt, 8))
                                    if last_error and attempt == max_write_retries:
                                        raise RuntimeError(
                                            f"Neo4j chunk write failed after {max_write_retries} attempts"
                                        ) from last_error

                                    # Mark the same successfully written docs in Mongo.
                                    mongo_ops = [
                                        UpdateOne(
                                            {"_id": d["_id"]},
                                            {"$set": {"transformed_to_neo4j": True}},
                                        )
                                        for d in valid_docs
                                    ]
                                    if mongo_ops:
                                        collection.bulk_write(mongo_ops, ordered=False, session=mongo_session)

                                    total_processed += len(batch_comments)
                                    total_batches += 1
                                    logger.info("Processed %d comments/replies so far...", total_processed)

                                batch_docs = []

                        # Process remaining docs in the final chunk.
                        if batch_docs:
                            valid_docs = []
                            batch_comments = []
                            for d in batch_docs:
                                comment_id = d.get("comment_id")
                                video_id = d.get("video_id")
                                if not comment_id or not video_id:
                                    logger.warning(
                                        "Skipping document with missing comment_id or video_id: %s",
                                        d.get("_id"),
                                    )
                                    continue
                                valid_docs.append(d)
                                batch_comments.append({
                                    "comment_id": comment_id,
                                    "parent_id": d.get("parent_id"),
                                    "video_id": video_id,
                                    "channel_id": d.get("channel_id"),
                                    "authorDisplayName": d.get("authorDisplayName"),
                                    "authorProfileImageUrl": d.get("authorProfileImageUrl"),
                                    "authorChannelUrl": d.get("authorChannelUrl"),
                                    "authorChannelId": d.get("authorChannelId"),
                                    "canReply": d.get("canReply"),
                                    "totalReplyCount": d.get("totalReplyCount"),
                                    "textDisplay": d.get("textDisplay"),
                                    "textOriginal": d.get("textOriginal"),
                                    "canRate": d.get("canRate"),
                                    "viewerRating": d.get("viewerRating"),
                                    "likeCount": d.get("likeCount"),
                                    "publishedAt": d.get("publishedAt"),
                                    "updatedAt": d.get("updatedAt")
                                })

                            if batch_comments:
                                last_error = None
                                for attempt in range(1, max_write_retries + 1):
                                    try:
                                        run_write(neo4j_session, lambda tx: tx.run(neo4j_query, comments=batch_comments))
                                        break
                                    except Exception as e:
                                        last_error = e
                                        logger.warning(
                                            "Neo4j final chunk write failed (attempt %d/%d): %s",
                                            attempt,
                                            max_write_retries,
                                            e,
                                        )
                                        if attempt < max_write_retries:
                                            time.sleep(min(2 ** attempt, 8))
                                if last_error and attempt == max_write_retries:
                                    raise RuntimeError(
                                        f"Neo4j final chunk write failed after {max_write_retries} attempts"
                                    ) from last_error

                                mongo_ops = [
                                    UpdateOne(
                                        {"_id": d["_id"]},
                                        {"$set": {"transformed_to_neo4j": True}},
                                    )
                                    for d in valid_docs
                                ]
                                if mongo_ops:
                                    collection.bulk_write(mongo_ops, ordered=False, session=mongo_session)
                                total_processed += len(batch_comments)
                                total_batches += 1

                    finally:
                        cursor.close()

        if total_processed == 0:
            logger.info("No comments/replies to process")
            return

        logger.info(f"Successfully transformed {total_processed} comments/replies in {total_batches} batches to Neo4j")

    except Exception as e:
        logger.error(f"Error in transforming comments to Neo4j: {e}", exc_info=True)
        raise
    finally:
        # Close Neo4j driver
        if driver:
            driver.close()

# DAG definition
with DAG(
    default_args=default_args,
    dag_id="youtube_comments_to_neo4j",
    description= 'A DAG to transform YouTube comments and replies to Neo4j',
    schedule=None,
    start_date=datetime(2025, 1, 19),
    catchup=False,
    tags=['youtube_comments_to_neo4j'],
) as dag:

    transform_comments_task = PythonOperator(
        task_id="transform_comments_unified",
        python_callable=transform_comments_unified_task,
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

    transform_comments_task
