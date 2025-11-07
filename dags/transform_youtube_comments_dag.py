from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
import logging
from callbacks import task_failure_callback, task_success_callback
import os
from datetime import datetime, timedelta
from pymongo import UpdateOne

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
        collections = [db.youtube_video_comments, db.youtube_video_replies]

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
        batch_size = 25
        mongo_update_batch_size = 50

        # Optimized Neo4j query - all operations in one transaction
        neo4j_query = """
        UNWIND $comments AS c
        MERGE (comment:YouTubeComment {comment_id: c.comment_id})
        SET
            comment.parent_id = c.parent_id,
            comment.video_id = c.video_id,
            comment.channel_id = c.channel_id,
            comment.authorDisplayName = c.authorDisplayName,
            comment.authorProfileImageUrl = c.authorProfileImageUrl,
            comment.authorChannelUrl = c.authorChannelUrl,
            comment.authorChannelId = c.authorChannelId,
            comment.canReply = c.canReply,
            comment.totalReplyCount = c.totalReplyCount,
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

        # Reuse Neo4j session for all collections to reduce connection overhead
        with driver.session() as neo4j_session:
            for collection in collections:
                # Use MongoDB session for better consistency
                with mongo_client.start_session() as mongo_session:
                    cursor = collection.find(
                        {"transformed_to_neo4j": False},
                        no_cursor_timeout=True,
                        session=mongo_session
                    ).batch_size(batch_size)

                    try:
                        batch_comments = []
                        mongo_bulk_ops = []

                        for doc in cursor:
                            try:
                                comment_id = doc.get("comment_id")
                                video_id = doc.get("video_id")

                                # Validate required fields
                                if not comment_id or not video_id:
                                    logger.warning(f"Skipping document with missing comment_id or video_id: {doc.get('_id')}")
                                    continue

                                # Prepare comment data for batch processing
                                batch_comments.append({
                                    "comment_id": comment_id,
                                    "parent_id": doc.get("parent_id"),
                                    "video_id": video_id,
                                    "channel_id": doc.get("channel_id"),
                                    "authorDisplayName": doc.get("authorDisplayName"),
                                    "authorProfileImageUrl": doc.get("authorProfileImageUrl"),
                                    "authorChannelUrl": doc.get("authorChannelUrl"),
                                    "authorChannelId": doc.get("authorChannelId"),
                                    "canReply": doc.get("canReply"),
                                    "totalReplyCount": doc.get("totalReplyCount"),
                                    "textDisplay": doc.get("textDisplay"),
                                    "textOriginal": doc.get("textOriginal"),
                                    "canRate": doc.get("canRate"),
                                    "viewerRating": doc.get("viewerRating"),
                                    "likeCount": doc.get("likeCount"),
                                    "publishedAt": doc.get("publishedAt"),
                                    "updatedAt": doc.get("updatedAt")
                                })

                                # Prepare MongoDB bulk update
                                mongo_bulk_ops.append(
                                    UpdateOne(
                                        {"comment_id": comment_id},
                                        {"$set": {"transformed_to_neo4j": True}}
                                    )
                                )

                                # Process batch when it reaches the batch size
                                if len(batch_comments) >= batch_size:
                                    # Insert batch into Neo4j using transaction
                                    neo4j_session.write_transaction(
                                        lambda tx: tx.run(neo4j_query, comments=batch_comments)
                                    )

                                    total_processed += len(batch_comments)
                                    total_batches += 1
                                    batch_comments = []

                                    # Batch MongoDB updates to reduce write operations
                                    if len(mongo_bulk_ops) >= mongo_update_batch_size:
                                        collection.bulk_write(mongo_bulk_ops, session=mongo_session)
                                        mongo_bulk_ops = []
                                        logger.info(f"Processed {total_processed} comments/replies so far...")

                            except Exception as e:
                                logger.error(f"Error transforming comment {doc.get('comment_id')}: {e}", exc_info=True)
                                continue

                        # Process remaining batch
                        if batch_comments:
                            neo4j_session.write_transaction(
                                lambda tx: tx.run(neo4j_query, comments=batch_comments)
                            )
                            total_processed += len(batch_comments)
                            total_batches += 1

                        # Final MongoDB bulk update for any remaining operations
                        if mongo_bulk_ops:
                            collection.bulk_write(mongo_bulk_ops, session=mongo_session)

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
