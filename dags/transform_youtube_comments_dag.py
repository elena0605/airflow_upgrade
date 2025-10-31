from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
import logging
from callbacks import task_failure_callback, task_success_callback
import os
from datetime import datetime, timedelta

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

        with driver.session() as session:
            total_processed = 0

            for collection in collections:
                # Use MongoDB session for better consistency
                with mongo_client.start_session() as mongo_session:
                    cursor = collection.find(
                        {"transformed_to_neo4j": False},
                        no_cursor_timeout=True,
                        session=mongo_session
                    )

                    try:
                        for doc in cursor:
                            try:
                                comment_id = doc.get("comment_id")
                                video_id = doc.get("video_id")

                                # Validate required fields
                                if not comment_id or not video_id:
                                    logger.warning(f"Skipping document with missing comment_id or video_id: {doc.get('_id')}")
                                    continue

                                # Merge comment node (removed redundant comment_id SET)
                                session.run(
                                    """
                                    MERGE (c:YouTubeComment {comment_id: $comment_id})
                                    SET
                                        c.parent_id = $parent_id,
                                        c.video_id = $video_id,
                                        c.channel_id = $channel_id,
                                        c.authorDisplayName = $authorDisplayName,
                                        c.authorProfileImageUrl = $authorProfileImageUrl,
                                        c.authorChannelUrl = $authorChannelUrl,
                                        c.authorChannelId = $authorChannelId,
                                        c.canReply = $canReply,
                                        c.totalReplyCount = $totalReplyCount,
                                        c.textDisplay = $textDisplay,
                                        c.textOriginal = $textOriginal,
                                        c.canRate = $canRate,
                                        c.viewerRating = $viewerRating,
                                        c.likeCount = $likeCount,
                                        c.publishedAt = $publishedAt,
                                        c.updatedAt = $updatedAt
                                    """,
                                    comment_id=comment_id,
                                    parent_id=doc.get("parent_id"),
                                    video_id=video_id,
                                    channel_id=doc.get("channel_id"),
                                    authorDisplayName=doc.get("authorDisplayName"),
                                    authorProfileImageUrl=doc.get("authorProfileImageUrl"),
                                    authorChannelUrl=doc.get("authorChannelUrl"),
                                    authorChannelId=doc.get("authorChannelId"),
                                    canReply=doc.get("canReply"),
                                    totalReplyCount=doc.get("totalReplyCount"),
                                    textDisplay=doc.get("textDisplay"),
                                    textOriginal=doc.get("textOriginal"),
                                    canRate=doc.get("canRate"),
                                    viewerRating=doc.get("viewerRating"),
                                    likeCount=doc.get("likeCount"),
                                    publishedAt=doc.get("publishedAt"),
                                    updatedAt=doc.get("updatedAt")
                                )

                                # Link comment to video
                                session.run(
                                    """
                                    MERGE (v:YouTubeVideo {video_id: $video_id})
                                    MERGE (c:YouTubeComment {comment_id: $comment_id})
                                    MERGE (v)-[r:HAS_COMMENT {platform: 'YouTube'}]->(c)
                                    """,
                                    video_id=video_id,
                                    comment_id=comment_id
                                )

                                # If it's a reply, link to parent comment
                                parent_id = doc.get("parent_id")
                                if parent_id:
                                    session.run(
                                        """
                                        MERGE (parent:YouTubeComment {comment_id: $parent_id})
                                        MERGE (c:YouTubeComment {comment_id: $comment_id})
                                        MERGE (c)-[:REPLY_TO_COMMENT]->(parent)
                                        """,
                                        parent_id=parent_id,
                                        comment_id=comment_id
                                    )

                                # Mark as transformed
                                collection.update_one(
                                    {"comment_id": comment_id},
                                    {"$set": {"transformed_to_neo4j": True}},
                                    session=mongo_session
                                )
                                total_processed += 1

                            except Exception as e:
                                logger.error(f"Error transforming comment {doc.get('comment_id')}: {e}", exc_info=True)
                                continue

                    finally:
                        cursor.close()

        logger.info(f"Successfully transformed {total_processed} comments/replies to Neo4j")

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
