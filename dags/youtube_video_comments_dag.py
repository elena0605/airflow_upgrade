from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from callbacks import task_failure_callback, task_success_callback
from airflow.exceptions import AirflowFailException
import logging
import requests
from pymongo.errors import BulkWriteError
import youtube_etl as ye
import os

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

with DAG(
    "youtube_video_comments",
     default_args=default_args,
     description= 'A DAG to fetch, store, and transform YouTube video comments',
     schedule=None,
     start_date=datetime(2025, 1, 19),
     catchup=False,
     tags=['youtube_video_comments'],

) as dag:
        def fetch_and_store_video_comments(**context):
          cursor = None
          try:  
            # Choose the connection ID based on your environment (development or production)
            mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
            hook = MongoHook(mongo_conn_id=mongo_conn_id)
            client = hook.get_conn()
            # Dynamically choose the database based on the environment
            db_name = "rbl" if airflow_env == "production" else "airflow_db"
            db = client[db_name]  # Use the appropriate database based on environment
            
            video_collection = db.youtube_channel_videos
            comment_collection = db.youtube_video_comments

            comment_collection.create_index("comment_id", unique=True)
            comment_collection.create_index("video_id")  # For faster lookups
            comment_collection.create_index("transformed_to_neo4j") 
            

            # Use MongoDB session for better consistency and no_cursor_timeout to prevent cursor expiration
            with client.start_session() as session:
                # Get videos that haven't had comments fetched yet
                cursor = video_collection.find(
                  {"comments_fetched": {"$ne": True}},
                  {"video_id": 1, "channel_id": 1, "_id": 0},
                  no_cursor_timeout=True,
                  session=session
                ).batch_size(50)  # Process in batches to avoid memory issues

                videos_processed = 0
                new_comment_ids = []         

                for video_doc in cursor:
                    video_id = video_doc.get("video_id")
                    channel_id = video_doc.get("channel_id")

                    if not video_id:
                        continue

                    logger.info(f"Fetching comments for video_id: {video_id}")

                    try:
                        comments = ye.get_top_level_comments(video_id)

                        if comments:
                            for comment in comments:
                                comment["fetched_at"] = datetime.now()
                                comment["transformed_to_neo4j"] = False                    
                            try:
                                result = comment_collection.insert_many(comments,ordered=False,session=session) 
                                new_comment_ids.extend([c['comment_id'] for c in comments])
                                logger.info(f"Stored {len(comments)} new comments for video {video_id}")
                                
                            except BulkWriteError as e:
                                for comment in comments:
                                    try:
                                        comment_collection.update_one(
                                            {"comment_id": comment['comment_id']},
                                            {"$setOnInsert": {
                                                "transformed_to_neo4j": False,
                                                "channel_id": channel_id
                                            }},
                                            upsert=True,
                                            session=session
                                        )
                                        existing = comment_collection.find_one(
                                            {"comment_id": comment['comment_id'],
                                            "transformed_to_neo4j": False},
                                            session=session
                                        )
                                        if existing:
                                            new_comment_ids.append(comment['comment_id'])
                                    except Exception as e:
                                        logger.error(f"Error checking comment {comment.get('comment_id')}: {e}")
                                        continue                                          
                        # Mark video as processed
                        video_collection.update_one(
                            {"video_id": video_id},
                            {
                                "$set": {
                                    "comments_fetched": True,
                                    "comments_fetched_at": datetime.now(),
                                    "comments_count": len(comments) if comments else 0
                                }
                            },
                            session=session
                        )
                        videos_processed += 1

                    except Exception as e:
                        logger.error(f"Error processing video {video_id}: {e}")
                        continue

                logger.info(f"Processed {videos_processed} videos, found {len(new_comment_ids)} comments")
                

          except Exception as e:
            logger.error(f"Error in fetch_and_store_comments: {e}", exc_info=True)
            raise
          finally:
            # Always close the cursor to prevent resource leaks
            if cursor is not None:
                try:
                    cursor.close()
                    logger.info("Cursor closed successfully")
                except Exception as e:
                    logger.warning(f"Error closing cursor: {e}")


        
        # def transform_to_graph(**context):
        #  try:    
        #     # Choose MongoDB connection based on environment
        #     mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
        #     hook = MongoHook(mongo_conn_id=mongo_conn_id)
        #     client = hook.get_conn()
        #     # Choose database based on environment
        #     db_name = "rbl" if airflow_env == "production" else "airflow_db"
        #     db = client[db_name]
        #     collection = db.youtube_video_comments           

        #     # Choose Neo4j connection based on environment
        #     neo4j_conn_id = "neo4j_prod" if airflow_env == "production" else "neo4j_default"
        #     hook = Neo4jHook(conn_id=neo4j_conn_id) 
        #     driver = hook.get_conn()

        #     with driver.session() as session:
        #         # Only fetch untransformed comments
        #         documents = collection.find({
                 
        #           "transformed_to_neo4j": False
        #         })            

        #         comments_processed = 0

        #         for doc in documents:
        #          try:  
        #             session.run(
        #                 """
        #                 MERGE(c:YouTubeVideoComment {comment_id: $comment_id})
        #                 MERGE(v:YouTubeVideo {video_id: $video_id})
        #                 SET
        #                   c.comment_id = $comment_id,
        #                   c.channel_id = $channel_id,
        #                   c.video_id = $video_id,
        #                   c.canReply = $canReply,
        #                   c.totalReplyCount = $totalReplyCount,
        #                   c.text = $text,
        #                   c.authorDisplayName = $authorDisplayName,
        #                   c.authorProfileImageUrl = $authorProfileImageUrl,
        #                   c.authorChannelUrl = $authorChannelUrl,
        #                   c.authorChannelId = $authorChannelId,
        #                   c.canRate = $canRate,
        #                   c.viewerRating = $viewerRating,
        #                   c.likeCount = $likeCount,
        #                   c.publishedAt =$publishedAt,
        #                   c.updatedAt = $updatedAt
        #                 MERGE (c)-[:COMMENT_ON_YOUTUBE_VIDEO]->(v)
        #                 """,
        #                 comment_id = doc.get("comment_id"),
        #                 channel_id = doc.get("channel_id"),
        #                 video_id = doc.get("video_id"),
        #                 canReply = doc.get("canReply"),
        #                 totalReplyCount = doc.get("totalReplyCount"),
        #                 text = doc.get("text"),
        #                 authorDisplayName = doc.get("authorDisplayName"),
        #                 authorProfileImageUrl = doc.get("authorProfileImageUrl"),
        #                 authorChannelUrl = doc.get("authorChannelUrl"),
        #                 authorChannelId = doc.get("authorChannelId"),
        #                 canRate = doc.get("canRate"),
        #                 viewerRating = doc.get("viewerRating"),
        #                 likeCount = doc.get("likeCount"),
        #                 publishedAt = doc.get("publishedAt"),
        #                 updatedAt = doc.get("updatedAt"),
        #             )
        #             # Mark as transformed in MongoDB
        #             collection.update_one(
        #                 {"comment_id": doc["comment_id"]},
        #                 {"$set": {"transformed_to_neo4j": True}}
        #             )
        #             comments_processed += 1
        #             logger.info(f"Transformed comment {doc.get('comment_id')} for video {doc.get('video_id')}")

        #          except Exception as e:
        #             logger.error(f"Error transforming comment {doc.get('comment_id')}: {e}")
        #             continue

        #     logger.info(f"Successfully transformed {comments_processed} comments to Neo4j")
        #  except Exception as e:  
        #             logger.error(f"Error in transforming comments to Neo4j: {e}")
        #             raise

        fetch_and_store_video_comments_task = PythonOperator(
            task_id = 'fetch_and_store_video_comments',
            python_callable = fetch_and_store_video_comments,
        )

        # transform_to_graph_task = PythonOperator(
        #     task_id = 'transform_to_graph',
        #     python_callable = transform_to_graph,
        # )

        # fetch_and_store_video_comments_task >> transform_to_graph_task
        fetch_and_store_video_comments_task