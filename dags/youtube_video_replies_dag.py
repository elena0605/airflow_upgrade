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

# Set up logging
logger = logging.getLogger("airflow.task")

# Get environment variables 
airflow_env = os.getenv("AIRFLOW_ENV", "development")

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
     description= 'A DAG to fetch, store, and transform YouTube video replies',
     schedule=None,
     start_date=datetime(2025, 1, 30),
     catchup=False,
     tags=['youtube_video_replies'],

) as dag:
        def fetch_and_store_video_replies(**context):
            try:
                # Choose the connection ID based on your environment (development or production)
                mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
                hook = MongoHook(mongo_conn_id=mongo_conn_id)
                client = hook.get_conn()
                # Dynamically choose the database based on the environment
                db_name = "rbl" if airflow_env == "production" else "airflow_db"
                db = client[db_name]  # Use the appropriate database based on environment

                comment_collection = db.youtube_video_comments
                replies_collection = db.youtube_video_replies

                # Create indexes
                replies_collection.create_index("comment_id", unique=True)
                replies_collection.create_index("parent_id")  # For faster lookups
                replies_collection.create_index("transformed_to_neo4j")  # For transformation queries

                # Get comments that haven't had replies fetched yet
                comment_documents = comment_collection.find(
                    {"replies_fetched": {"$ne": True}},
                    {"comment_id": 1, "channel_id": 1, "video_id": 1, "_id": 0}
                )

                comments_processed = 0
                new_reply_ids = []

                for comment_doc in comment_documents:
                    comment_id = comment_doc.get("comment_id")
                    channel_id = comment_doc.get("channel_id")
                    video_id = comment_doc.get("video_id")

                    if not comment_id:
                        continue

                    logger.info(f"Fetching replies for comment_id: {comment_id}")

                    try:
                        replies = ye.get_replies(comment_id, video_id)
                        
                        if replies:
                            for reply in replies:
                                
                                reply["fetched_at"] = datetime.now()
                                reply["transformed_to_neo4j"] = False

                            try:
                                result = replies_collection.insert_many(replies, ordered=False)
                                new_reply_ids.extend([r['comment_id'] for r in replies])
                                logger.info(f"Stored {len(replies)} new replies for comment {comment_id}")

                            except BulkWriteError as bwe:
                                for reply in replies:
                                    try:
                                        replies_collection.update_one(
                                            {"comment_id": reply['comment_id']},
                                            {"$setOnInsert": {
                                                "transformed_to_neo4j": False,
                                                "channel_id": channel_id
                                            }},
                                            upsert=True
                                        )
                                        existing = replies_collection.find_one({
                                            "comment_id": reply['comment_id'],
                                            "transformed_to_neo4j": False
                                        })
                                        if existing:
                                            new_reply_ids.append(reply['comment_id'])
                                    except Exception as e:
                                        logger.error(f"Error checking reply {reply.get('comment_id')}: {e}")
                                        continue

                                    logger.info(f"Some replies already exist for comment {comment_id}, checking transformation status")
                        else:
                            logger.info(f"No replies found for comment_id: {comment_id}")

                        # Mark comment as processed
                        comment_collection.update_one(
                            {"comment_id": comment_id},
                            {
                                "$set": {
                                    "replies_fetched": True,
                                    "replies_fetched_at": datetime.now(),
                                    "replies_count": len(replies) if replies else 0
                                }
                            }
                        )
                        comments_processed += 1

                    except Exception as e:
                        logger.error(f"Error processing replies for comment {comment_id}: {e}")
                        continue

                logger.info(f"Processed {comments_processed} comments, found {len(new_reply_ids)} replies")
                

            except Exception as e:
                logger.error(f"Error in fetch_and_store_replies: {e}")
                raise
        
        # def transform_to_graph(**context):
        #     try:
        #         mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
        #         mongo_hook = MongoHook(mongo_conn_id=mongo_conn_id)
        #         mongo_client = mongo_hook.get_conn()
        #         db_name = "rbl" if airflow_env == "production" else "airflow_db"
        #         db = mongo_client[db_name]
        #         collection = db.youtube_video_replies

        #         neo4j_conn_id = "neo4j_prod" if airflow_env == "production" else "neo4j_default"
        #         hook = Neo4jHook(conn_id=neo4j_conn_id)
        #         driver = hook.get_conn()

        #         with driver.session() as session:
        #             # Only fetch untransformed replies
        #             documents = collection.find({
                        
        #                 "transformed_to_neo4j": False
        #             })

        #             replies_processed = 0

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

        #                     # Mark as transformed in MongoDB
        #                     collection.update_one(
        #                         {"reply_id": doc["reply_id"]},
        #                         {"$set": {"transformed_to_neo4j": True}}
        #                     )

        #                     replies_processed += 1
        #                     logger.info(f"Transformed reply {doc.get('reply_id')} for comment {doc.get('parent_id')}")

        #                 except Exception as e:
        #                     logger.error(f"Error transforming reply {doc.get('reply_id')}: {e}")
        #                     continue

        #             logger.info(f"Successfully transformed {replies_processed} replies to Neo4j")

        #     except Exception as e:
        #         logger.error(f"Error in transforming replies to Neo4j: {e}")
        #         raise
           
        fetch_and_store_video_replies_task = PythonOperator(
            task_id = 'fetch_and_store_video_replies',
            python_callable = fetch_and_store_video_replies,
        )
        # transform_to_graph_task = PythonOperator(
        #     task_id = 'transform_to_graph',
        #     python_callable = transform_to_graph,
        # )

        # fetch_and_store_video_replies_task >> transform_to_graph_task
        fetch_and_store_video_replies_task