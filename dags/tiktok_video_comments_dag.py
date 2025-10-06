from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from callbacks import task_failure_callback, task_success_callback
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne
import logging
import tiktok_etl as te
from time import sleep
from requests.exceptions import HTTPError
import os
# Set up logging
logger = logging.getLogger("airflow.task")

local_tz = pendulum.timezone("Europe/Amsterdam")

# Get environment variables 
airflow_env = os.getenv("AIRFLOW_ENV", "development")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_callback,
    "on_success_callback": task_success_callback,
}

with DAG(
    "tiktok_video_comments_dag",
    default_args=default_args,
    description="DAG to fetch and store TikTok video comments",
    schedule="0 16 * * *",
    start_date=pendulum.datetime(2025, 2, 13, tz=local_tz),
    catchup=False,
    tags=['tiktok_comments'],
) as dag:

    def fetch_and_store_comments(**context):
        # Choose the connection ID based on your environment (development or production)
        mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
        hook = MongoHook(mongo_conn_id=mongo_conn_id)   
        client = hook.get_conn()
        # Dynamically choose the database based on the environment
        db_name = "rbl" if airflow_env == "production" else "airflow_db"
        db = client[db_name]  # Use the appropriate database based on environment
        videos_collection = db.tiktok_user_video
        comments_collection = db.tiktok_video_comments

        # Create indexes
        comments_collection.create_index("id", unique=True)
        comments_collection.create_index("video_id")
        
        try:        
            # Get only videos that don't have comments fetched yet
            video_documents = videos_collection.find(
                {"comments_fetched": {"$ne": True}},
                {"video_id": 1, "username": 1, "_id": 0},
                no_cursor_timeout=True
            )
            
            videos_processed = 0
            new_comments_count = 0
            wait_time = 2 # Initial wait time between requests

            for video_doc in video_documents:
                video_id = video_doc.get("video_id")
                username = video_doc.get("username")  

                if not video_id or not username:
                    continue

                # Add delay between requests
                if videos_processed > 0:
                    logger.info(f"Waiting {wait_time} seconds before next request...")
                    sleep(wait_time)

                logger.info(f"Fetching comments for video: {video_id}")
                
                try:
                    # Get comments for the video
                    comments = te.tiktok_get_video_comments(video_id)
                    
                    if comments:
                        for comment in comments:
                            comment["username"] = username
                            comment["fetched_at"] = datetime.now()
                            comment["transformed_to_neo4j"] = False
                        try:
                            # Insert comments with ordered=False to continue on duplicate key errors
                            result = comments_collection.insert_many(comments, ordered=False)
                            new_comments = len(result.inserted_ids)
                            new_comments_count += new_comments
                            logger.info(f"Stored {new_comments} new comments for video {video_id}")

                        except BulkWriteError as bwe:
                            successful_inserts = len(comments) - len(bwe.details.get('writeErrors', []))
                            new_comments_count += successful_inserts
                            logger.info(f"Stored {successful_inserts} new comments for video {video_id} (some were duplicates)")
                           
                    else:
                        logger.info(f"No comments found for video {video_id}")
                    
                    # Mark video as processed
                    videos_collection.update_one(
                        {"video_id": video_id},
                        {
                            "$set": {
                                "comments_fetched": True,
                                "comments_fetched_at": datetime.now(),
                                "comments_count": len(comments) if comments else 0
                            }
                        }
                    )
                    videos_processed += 1
                    wait_time = 2  # Reset wait time after success

                except HTTPError as e:
                    if e.response.status_code == 429:  # Rate limit hit
                        logger.warning(f"Global rate limit hit while processing video {video_id}. Stopping fetch early.")
                        break
                    else:
                        logger.error(f"Error processing comments for video {video_id}: {e}", exc_info=True)
                        continue

                except Exception as e:
                    logger.error(f"Error processing comments for video {video_id}: {e}", exc_info=True)
                    continue

            logger.info(f"Processed {videos_processed} videos, fetched {new_comments_count} new comments total")

            # Store stats in XCom
            context['task_instance'].xcom_push(key='comments_stats', value={
                'videos_processed': videos_processed,
                'new_comments_count': new_comments_count
            })

        except Exception as e:
            logger.error(f"Error in fetch_and_store_comments: {e}", exc_info=True)
            raise

        finally:
            # Close the cursor
            video_documents.close()

    def transform_comments_to_graph(**context):
        mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
        mongo_hook = MongoHook(mongo_conn_id=mongo_conn_id)
        mongo_client = mongo_hook.get_conn()
        db_name = "rbl" if airflow_env == "production" else "airflow_db"
        db = mongo_client[db_name]
        comments_collection = db.tiktok_video_comments

        neo4j_conn_id = "neo4j_prod" if airflow_env == "production" else "neo4j_default"
        neo4j_hook = Neo4jHook(conn_id=neo4j_conn_id)
        driver = neo4j_hook.get_conn()

        batch_size = 500
        comments_processed = 0

        # --- Explicit MongoDB session ---
        with mongo_client.start_session() as session:
            comment_cursor = comments_collection.find(
                {"transformed_to_neo4j": False},
                no_cursor_timeout=True,
                session=session
            ).batch_size(batch_size)

            logger.info("Starting comment transformation from MongoDB to Neo4j")

            try:
                batch_comments = []
                for comment in comment_cursor:
                    batch_comments.append(comment)

                    if len(batch_comments) >= batch_size:
                        comments_processed += process_comment_batch(batch_comments, driver, comments_collection)
                        batch_comments = []

                if batch_comments:
                    comments_processed += process_comment_batch(batch_comments, driver, comments_collection)

                if comments_processed == 0:
                    logger.info(f"No comments to process")
                    return
                
                logger.info(f"Successfully processed {comments_processed} comments to Neo4j")

            except Exception as e:
                logger.error(f"Error in transform_comments_to_graph: {e}", exc_info=True)
                raise

            finally:
                comment_cursor.close()
    
    def process_comment_batch(batch_comments, driver, comments_collection):
        """
        Inserts a batch of top-level comments into Neo4j and marks them as processed in MongoDB
        """
        if not batch_comments:
            return 0
        
        neo4j_batch = []
        bulk_ops = []

        for comment in batch_comments:
            neo4j_batch.append({
                "comment_id": comment.get("id"),
                "video_id": comment.get("video_id"),
                "text": comment.get("text"),
                "like_count": comment.get("like_count"),
                "reply_count": comment.get("reply_count"),
                "create_time": comment.get("create_time"),
                "username": comment.get("username")
            })

            # Prepare bulk update for MongoDB
            bulk_ops.append(
                UpdateOne({"_id": comment["_id"]}, {"$set": {"transformed_to_neo4j": True}})
            )

        # Insert batch into Neo4j
        with driver.session() as session:
            session.run("""
            UNWIND $comments AS c
            MERGE (comment:TikTokComment {comment_id: c.comment_id})
            ON CREATE SET
                comment.text = c.text,
                comment.like_count = c.like_count,
                comment.reply_count = c.reply_count,
                comment.create_time = datetime(c.create_time),
                comment.username = c.username,
                comment.video_id = c.video_id

            MERGE (v:TikTokVideo {video_id: c.video_id})
            MERGE (comment)-[:POSTED_ON_TIKTOK_VIDEO]->(v)
            """, comments=neo4j_batch)

        # Update MongoDB as processed in bulk
        if bulk_ops:
            comments_collection.bulk_write(bulk_ops)

        logger.info(f"Processed {len(batch_comments)} comments to Neo4j")
        return len(batch_comments)

    #Define tasks
    fetch_and_store_comments_task = PythonOperator(
        task_id='fetch_and_store_comments',
        python_callable=fetch_and_store_comments,
    )

    transform_comments_to_graph_task = PythonOperator(
        task_id='transform_comments_to_graph',
        python_callable=transform_comments_to_graph,
    )

    # Set task dependencies
    fetch_and_store_comments_task >> transform_comments_to_graph_task 
    #transform_comments_to_graph_task 