from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from callbacks import task_failure_callback, task_success_callback
from airflow.exceptions import AirflowFailException
import logging
import requests
from pymongo.errors import DuplicateKeyError
import youtube_etl as ye

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
    "youtube_video_captions",
     default_args=default_args,
     description='A DAG to fetch, store, and transform YouTube video captions',
     schedule=None,
     start_date=datetime(2025, 2, 11),
     catchup=False,
     tags=['youtube_video_captions'],
) as dag:
    
    def fetch_and_store_video_captions():
        try:
            hook = MongoHook(mongo_conn_id="mongo_default")
            client = hook.get_conn()
            db = client.airflow_db
            
            video_collection = db.youtube_channel_videos
            caption_collection = db.youtube_video_captions

            # Create index for unique captions
            caption_collection.create_index([("caption_id", 1), ("video_id", 1)], unique=True)
            
            videos = video_collection.find({}, {"video_id": 1, "_id": 0})
            video_ids = [video["video_id"] for video in videos]

            logger.info(f"Found {len(video_ids)} videos to fetch captions for.")

            for video_id in video_ids:
                logger.info(f"Fetching captions for video_id: {video_id}")
                try:
                    caption = ye.get_captions(video_id)

                    if caption:
                        logger.info(f"Storing caption for video_id: {video_id}")
                        try:
                            caption_collection.insert_one(caption)
                            logger.info(f"Captions for video_id: {video_id} inserted into MongoDB successfully.")
                            
                        except DuplicateKeyError as e:
                            logger.info(f"Caption for video_id {video_id} already exist and was skipped. Error: {e}")
                    else:
                        logger.info(f"No caption found for video_id: {video_id}")

                except AirflowFailException as ae:
                    logger.error(f"Failure occurred while fetching video caption with video_id: {video_id}: {ae}")
                    raise
                except Exception as e:
                    logger.error(f"Unexpected error while processing video_id {video_id}: {e}")
                    raise

            logger.info("Finished fetching and storing captions for all videos.")

        except Exception as e:
            logger.error(f"Error in fetching video captions process: {e}")
            raise AirflowFailException(f"Failed to fetch and store video captions: {e}")

    def transform_to_graph():
        try:
            mongo_hook = MongoHook(mongo_conn_id="mongo_default")
            mongo_client = mongo_hook.get_conn()
            db = mongo_client.airflow_db
            collection = db.youtube_video_captions

            logger.info("Fetching captions from MongoDB...")

            hook = Neo4jHook(conn_id="neo4j_default")
            driver = hook.get_conn()

            with driver.session() as session:
                documents = collection.find({})

                for doc in documents:
                    logger.debug(f"Processing caption: {doc.get('caption_id')}")
                    session.run(
                        """
                        MERGE(c:YouTubeVideoCaption {caption_id: $caption_id})
                        MERGE(v:YouTubeVideo {video_id: $video_id})
                        SET
                          c.caption_id = $caption_id,
                          c.video_id = $video_id,
                          c.language = $language,
                          c.language_name = $language_name,
                          c.track_kind = $track_kind,
                          c.is_auto = $is_auto,
                          c.is_draft = $is_draft,
                          c.caption_content = $caption_content,
                          c.fetched_time = $fetched_time
                          MERGE (c)-[:CAPTIONOFVIDEO]->(v)
                        """,
                        caption_id=doc.get("caption_id"),
                        video_id=doc.get("video_id"),
                        language=doc.get("language"),
                        language_name=doc.get("language_name"),
                        track_kind=doc.get("track_kind"),
                        is_auto=doc.get("is_auto"),
                        is_draft=doc.get("is_draft"),
                        caption_content=doc.get("caption_content"),
                        fetched_time=doc.get("fetched_time")
                    )
                    logger.info(f"Processed caption: {doc.get('caption_id')} for video: {doc.get('video_id')}")

        except Exception as e:
            logger.error(f"Error in transforming captions to Neo4j: {e}")
            raise

    fetch_and_store_video_captions_task = PythonOperator(
        task_id='fetch_and_store_video_captions',
        python_callable=fetch_and_store_video_captions,
    )

    transform_to_graph_task = PythonOperator(
        task_id='transform_to_graph',
        python_callable=transform_to_graph,
    )

    fetch_and_store_video_captions_task >> transform_to_graph_task