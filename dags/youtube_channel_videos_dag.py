from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from callbacks import task_failure_callback, task_success_callback
from airflow.exceptions import AirflowFailException
import logging
import system as sy
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

# Files and directories paths
INPUT_PATH = "/opt/airflow/dags/youtube_influencers.csv"

with DAG(
    "youtube_channel_videos",
     default_args=default_args,
     description= 'A DAG to fetch, store, and transform YouTube channel videos',
     schedule=None,
     start_date=datetime(2025, 1, 15),
     catchup=False,
     tags=['youtube_channel_videos'],

) as dag:
        def load_channels_ids(file_path, **context):
            try:
                channels_ids = sy.read_channel_ids_from_csv(file_path)
                logger.info(f"Successfully loaded channels ids from {file_path}")
                logger.info(f"Usernames loaded: {channels_ids}")
                context['ti'].xcom_push(key='channels_ids', value=channels_ids)
                return channels_ids 
            except Exception as e:
                logger.error(f"Error while loading channles_id: {e}")
                raise

        def fetch_and_store_channel_videos(**context):
            # Choose the connection ID based on your environment (development or production)
            mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
            hook = MongoHook(mongo_conn_id=mongo_conn_id)
            client = hook.get_conn()
            # Dynamically choose the database based on the environment
            db_name = "rbl" if airflow_env == "production" else "airflow_db"
            db = client[db_name]  # Use the appropriate database based on environment
            collection = db.youtube_channel_videos           
        
            collection.create_index("video_id", unique=True)

            # Pull from the specific task that pushed the data
            channels_ids = context['ti'].xcom_pull(
                 task_ids='load_channels_ids',
                 key='channels_ids'
            )
            logger.info(f"channels_ids pulled from XCom: {channels_ids}")

            if not channels_ids:
               logger.warning("No channels_ids found, skipping data fetch and storage.")
               return

            new_video_ids = []

            start_date = "2023-01-01T00:00:00Z"
            end_date = "2023-12-31T23:59:59Z"

            for username, channel_id in channels_ids.items():
                logger.info(f"Fetching videos for {username} (channel_id: {channel_id})")

                try:
                    videos = ye.get_videos_by_date(channel_id, start_date, end_date)
                    if videos:
                        for video in videos:
                            video['transformed_to_neo4j'] = False  # Add transformation flag
                            video['timestamp'] = datetime.now()
                            
                        try:
                            result = collection.insert_many(videos, ordered=False)
                            new_video_ids.extend([v['video_id'] for v in videos])  # Track new videos
                            logger.info(f"Videos for {username} with channel_id: {channel_id} inserted into MongoDB successfully.")

                        except BulkWriteError as bwe:
                         # Check which videos need transformation even if they exist
                         for video in videos:
                          try:
                            collection.update_one(
                                {"video_id": video['video_id']},
                                {"$setOnInsert": {"transformed_to_neo4j": False}},
                                upsert=True
                            )
                            # If video exists but needs transformation, add to new_video_ids
                            existing = collection.find_one({
                                "video_id": video['video_id'],
                                "transformed_to_neo4j": False
                            })
                            if existing:
                                new_video_ids.append(video['video_id'])

                          except Exception as e:
                            logger.error(f"Error checking video {video.get('video_id')}: {e}")
                            continue

                        logger.info(f"Some videos already exist for {username}, checking transformation status")    
                    else:
                        logger.info(f"No videos found for {username} (channel_id: {channel_id}) in 2024.")

                except Exception as e:
                    logger.error(f"Error processing videos for {username}: {e}")
                    raise
        
            context['task_instance'].xcom_push(key='new_video_ids', value=new_video_ids)
            logger.info(f"Found {len(new_video_ids)} videos to process")

        def transform_to_graph(**context):
            new_video_ids = context['task_instance'].xcom_pull(
                task_ids='fetch_and_store_channel_videos',
                key='new_video_ids'
            )
            if not new_video_ids:
                logger.info("No new videos to transform")
                return

            logger.info(f"Transforming {len(new_video_ids)} new videos to graph")

            # Choose MongoDB connection based on environment
            mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
            mongo_hook = MongoHook(mongo_conn_id=mongo_conn_id)
            mongo_client = mongo_hook.get_conn()

            # Choose database based on environment
            db_name = "rbl" if airflow_env == "production" else "airflow_db"
            db = mongo_client[db_name]
            collection = db.youtube_channel_videos

            # Choose Neo4j connection based on environment
            neo4j_conn_id = "neo4j_prod" if airflow_env == "production" else "neo4j_default"
            hook = Neo4jHook(conn_id=neo4j_conn_id) 
            driver = hook.get_conn()

            with driver.session() as session:
                documents = collection.find({
                    "video_id": {"$in": new_video_ids},
                    "transformed_to_neo4j": False  # Only get untransformed videos
                })

                videos_processed = 0
                for doc in documents:
                 try:
                    thumbnail_ref = doc.get("thumbnails", {}).get("gridfs_id")
                    thumbnail_gridfs_id = str(thumbnail_ref) if thumbnail_ref else None
                    # Convert string counts to integers with default 0
                    view_count = int(doc.get("view_count", 0))
                    like_count = int(doc.get("like_count", 0))
                    comment_count = int(doc.get("comment_count", 0))
                    session.run(
                        """
                        MERGE(c:YouTubeChannel {channel_id: $channel_id})
                        MERGE(v:YouTubeVideo {video_id: $video_id})
                        SET
                           v.video_title = $video_title,
                           v.video_id = $video_id,
                           v.published_at = $published_at,
                           v.channel_id = $channel_id,
                           v.video_description = $video_description,
                           v.channel_title = $channel_title,
                           v.thumbnail_gridfs_id = $thumbnail_gridfs_id,
                           v.view_count = $view_count,
                           v.like_count = $like_count,
                           v.comment_count = $comment_count,
                           v.topic_categories = $topic_categories,
                           v.tags = $tags
                        MERGE (c)-[:PUBLISHED_ON_YOUTUBE]->(v)
                        """,
                        video_title = doc.get("video_title"),
                        video_id = doc.get("video_id"),
                        published_at = doc.get("published_at"),
                        channel_id = doc.get("channel_id"),
                        video_description = doc.get("video_description", ""),
                        channel_title = doc.get("channel_title", ""),
                        thumbnail_gridfs_id = thumbnail_gridfs_id,
                        view_count = view_count,
                        like_count = like_count,
                        comment_count = comment_count,
                        topic_categories = doc.get("topic_categories", []),
                        tags = doc.get("tags", [])
                    )

                    tags = doc.get("tags", [])
                    if tags:
                      for tag in tags:
                        session.run("""
                            MERGE (t:Tag {name: $tag_name})
                            WITH t
                            MATCH (v:YouTubeVideo {video_id: $video_id})
                            MERGE (v)-[:HAS_TAG]->(t)
                        """, {
                            'video_id': doc['video_id'],
                            'tag_name': tag
                        }) 
                       # Mark as transformed in MongoDB
                    collection.update_one(
                       {"video_id": doc["video_id"]},
                       {"$set": {"transformed_to_neo4j": True}}
                    ) 
                    videos_processed += 1
                    logger.info(f"Transformed video {doc['video_id']} to Neo4j")
                 except Exception as e:
                       logger.error(f"Error transforming video {doc.get('video_id')}: {e}")
                       continue

            logger.info(f"Successfully transformed {videos_processed} videos to Neo4j") 
         



        load_channels_ids_task = PythonOperator(
          task_id = 'load_channels_ids',
          python_callable=load_channels_ids,
          op_kwargs={'file_path': INPUT_PATH},
        )
        
        fetch_and_store_channel_videos_task = PythonOperator(
            task_id = 'fetch_and_store_channel_videos',
            python_callable = fetch_and_store_channel_videos,
        )

        transform_to_graph_task = PythonOperator(
            task_id = 'transform_to_graph',
            python_callable = transform_to_graph,
        )

        load_channels_ids_task >> fetch_and_store_channel_videos_task >> transform_to_graph_task