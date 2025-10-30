from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from callbacks import task_failure_callback, task_success_callback
from pymongo.errors import BulkWriteError
import os
import logging
import system as sy
import pandas as pd
import json
import tiktok_etl as te

# Set up logging
logger = logging.getLogger("airflow.task")

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

# File and directories paths
INPUT_PATH = "/opt/airflow/dags/influencers.csv"
OUTPUT_DIR = "/opt/airflow/dags/data/tiktok"
os.makedirs(OUTPUT_DIR, exist_ok=True)


with DAG(
    "tiktok_video_dag",
    default_args=default_args,
    description="A simple DAG to fetch TikTok user videos",
    schedule=None,
    start_date=datetime(2024, 12, 19),
    catchup=False,
    tags=['tiktok_videos'],
) as dag:

    def load_usernames(file_path, **context):
        try:
            usernames = sy.read_usernames_from_csv(file_path)
            clean_usernames = [username.strip() for username in usernames if username.strip()]
            logger.info(f"Successfully loaded {len(clean_usernames)} usernames from {file_path}")
            logger.info(f"Usernames loaded: {clean_usernames}")
            context['ti'].xcom_push(key='usernames', value=clean_usernames) 
            return clean_usernames
        except Exception as e:
            logger.error(f"Error while loading usernames: {e}")
            raise
    
    def fetch_and_store_user_video(**context):
     try:
        # Choose the connection ID based on your environment (development or production)
        mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
        hook = MongoHook(mongo_conn_id=mongo_conn_id)
        client = hook.get_conn()
        # Dynamically choose the database based on the environment
        db_name = "rbl" if airflow_env == "production" else "airflow_db"
        db = client[db_name]  # Use the appropriate database based on environment
        video_collection = db.tiktok_user_video
        video_collection.create_index("video_id", unique=True)
        
        usernames = context['ti'].xcom_pull(
            task_ids='load_usernames',
            key='usernames'
        )
        
        
        for username in usernames:             
            try:
                logger.info(f"Fetching data for username: {username}")
                user_data_list_df = te.tiktok_get_user_video_info(username=username)
                
                if user_data_list_df is None or user_data_list_df.empty:
                    logger.warning(f"No data found for username: {username}")
                    continue
                
                # Convert DataFrame to list of dictionaries
                videos = user_data_list_df.to_dict('records')
                
                # Ensure all videos have the transformed_to_neo4j flag
                for video in videos:
                    video['transformed_to_neo4j'] = False
                
                try:
                    # Insert many with ordered=False to continue on error
                    result = video_collection.insert_many(videos, ordered=False)
                    logger.info(f"Stored {len(result.inserted_ids)} videos for {username}")
                
                except BulkWriteError as bwe:                  
                    for video in videos:                  
                            result = video_collection.update_one(
                                {"video_id": video['video_id']},
                                {"$setOnInsert": {"transformed_to_neo4j": False}},
                                upsert=True
                            )
                             # Skip to next username
                    logger.info(f"Handled duplicate videos for {username}")

            except Exception as user_error:
                logger.error(f"Error processing username {username}: {str(user_error)}")
                continue
            
     except Exception as main_error:
        logger.error(f"Main process error: {str(main_error)}")
        raise


    def transform_to_graph(**context):       
        # Choose MongoDB connection based on environment
     mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
     mongo_hook = MongoHook(mongo_conn_id=mongo_conn_id)
     mongo_client = mongo_hook.get_conn()
     # Choose database based on environment
     db_name = "rbl" if airflow_env == "production" else "airflow_db"
     db = mongo_client[db_name]
     collection = db.tiktok_user_video

     # Use Neo4jHook to connect to Neo4j
     neo4j_conn_id = "neo4j_prod" if airflow_env == "production" else "neo4j_default"
     hook = Neo4jHook(conn_id=neo4j_conn_id) 
     driver = hook.get_conn() 
     with driver.session() as session:
        # Fetch new video documents from MongoDB
         documents = collection.find({"transformed_to_neo4j": False})
         for doc in documents:
            username = doc.get("username")
            video_id = doc.get("video_id")

            if not username or not video_id:
                logging.warning(f"Skipping invalid document with username {username} or video_id {video_id}.") 
                continue
            
            # Safe extraction from potentially None struct fields
            video_label = doc.get("video_label") or {}
            video_tag = doc.get("video_tag")

            if isinstance(video_tag, dict):
                video_tag_type = video_tag.get("video_tag_type", "")
            else:
                video_tag_type = ""

            hashtag_info_list = doc.get("hashtag_info_list") or []
            sticker_info_list = doc.get("sticker_info_list") or []    

            try:
                session.run(
                    """
                    MERGE (u:TikTokUser {username: $username})
                    MERGE (v:TikTokVideo {video_id: $video_id})
                    ON CREATE SET
                       v.video_description = $video_description,
                       v.create_time = $create_time,
                       v.region_code = $region_code,
                       v.share_count = $share_count,
                       v.view_count = $view_count,
                       v.like_count = $like_count,
                       v.comment_count = $comment_count,
                       v.music_id = $music_id,
                       v.voice_to_text = $voice_to_text,
                       v.is_stem_verified = $is_stem_verified,
                       v.video_duration = $video_duration, 
                       v.video_title = $video_title,
                       v.video_author_url = $video_author_url,
                       v.video_thumbnail_url = $video_thumbnail_url,
                       v.video_mention_list = $video_mention_list,
                       v.video_label_content = $video_label_content,
                       v.video_tag_type = $video_tag_type,
                       v.search_id = $search_id,
                       v.username = $username
                    MERGE (u)-[r:HAS_VIDEO]->(v)
                    SET r.platform = "TikTok"
                    """,
                    username=username,
                    video_id=video_id,
                    video_description=doc.get("video_description", ""),
                    create_time=doc.get("create_time", ""),
                    region_code=doc.get("region_code", ""),
                    share_count=doc.get("share_count", 0),
                    view_count=doc.get("view_count", 0),
                    like_count=doc.get("like_count", 0),
                    comment_count=doc.get("comment_count", 0),
                    music_id = str(doc.get("music_id", "")),
                    voice_to_text=doc.get("voice_to_text", ""),
                    is_stem_verified=doc.get("is_stem_verified", False),
                    video_duration=doc.get("video_duration", 0),
                    video_mention_list = doc.get("video_mention_list", []),
                    video_title = doc.get("video_title", ""),
                    video_author_url = doc.get("video_author_url", ""),
                    video_thumbnail_url = doc.get("video_thumbnail_url", ""),
                    video_label_content = video_label.get("content", ""),
                    video_tag_type = video_tag_type,
                    search_id = doc.get("search_id", "")                                                    
                )
                # Create Hashtag nodes and connect them to the video
                for hashtag in hashtag_info_list:
                    hashtag_id = str(hashtag.get("hashtag_id"))
                    hashtag_name = hashtag.get("hashtag_name")
                    hashtag_description = hashtag.get("hashtag_description")
                    if hashtag_id and hashtag_name:
                        session.run("""
                            MERGE (h:TikTokHashtag {id: $hashtag_id})
                            SET h.name = $hashtag_name,
                                h.description = $hashtag_description
                            WITH h
                            MATCH (v:TikTokVideo {video_id: $video_id})
                            MERGE (v)-[r:HAS_TAG]->(h)
                            SET r.platform = "TikTok"
                        """, {
                            "video_id": video_id,
                            "hashtag_id": hashtag_id,
                            "hashtag_name": hashtag_name,
                            "hashtag_description": hashtag_description
                        })
                for sticker in sticker_info_list:
                    sticker_id = str(sticker.get("sticker_id"))
                    sticker_name = sticker.get("sticker_name")
                    if sticker_id and sticker_name:
                        session.run("""
                            MERGE (s:TikTokSticker {id: $sticker_id, name: $sticker_name})
                            WITH s
                            MATCH (v:TikTokVideo {video_id: $video_id})
                            MERGE (v)-[:HAS_STICKER]->(s)
                        """, {
                            "video_id": video_id,
                            "sticker_id": sticker_id,
                            "sticker_name": sticker_name
                        })        
                collection.update_one(
                    {"video_id": video_id},
                    {"$set": {"transformed_to_neo4j": True}}
                )
                logging.info(f"Data for username {username} and video_id {video_id} stored in Neo4j successfully.")
            except Exception as e:
                logging.error(f"Error processing data for username {username}and video_id {video_id}: {e}", exc_info=True)     
      

    load_usernames_task = PythonOperator(
        task_id='load_usernames',
        python_callable=load_usernames,
        op_kwargs={'file_path': INPUT_PATH},
    )

    fetch_and_store_user_video_task = PythonOperator(
        task_id='fetch_and_store_user_video',
        python_callable=fetch_and_store_user_video,
    )

    transform_to_graph_task = PythonOperator(
    task_id="transform_to_graph",
    python_callable=transform_to_graph,
    )
   

    load_usernames_task >> fetch_and_store_user_video_task >> transform_to_graph_task   
    