from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from callbacks import task_failure_callback, task_success_callback
import logging
from pymongo.errors import DuplicateKeyError
import system as sy
import youtube_etl as ye
from airflow.exceptions import AirflowFailException
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
    "youtube_channel_stats_dag",
     default_args=default_args,
     description= 'A DAG to fetch, store, and transform YouTube channel statistics',
     schedule=None,
     start_date=datetime(2025, 1, 14),
     catchup=False,
     tags=['youtube_channel_stats'],

) as dag:
        def load_channels_ids(file_path, **context):
            try:
                logger.info(f"Attempting to load channels from file: {file_path}")
                channels_ids = sy.read_channel_ids_from_csv(file_path)
                logger.info(f"Successfully loaded channels ids from {file_path}")
                logger.info(f"Number of channels loaded: {len(channels_ids)}")
                logger.info(f"Sample channels: {dict(list(channels_ids.items())[:3])}")  # Show first 3
                context['ti'].xcom_push(key='channels_ids', value=channels_ids)
                logger.info(f"Successfully pushed to XCom with key 'channels_ids'")
                return channels_ids  # Add return statement
            except Exception as e:
                logger.error(f"Error while loading channles_id: {e}")
                raise

        def fetch_and_store_channel_stats(**context):
             # Choose the connection ID based on your environment (development or production)
             mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
             hook = MongoHook(mongo_conn_id=mongo_conn_id)
             client = hook.get_conn()
             # Dynamically choose the database based on the environment
             db_name = "rbl" if airflow_env == "production" else "airflow_db"
             db = client[db_name]  # Use the appropriate database based on environment
             collection = db.youtube_channel_stats

             collection.create_index("channel_id", unique=True )

             logger.info(f"Attempting to pull 'channels_ids' from XCom...")
             
             # Pull from the specific task that pushed the data
             channels_ids = context['ti'].xcom_pull(
                 task_ids='load_channels_ids',
                 key='channels_ids'
             )
             logger.info(f"channels_ids pulled from XCom: {channels_ids}")
             logger.info(f"Type of channels_ids: {type(channels_ids)}")
             if channels_ids:
                 logger.info(f"Number of channels in XCom: {len(channels_ids)}")
             else:
                 logger.info("channels_ids is None or empty")

             if not channels_ids:
               logger.warning("No channels_ids found, skipping data fetch and storage.")
               return

             new_channel_ids = []  # Track new channels 
             
             for username in channels_ids:
                try:
                    channel_id = channels_ids[username]
                    logger.info(f"Fetching channel stats for channel with username: {username} and channel_id: {channel_id}")
                    channel_stats = ye.get_channels_statistics(channel_id)

                    if channel_stats is None:
                       raise AirflowFailException(f"Failed to fetch statistics for channel ID: {channel_id}") 

                    channel_stats['transformed_to_neo4j'] = False
                    channel_stats['timestamp'] = datetime.now() 
                    channel_stats['username'] = username

                    try:
                        collection.insert_one(channel_stats)
                        new_channel_ids.append(channel_id)  # Track successful insert
                        logger.info(f"Stored new channel: {username}")

                    except DuplicateKeyError as e: 
                        # Check if it needs transformation
                        existing_doc = collection.find_one(
                            {"channel_id": channel_id, "transformed_to_neo4j": False}
                        )
                        if existing_doc:
                            new_channel_ids.append(channel_id)
                            logger.info(f"Channel {username} exists but needs transformation")    
                        else:
                            logger.info(f"Channel {username} already exists, skipping")

                except Exception as e:
                    logger.error(f"Error processing {username}: {e}")
                    raise e
             # Pass new channels to transform task
             context['task_instance'].xcom_push(key='new_channel_ids', value=new_channel_ids)
             logger.info(f"Stored {len(new_channel_ids)} new channels")

        def transform_to_graph(**context):
            # Get new channel IDs from previous task
             new_channel_ids = context['task_instance'].xcom_pull(
                task_ids='fetch_and_store_channel_stats',
                key='new_channel_ids'
             )
             
             if not new_channel_ids:
                logger.info("No new channels to transform")
                return
             logger.info(f"Transforming {len(new_channel_ids)} new channels to graph") 
          
             # Choose MongoDB connection based on environment
             mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
             mongo_hook = MongoHook(mongo_conn_id=mongo_conn_id)
             mongo_client = mongo_hook.get_conn()
             # Choose database based on environment
             db_name = "rbl" if airflow_env == "production" else "airflow_db"
             db = mongo_client[db_name]
             collection = db.youtube_channel_stats

             # Choose Neo4j connection based on environment
             neo4j_conn_id = "neo4j_prod" if airflow_env == "production" else "neo4j_default"
             hook = Neo4jHook(conn_id=neo4j_conn_id) 
             driver = hook.get_conn()
             
             
             with driver.session(database="neo4j") as session:
                 # Only fetch new channels from MongoDB
                 documents = collection.find({
                    "channel_id": {"$in": new_channel_ids},
                    "transformed_to_neo4j": False  # Only get untransformed channels
                 })

                 channels_processed = 0

                 for doc in documents:
                  try:  
                    topic_categories = doc.get("topic_categories", [])
                
                    session.run(
                        """
                        MERGE(c:YouTubeChannel {channel_id: $channel_id})
                        SET c.channel_id = $channel_id,
                            c.title = $title,
                            c.view_count = $view_count,
                            c.subscriber_count = $subscriber_count,
                            c.video_count = $video_count,
                            c.hidden_subscriber_count = $hidden_subscriber_count,
                            c.description = $description,
                            c.keywords = $keywords,
                            c.country = $country,
                            c.topic_categories = $topics,
                            c.username = $username
                        """,
                        channel_id = doc.get("channel_id"),
                        title = doc.get("title"),
                        view_count = doc.get("view_count", 0),
                        subscriber_count = doc.get("subscriber_count", 0),
                        video_count = doc.get("video_count", 0),
                        hidden_subscriber_count = doc.get("hidden_subscriber_count", False),
                        description = doc.get("description", ""),
                        keywords = doc.get("keywords", []),
                        country = doc.get("country", "Unknown"),
                        topics = doc.get("topic_categories", []),
                        username = doc.get("username", "")
                    )
                    # Mark as transformed in MongoDB
                    collection.update_one(
                       {"channel_id": doc.get("channel_id")},
                       {"$set": {"transformed_to_neo4j": True}}
                    )
                    channels_processed += 1
                    logger.info(f"Transformed channel {doc.get('channel_id')} to Neo4j")

                  except Exception as e:
                    logger.error(f"Error transforming channel {doc.get('channel_id')}: {e}")
                    raise

                 logger.info(f"Successfully processed {channels_processed} channels to Neo4j")  

        load_channels_ids_task = PythonOperator(
            task_id = 'load_channels_ids',
            python_callable=load_channels_ids,
            op_kwargs={'file_path': INPUT_PATH},
        )

        fetch_and_store_channel_stats_task = PythonOperator(
            task_id = 'fetch_and_store_channel_stats',
            python_callable = fetch_and_store_channel_stats,
        )

        transform_to_graph_task = PythonOperator(
            task_id="transform_to_graph",
            python_callable=transform_to_graph,
        )

        load_channels_ids_task >> fetch_and_store_channel_stats_task >> transform_to_graph_task

     

