from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from pymongo.errors import DuplicateKeyError
from callbacks import task_failure_callback, task_success_callback
import os
import logging
import system as sy
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


INPUT_PATH = "/opt/airflow/dags/influencers.csv"
OUTPUT_DIR = "/opt/airflow/dags/data/tiktok"
os.makedirs(OUTPUT_DIR, exist_ok=True)


with DAG(
    "tiktok_dag",
    default_args=default_args,
    description="A simple DAG to fetch TikTok user info",
    schedule=None,
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=['tiktok_user_info'],
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

    def fetch_all_user_data(**context):
        logger.info("Fetching data for all usernames...")
        usernames = context['ti'].xcom_pull(
            task_ids='load_usernames',
            key='usernames'
        )
        logger.info(f"Usernames pulled from XCom: {usernames}")

        if not usernames:
            logger.warning("No usernames found, skipping data fetch.")
            return    
        
        fetched_data = {}
        for username in usernames:
            logger.info(f"Fetching data for username: {username}")
            try:
                user_data = te.tiktok_get_user_info(username=username, output_dir=OUTPUT_DIR, **context)
                if user_data is not None and not user_data.empty:
                    # Convert DataFrame to dict and store in fetched_data
                    user_dict = user_data.iloc[0].to_dict()
                    fetched_data[username] = user_dict
                    logger.info(f"Successfully fetched data for username: {username}")
                else:
                    logger.warning(f"No data returned for username: {username}")
            except Exception as e:
                logger.error(f"Error fetching data for username {username}: {e}", exc_info=True)
                continue  # Continue with other usernames instead of failing the entire task
        
        # Push all fetched data to XCom as a single object
        if fetched_data:
            context['ti'].xcom_push(key='fetched_user_data', value=fetched_data)
            logger.info(f"Pushed {len(fetched_data)} user records to XCom")
        else:
            logger.warning("No user data was successfully fetched")
    
    def store_user_data(**context):
        # Choose the connection ID based on your environment (development or production)
        mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
        hook = MongoHook(mongo_conn_id=mongo_conn_id)
        client = hook.get_conn()
        # Dynamically choose the database based on the environment
        db_name = "rbl" if airflow_env == "production" else "airflow_db"
        db = client[db_name]  # Use the appropriate database based on environment
        collection = db.tiktok_user_info

        # Track new users
        new_usernames = []

        collection.create_index("username", unique=True)

        # Get usernames from previous task
        usernames = context['ti'].xcom_pull(task_ids='load_usernames', key='usernames')
        logger.info(f"Processing {len(usernames)} users")
        logger.info(f"AIRFLOW_ENV is set to: {airflow_env}")

    
        if not usernames:
           logger.warning("No usernames found, skipping data storage.")
           return

        # Get all fetched user data from XCom
        fetched_data = context['ti'].xcom_pull(
            task_ids='fetch_all_user_data',
            key='fetched_user_data'
        )
        
        if not fetched_data:
            logger.warning("No fetched user data found in XCom, skipping data storage.")
            return
        
        logger.info(f"Retrieved {len(fetched_data)} user records from XCom")
        
        for username in usernames:
            clean_username = username.strip()
            # Retrieve the fetched data for this username from the fetched_data dict
            user_data = fetched_data.get(clean_username)

            if user_data:
                try:
                    # Prepare the data to be inserted into MongoDB
                    user_data["username"] = clean_username 
                    user_data["timestamp"] = datetime.now()
                    user_data["transformed_to_neo4j"] = False

                    # Insert data into MongoDB
                    try:
                        collection.insert_one(user_data)
                        new_usernames.append(username)
                        logger.info(f"New user stored: {username}")

                    except DuplicateKeyError as e:
                        logger.info(f"User Info for username: {username} already exist. Skipping insertion. Error: {e}")               
                    
                except Exception as e:
                    logger.error(f"Error inserting data into MongoDB for {username}: {e}", exc_info=True)
                    continue  # Continue with other users instead of failing the entire task
            else:
                logger.warning(f"No data found for username {username}, skipping insertion.")

        context['task_instance'].xcom_push(key='new_usernames', value=new_usernames)
        logger.info(f"Stored {len(new_usernames)} new users")

    def transform_to_graph(**context):
     
     # Choose MongoDB connection based on environment
     mongo_conn_id = "mongo_prod" if airflow_env == "production" else "mongo_default"
     mongo_hook = MongoHook(mongo_conn_id=mongo_conn_id)
     mongo_client = mongo_hook.get_conn()
     # Choose database based on environment
     db_name = "rbl" if airflow_env == "production" else "airflow_db"
     db = mongo_client[db_name]
     collection = db.tiktok_user_info

    # Choose Neo4j connection based on environment
     neo4j_conn_id = "neo4j_prod" if airflow_env == "production" else "neo4j_default"
     hook = Neo4jHook(conn_id=neo4j_conn_id) 
     driver = hook.get_conn() 
     with driver.session() as session:
       
        # Fetch all user data from MongoDB
        documents = collection.find({"transformed_to_neo4j": False})
        for doc in documents:
            # Create or update User nodes in Neo4j
            session.run(
                """
                MERGE (u:TikTokUser {username: $username})
                SET u.display_name = $display_name,
                    u.bio_description = $bio_description,
                    u.bio_url = $bio_url,
                    u.avatar_url = $avatar_url,
                    u.follower_count = $follower_count,
                    u.following_count = $following_count,
                    u.likes_count = $likes_count,
                    u.video_count = $video_count,
                    u.is_verified = $is_verified   
                """,
                username=doc.get("username"),
                display_name=doc.get("display_name"),
                bio_description=doc.get("bio_description"),
                bio_url=doc.get("bio_url"),
                avatar_url=doc.get("avatar_url"),
                follower_count=doc.get("follower_count", 0),
                following_count=doc.get("following_count", 0),
                likes_count=doc.get("likes_count", 0),
                video_count=doc.get("video_count", 0),
                is_verified=doc.get("is_verified", False),
            )
            collection.update_one(
                {"username": doc.get("username")},
                {"$set": {"transformed_to_neo4j": True}}
            )   
        

    load_usernames_task = PythonOperator(
        task_id='load_usernames',
        python_callable=load_usernames,
        op_kwargs={'file_path': INPUT_PATH},
    )

    fetch_user_info_task = PythonOperator(
        task_id='fetch_all_user_data',
        python_callable=fetch_all_user_data,
    )

    store_user_data_task = PythonOperator(
        task_id= 'store_user_data',
        python_callable= store_user_data,
    )

    transform_to_graph_task = PythonOperator(
    task_id="transform_to_graph",
    python_callable=transform_to_graph,
    )

    load_usernames_task >> fetch_user_info_task >> store_user_data_task >> transform_to_graph_task