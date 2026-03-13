"""
DAG to analyze TikTok video comments using AI and update Neo4j with:
- Frequent topics with frequency-based weights
- Comment summary description

For each TikTok video in Neo4j, this DAG:
1. Retrieves all comments connected by video_id
2. Analyzes comments using AI (handles multiple languages)
3. Extracts frequent topics with weights
4. Generates a summary description
5. Updates the TikTokVideo node in Neo4j
"""

from __future__ import annotations
from datetime import datetime, timedelta
import pendulum  # pyright: ignore[reportMissingImports]
import os
import json
import logging
from typing import List, Dict, Any, Optional

from airflow import DAG  # pyright: ignore[reportMissingImports]
from airflow.providers.standard.operators.python import PythonOperator  # pyright: ignore[reportMissingImports]
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook  # pyright: ignore[reportMissingImports]
from callbacks import task_failure_callback, task_success_callback
from openai import OpenAI  # pyright: ignore[reportMissingImports]
from openai import AzureOpenAI  # pyright: ignore[reportMissingImports]

# Set up logging
logger = logging.getLogger("airflow.task")

local_tz = pendulum.timezone("Europe/Amsterdam")

# Get environment variables
airflow_env = os.getenv("AIRFLOW_ENV", "development")
neo4j_conn_id = "neo4j_prod" if airflow_env == "production" else "neo4j_default"

# AI Configuration
MAX_COMMENTS_PER_VIDEO = 1000  # Limit comments to avoid token limits

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": task_failure_callback,
    "on_success_callback": task_success_callback,
}

with DAG(
    "tiktok_comments_analysis_dag",
    default_args=default_args,
    description="Analyze TikTok video comments with AI and extract topics + summary",
    schedule=None,  # Manual-only: will not run on a schedule
    start_date=pendulum.datetime(2025, 1, 15, tz=local_tz),
    catchup=False,
    tags=['tiktok', 'comments', 'ai_analysis', 'neo4j'],
    params={
        "limit": None,  # Process all videos (set to a number to limit)
        "offset": 0,    # Start from beginning
        "video_ids": None,  # Specific video IDs to process (comma-separated string)
        "skip_analyzed": True  # Skip videos that already have analysis (set to False to re-analyze)
    }
) as dag:

    def get_openai_client():
        """
        Get OpenAI or Azure OpenAI client from environment variables.

        Azure env vars expected from `.env` (docker compose `env_file`):
        - AZURE_OPENAI_ENDPOINT
        - AZURE_OPENAI_API_KEY
        - AZURE_OPENAI_API_VERSION (optional; default set below)
        - AZURE_OPENAI_MODEL (chat deployment name)
        """
        # Azure first (only if fully configured)
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        azure_api_key = os.getenv("AZURE_OPENAI_API_KEY")
        azure_api_version = os.getenv("AZURE_OPENAI_API_VERSION") or "2024-10-21"
        azure_chat_deployment = os.getenv("AZURE_OPENAI_MODEL")  # deployment name on Azure

        if azure_endpoint and azure_api_key and azure_chat_deployment:
            logger.info("Using Azure OpenAI client")
            return AzureOpenAI(
                api_key=azure_api_key,
                api_version=azure_api_version,
                azure_endpoint=azure_endpoint,
            )

        if any([azure_endpoint, azure_api_key, azure_chat_deployment]) and not all(
            [azure_endpoint, azure_api_key, azure_chat_deployment]
        ):
            logger.warning(
                "Azure OpenAI is partially configured. "
                "Expected AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY, AZURE_OPENAI_MODEL. "
                "Falling back to regular OpenAI."
            )

        # Regular OpenAI fallback
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("Missing OPENAI_API_KEY (and Azure OpenAI not fully configured).")
        logger.info("Using regular OpenAI client")
        return OpenAI(api_key=api_key)


    def get_neo4j_driver():
        """Get Neo4j driver using Neo4jHook based on environment."""
        neo4j_hook = Neo4jHook(conn_id=neo4j_conn_id)
        return neo4j_hook.get_conn()

    def get_videos_with_comments(driver, limit: Optional[int] = None, offset: int = 0, 
                                 skip_analyzed: bool = False):
        """
        Query Neo4j for TikTok videos that have comments.
        Returns list of video dictionaries with video_id and comment count.
        """
        if skip_analyzed:
            query = """
            MATCH (v:TikTokVideo)-[:HAS_COMMENT]->(c:TikTokComment)
            WHERE (v.comment_summary_description IS NULL OR v.comment_summary_description = '')
            WITH v, count(c) as comment_count
            RETURN v.video_id as video_id, comment_count
            ORDER BY v.video_id
            """
        else:
            query = """
            MATCH (v:TikTokVideo)-[:HAS_COMMENT]->(c:TikTokComment)
            WITH v, count(c) as comment_count
            RETURN v.video_id as video_id, comment_count
            ORDER BY v.video_id
            """
        
        if limit:
            query += f" SKIP {offset} LIMIT {limit}"
        
        with driver.session() as session:
            result = session.run(query)
            videos = [{"video_id": record["video_id"], "comment_count": record["comment_count"]} 
                      for record in result]
            return videos

    def get_comments_for_video(driver, video_id: str) -> List[Dict[str, Any]]:
        """
        Retrieve all comments for a specific video from Neo4j.
        Returns list of comment dictionaries with text and metadata.
        """
        query = """
        MATCH (v:TikTokVideo {video_id: $video_id})-[:HAS_COMMENT]->(c:TikTokComment)
        RETURN c.text as text, c.comment_id as comment_id, 
               c.like_count as like_count, c.username as username,
               c.create_time as create_time
        ORDER BY c.create_time DESC
        LIMIT $limit
        """
        
        with driver.session() as session:
            result = session.run(query, video_id=video_id, limit=MAX_COMMENTS_PER_VIDEO)
            comments = []
            for record in result:
                comment_text = record.get("text", "")
                if comment_text and comment_text.strip():  # Only include non-empty comments
                    comments.append({
                        "text": comment_text.strip(),
                        "comment_id": record.get("comment_id"),
                        "like_count": record.get("like_count", 0),
                        "username": record.get("username"),
                        "create_time": str(record.get("create_time", ""))
                    })
            return comments

    def analyze_comments_with_ai(client, comments: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Use AI to analyze comments and extract:
        1. Frequent topics with frequency-based weights
        2. Comment summary description
        
        Handles multiple languages automatically.
        """
        if not comments:
            return {
                "topics": [],
                "summary": "No comments available for analysis."
            }
        
        # Combine all comment texts
        comment_texts = [c["text"] for c in comments]
        combined_text = "\n\n".join(comment_texts[:MAX_COMMENTS_PER_VIDEO])
        
        # Truncate if too long (to avoid token limits)
        max_chars = 50000  # Conservative limit
        if len(combined_text) > max_chars:
            combined_text = combined_text[:max_chars]
            logger.warning(f"Truncated comments text to {max_chars} characters")
        
        # Create prompt for analysis
        prompt = f"""Analyze the following TikTok video comments and provide:

1. **Frequent Topics**: Extract the most frequently discussed topics/themes. For each topic, provide:
   - The topic name (in English, even if comments are in other languages)
   - A weight (0.0 to 1.0) based on how frequently it appears relative to other topics
   - Topics should be specific and meaningful (avoid generic terms like "comment" or "video")

2. **Summary**: Write a comprehensive summary description of what the comments are discussing, 
   the overall sentiment, key themes, and main points raised by viewers. The summary should be 
   in English and be 2-4 sentences long.

The comments may be in different languages - analyze them in their original language but 
provide the output in English.

Comments to analyze:
{combined_text}

Return your response as a JSON object with this exact structure:
{{
    "topics": [
        {{"topic": "topic name", "weight": 0.85}},
        {{"topic": "another topic", "weight": 0.72}},
        ...
    ],
    "summary": "A comprehensive summary description of the comments..."
}}

Only return the JSON object, no additional text."""

        try:
            # Determine if using Azure or regular OpenAI
            model = "gpt-4o-mini"  # Default model
            try:
                # IMPORTANT (Azure): this must be the *deployment name*, not the base model name.
                azure_model = (
                    os.getenv("AZURE_OPENAI_MODEL")
                    or os.getenv("AZURE_OPENAI_CHAT_MODEL")
                    or os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT")
                    or os.getenv("OPENAI_MODEL")
                )
                if azure_model:
                    model = azure_model
            except:
                pass
            
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are an expert at analyzing social media comments and extracting meaningful insights. Always respond with valid JSON only."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )
            
            result_text = response.choices[0].message.content
            result = json.loads(result_text)
            
            # Validate and normalize topics
            topics = result.get("topics", [])
            if topics:
                # Normalize weights to ensure they sum to a reasonable range
                total_weight = sum(t.get("weight", 0) for t in topics)
                if total_weight > 0:
                    # Normalize so top topics have higher weights
                    max_weight = max(t.get("weight", 0) for t in topics)
                    if max_weight > 0:
                        for topic in topics:
                            topic["weight"] = round(topic["weight"] / max_weight, 3)
            
            return {
                "topics": topics[:20],  # Limit to top 20 topics
                "summary": result.get("summary", "Unable to generate summary.")
            }
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse AI response as JSON: {e}")
            logger.error(f"Response was: {result_text[:500]}")
            return {
                "topics": [],
                # Empty summary => upstream will NOT write anything to Neo4j
                "summary": ""
            }
        except Exception as e:
            logger.error(f"Error calling AI for comment analysis: {e}")
            if "DeploymentNotFound" in str(e):
                logger.error(
                    "Azure OpenAI deployment not found. In Azure OpenAI, the `model=` parameter must be "
                    "your *deployment name*. Set an Airflow Variable like `AZURE_OPENAI_MODEL` (chat deployment) "
                    "and `AZURE_OPENAI_EMBEDDING_MODEL` (embedding deployment) to those deployment names."
                )
            return {
                "topics": [],
                # Empty summary => upstream will NOT write anything to Neo4j
                "summary": ""
            }

    def update_video_in_neo4j(driver, video_id: str, topics: List[Dict[str, Any]], 
                              summary: str):
        """
        Update the TikTokVideo node in Neo4j with analyzed comment data.
        """
        # Neo4j properties cannot store maps/dicts (or list of maps). Persist as:
        # - list of topic strings
        # - list of weights (floats) aligned by index
        # - JSON string as a convenience for downstream consumers
        topic_names: List[str] = []
        topic_weights: List[float] = []
        for t in (topics or []):
            if not isinstance(t, dict):
                continue
            name = t.get("topic")
            weight = t.get("weight")
            if isinstance(name, str) and name.strip():
                topic_names.append(name.strip())
                try:
                    topic_weights.append(float(weight))
                except Exception:
                    topic_weights.append(0.0)

        topics_json = json.dumps(
            [{"topic": n, "weight": w} for n, w in zip(topic_names, topic_weights)],
            ensure_ascii=False,
        )

        query = """
        MATCH (v:TikTokVideo {video_id: $video_id})
        SET v.comments_frequent_topics = $topic_names,
            v.comments_frequent_topic_weights = $topic_weights,
            v.comments_frequent_topics_json = $topics_json,
            v.comment_summary_description = $summary,
            v.comments_analyzed_at = datetime()

        WITH v, $topic_names AS topic_names, $topic_weights AS topic_weights
        // Keep relationships in sync with the latest analysis run
        OPTIONAL MATCH (v)-[old:HAS_COMMENT_TOPIC]->(:Topic)
        DELETE old

        WITH v, topic_names, topic_weights
        CALL (v, topic_names, topic_weights) {
          UNWIND range(0, size(topic_names) - 1) AS i
          WITH
            v,
            topic_names[i] AS topic,
            topic_weights[i] AS weight
          MERGE (t:Topic {name: topic})
          MERGE (v)-[r:HAS_COMMENT_TOPIC]->(t)
          SET r.weight = weight,
              r.platform = "TikTok",
              r.source = "tiktok_comments",
              r.analyzed_at = v.comments_analyzed_at
          RETURN count(*) AS _
        }

        RETURN v.video_id as video_id
        """
        
        try:
            with driver.session() as session:
                result = session.run(
                    query,
                    video_id=video_id,
                    topic_names=topic_names,
                    topic_weights=topic_weights,
                    topics_json=topics_json,
                    summary=summary
                )
                record = result.single()
                if record:
                    logger.info(f"Successfully updated video {video_id} with comment analysis")
                    return True
                else:
                    logger.warning(f"Video {video_id} not found in Neo4j")
                    return False
        except Exception as e:
            logger.error(f"Error updating video {video_id} in Neo4j: {e}")
            return False

    def process_video(driver, client, video_id: str) -> bool:
        """
        Process a single video: get comments, analyze, and update Neo4j.
        Returns True if successful, False otherwise.
        """
        try:
            logger.info(f"Processing video: {video_id}")
            
            # Get comments
            comments = get_comments_for_video(driver, video_id)
            if not comments:
                logger.warning(f"No comments found for video {video_id}")
                return False
            
            logger.info(f"Found {len(comments)} comments for video {video_id}")
            
            # Analyze comments with AI
            analysis = analyze_comments_with_ai(client, comments)
            
            if not analysis.get("summary"):
                logger.warning(f"Failed to generate analysis for video {video_id}")
                return False
            
            # Update Neo4j
            success = update_video_in_neo4j(
                driver,
                video_id,
                analysis["topics"],
                analysis["summary"]
            )
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing video {video_id}: {e}", exc_info=True)
            return False

    def analyze_tiktok_comments_task(**context):
        """
        Main task function to analyze TikTok comments.
        """
        logger.info("Starting TikTok comment analysis")
        logger.info(f"Environment: {airflow_env}")
        logger.info(f"Neo4j connection: {neo4j_conn_id}")
        
        # Get parameters from context
        params = context.get("params", {})
        limit = params.get("limit", None)
        offset = params.get("offset", 0)
        video_ids = params.get("video_ids", None)
        skip_analyzed = params.get("skip_analyzed", True)  # Default to True to avoid re-processing
        
        # Convert video_ids string to list if provided
        if video_ids and isinstance(video_ids, str):
            video_ids = [vid.strip() for vid in video_ids.split(",") if vid.strip()]
        
        logger.info(f"Skip already analyzed: {skip_analyzed}")
        
        # Initialize clients
        try:
            driver = get_neo4j_driver()
            client = get_openai_client()
        except Exception as e:
            logger.error(f"Failed to initialize clients: {e}")
            return
        
        try:
            # Get videos to process
            if video_ids:
                videos = [{"video_id": vid, "comment_count": 0} for vid in video_ids]
                logger.info(f"Processing {len(videos)} specific videos")
            else:
                videos = get_videos_with_comments(driver, limit=limit, offset=offset, 
                                                skip_analyzed=skip_analyzed)
                logger.info(f"Found {len(videos)} videos with comments to process")
            
            if not videos:
                logger.info("No videos found to process")
                return
            
            # Process videos
            successful = 0
            failed = 0
            
            for i, video in enumerate(videos, 1):
                video_id = video["video_id"]
                logger.info(f"Processing video {i}/{len(videos)}: {video_id}")
                
                if process_video(driver, client, video_id):
                    successful += 1
                else:
                    failed += 1
                
                # Log progress every 10 videos
                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{len(videos)} videos processed ({successful} successful, {failed} failed)")
            
            logger.info(f"Analysis complete: {successful} successful, {failed} failed out of {len(videos)} videos")
            
        except Exception as e:
            logger.error(f"Error in main processing: {e}", exc_info=True)
        finally:
            driver.close()

    analyze_comments = PythonOperator(
        task_id="analyze_tiktok_comments",
        python_callable=analyze_tiktok_comments_task,
    )

    analyze_comments

