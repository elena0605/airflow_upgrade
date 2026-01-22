"""
DAG to analyze YouTube video comments using AI and update Neo4j with:
- Frequent topics with frequency-based weights
- Comment summary description

Assumes comments are already in Neo4j (see `youtube_comments_to_neo4j`):
- (:YouTubeVideo)-[:HAS_COMMENT {platform: 'YouTube'}]->(:YouTubeComment)

Writes (stored on :YouTubeVideo):
- comments_frequent_topics (list[str])
- comments_frequent_topic_weights (list[float])
- comments_frequent_topics_json (json string)
- comment_summary_description (string)
- comments_analyzed_at (datetime)

Also maintains relationships:
- (v:YouTubeVideo)-[:HAS_COMMENT_TOPIC]->(t:Topic) with {weight, platform, source, analyzed_at}
"""

from __future__ import annotations

from datetime import timedelta
import json
import logging
import os
from typing import Any, Dict, List, Optional

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from callbacks import task_failure_callback, task_success_callback
from openai import OpenAI, AzureOpenAI

logger = logging.getLogger("airflow.task")
local_tz = pendulum.timezone("Europe/Amsterdam")

# Environment / connections
airflow_env = os.getenv("AIRFLOW_ENV", "development")
neo4j_conn_id = "neo4j_prod" if airflow_env == "production" else "neo4j_default"

# AI Configuration
MAX_COMMENTS_PER_VIDEO = 1000  # avoid token limits


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    return str(v).strip()


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


def get_videos_with_comments(
    driver,
    limit: Optional[int] = None,
    offset: int = 0,
    skip_analyzed: bool = False,
):
    """
    Query Neo4j for YouTube videos that have comments.
    Returns list of video dictionaries with video_id and comment count.
    """
    if skip_analyzed:
        query = """
        MATCH (v:YouTubeVideo)-[:HAS_COMMENT]->(c:YouTubeComment)
        WHERE (v.comment_summary_description IS NULL OR v.comment_summary_description = '')
        WITH v, count(c) as comment_count
        RETURN v.video_id as video_id, comment_count
        ORDER BY v.video_id
        """
    else:
        query = """
        MATCH (v:YouTubeVideo)-[:HAS_COMMENT]->(c:YouTubeComment)
        WITH v, count(c) as comment_count
        RETURN v.video_id as video_id, comment_count
        ORDER BY v.video_id
        """

    if limit:
        query += f" SKIP {int(offset)} LIMIT {int(limit)}"

    with driver.session() as session:
        result = session.run(query)
        return [
            {"video_id": record["video_id"], "comment_count": record["comment_count"]}
            for record in result
        ]


def get_comments_for_video(driver, video_id: str) -> List[Dict[str, Any]]:
    """
    Retrieve comments for a specific YouTube video from Neo4j.
    Returns list of comment dictionaries with text and metadata.
    """
    query = """
    MATCH (v:YouTubeVideo {video_id: $video_id})-[:HAS_COMMENT]->(c:YouTubeComment)
    RETURN
      coalesce(c.textOriginal, c.textDisplay, '') as text,
      c.comment_id as comment_id,
      c.likeCount as like_count,
      c.authorDisplayName as username,
      c.publishedAt as create_time
    ORDER BY c.publishedAt DESC
    LIMIT $limit
    """

    with driver.session() as session:
        result = session.run(query, video_id=video_id, limit=MAX_COMMENTS_PER_VIDEO)
        comments: List[Dict[str, Any]] = []
        for record in result:
            comment_text = _safe_str(record.get("text", ""))
            if comment_text:
                comments.append(
                    {
                        "text": comment_text,
                        "comment_id": record.get("comment_id"),
                        "like_count": record.get("like_count", 0),
                        "username": record.get("username"),
                        "create_time": str(record.get("create_time", "")),
                    }
                )
        return comments


def analyze_comments_with_ai(client, comments: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Use AI to analyze comments and extract:
    1. Frequent topics with frequency-based weights
    2. Comment summary description

    Handles multiple languages automatically.
    """
    if not comments:
        return {"topics": [], "summary": "No comments available for analysis."}

    comment_texts = [c["text"] for c in comments if _safe_str(c.get("text"))]
    combined_text = "\n\n".join(comment_texts[:MAX_COMMENTS_PER_VIDEO])

    # Truncate if too long (avoid token limits)
    max_chars = 50000
    if len(combined_text) > max_chars:
        combined_text = combined_text[:max_chars]
        logger.warning("Truncated comments text to %d characters", max_chars)

    prompt = f"""Analyze the following YouTube video comments and provide:

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
        {{"topic": "another topic", "weight": 0.72}}
    ],
    "summary": "A comprehensive summary description of the comments..."
}}

Only return the JSON object, no additional text."""

    result_text = ""
    try:
        model = "gpt-4o-mini"
        azure_model = (
            os.getenv("AZURE_OPENAI_MODEL")
            or os.getenv("AZURE_OPENAI_CHAT_MODEL")
            or os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT")
            or os.getenv("OPENAI_MODEL")
        )
        if azure_model:
            model = azure_model

        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an expert at analyzing social media comments and extracting meaningful "
                        "insights. Always respond with valid JSON only."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )

        result_text = response.choices[0].message.content
        result = json.loads(result_text)

        topics = result.get("topics", []) or []
        if topics:
            # Normalize weights relative to max
            try:
                max_weight = max(float(t.get("weight", 0.0)) for t in topics)
            except Exception:
                max_weight = 0.0
            if max_weight > 0:
                for t in topics:
                    try:
                        t["weight"] = round(float(t.get("weight", 0.0)) / max_weight, 3)
                    except Exception:
                        t["weight"] = 0.0

        return {"topics": topics[:20], "summary": _safe_str(result.get("summary", ""))}

    except json.JSONDecodeError as e:
        logger.error("Failed to parse AI response as JSON: %s", e)
        logger.error("Response was: %s", (result_text or "")[:500])
        return {"topics": [], "summary": ""}
    except Exception as e:
        logger.error("Error calling AI for comment analysis: %s", e)
        return {"topics": [], "summary": ""}


def update_video_in_neo4j(driver, video_id: str, topics: List[Dict[str, Any]], summary: str) -> bool:
    """
    Update the YouTubeVideo node in Neo4j with analyzed comment data.
    """
    topic_names: List[str] = []
    topic_weights: List[float] = []
    for t in (topics or []):
        if not isinstance(t, dict):
            continue
        name = _safe_str(t.get("topic"))
        if not name:
            continue
        topic_names.append(name)
        try:
            topic_weights.append(float(t.get("weight", 0.0)))
        except Exception:
            topic_weights.append(0.0)

    topics_json = json.dumps(
        [{"topic": n, "weight": w} for n, w in zip(topic_names, topic_weights)],
        ensure_ascii=False,
    )

    query = """
    MATCH (v:YouTubeVideo {video_id: $video_id})
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
          r.platform = "YouTube",
          r.source = "youtube_comments",
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
                summary=summary,
            )
            record = result.single()
            if record:
                logger.info("Successfully updated YouTube video %s with comment analysis", video_id)
                return True
            logger.warning("YouTube video %s not found in Neo4j", video_id)
            return False
    except Exception as e:
        logger.error("Error updating YouTube video %s in Neo4j: %s", video_id, e)
        return False


def process_video(driver, client, video_id: str) -> bool:
    """
    Process a single video: get comments, analyze, and update Neo4j.
    """
    try:
        logger.info("Processing YouTube video: %s", video_id)
        comments = get_comments_for_video(driver, video_id)
        if not comments:
            logger.warning("No comments found for YouTube video %s", video_id)
            return False

        logger.info("Found %d comments for YouTube video %s", len(comments), video_id)
        analysis = analyze_comments_with_ai(client, comments)

        if not analysis.get("summary"):
            logger.warning("Failed to generate analysis for YouTube video %s", video_id)
            return False

        return update_video_in_neo4j(driver, video_id, analysis["topics"], analysis["summary"])
    except Exception as e:
        logger.error("Error processing YouTube video %s: %s", video_id, e, exc_info=True)
        return False


def analyze_youtube_comments_task(**context):
    logger.info("Starting YouTube comment analysis")
    logger.info("Environment: %s", airflow_env)
    logger.info("Neo4j connection: %s", neo4j_conn_id)

    params = context.get("params", {}) or {}
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    merged = {**params, **conf}  # conf wins

    limit = merged.get("limit", None)
    offset = int(merged.get("offset", 0) or 0)
    video_ids = merged.get("video_ids", None)
    skip_analyzed = bool(merged.get("skip_analyzed", True))

    if video_ids and isinstance(video_ids, str):
        video_ids = [vid.strip() for vid in video_ids.split(",") if vid.strip()]

    try:
        driver = get_neo4j_driver()
        client = get_openai_client()
    except Exception as e:
        logger.error("Failed to initialize clients: %s", e)
        return

    try:
        if video_ids:
            videos = [{"video_id": vid, "comment_count": 0} for vid in video_ids]
            logger.info("Processing %d specific YouTube videos", len(videos))
        else:
            videos = get_videos_with_comments(driver, limit=limit, offset=offset, skip_analyzed=skip_analyzed)
            logger.info("Found %d YouTube videos with comments to process", len(videos))

        if not videos:
            logger.info("No YouTube videos found to process")
            return

        successful = 0
        failed = 0

        for i, video in enumerate(videos, 1):
            video_id = video["video_id"]
            logger.info("Processing video %d/%d: %s", i, len(videos), video_id)

            if process_video(driver, client, video_id):
                successful += 1
            else:
                failed += 1

            if i % 10 == 0:
                logger.info(
                    "Progress: %d/%d videos processed (%d successful, %d failed)",
                    i,
                    len(videos),
                    successful,
                    failed,
                )

        logger.info(
            "Analysis complete: %d successful, %d failed out of %d videos",
            successful,
            failed,
            len(videos),
        )
    except Exception as e:
        logger.error("Error in main processing: %s", e, exc_info=True)
    finally:
        driver.close()


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
    "youtube_comments_analysis_dag",
    default_args=default_args,
    description="Analyze YouTube video comments with AI and extract topics + summary",
    schedule=None,  # Manual-only
    start_date=pendulum.datetime(2025, 1, 21, tz=local_tz),
    catchup=False,
    tags=["youtube", "comments", "ai_analysis", "neo4j"],
    params={
        "limit": None,
        "offset": 0,
        "video_ids": None,  # comma-separated string or list
        "skip_analyzed": True,
    },
) as dag:
    analyze_comments = PythonOperator(
        task_id="analyze_youtube_comments",
        python_callable=analyze_youtube_comments_task,
    )

    analyze_comments

