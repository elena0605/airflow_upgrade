"""
DAG to enrich YouTubeVideo nodes in Neo4j with a high-level "video summary" and
"relevant topics with weights" using existing fields:

- comment_summary_description
- comments_frequent_topics_json
- thumbnail_description
- thumbnail_keywords
- thumbnail_url
- topic_categories
- video_description
- video_title
- video_url
- tags
- channel_title

Outputs (stored on :YouTubeVideo):
- video_summary_description (<= 1000 chars)
- video_summary_topics (list[str])
- video_summary_topic_weights (list[float])
- video_summary_topics_json (json string)
- video_summary_enriched_at (datetime)

Also maintains:
- (v:YouTubeVideo)-[:HAS_VIDEO_TOPIC]->(t:Topic) with {weight, platform, source, analyzed_at}
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

airflow_env = os.getenv("AIRFLOW_ENV", "development")
neo4j_conn_id = "neo4j_prod" if airflow_env == "production" else "neo4j_default"

AZURE_CHAT_DEPLOYMENT = os.getenv("AZURE_OPENAI_MODEL")  # Azure deployment name


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    return str(v).strip()


def _as_str_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        out: List[str] = []
        for x in v:
            s = _safe_str(x)
            if s:
                out.append(s)
        return out
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        # allow comma-separated fallbacks
        if "," in s:
            return [p.strip() for p in s.split(",") if p.strip()]
        return [s]
    # scalar fallback
    s = _safe_str(v)
    return [s] if s else []


def _get_openai_client():
    """Get OpenAI or Azure OpenAI client from environment variables."""
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    azure_api_key = os.getenv("AZURE_OPENAI_API_KEY")
    azure_api_version = os.getenv("AZURE_OPENAI_API_VERSION") or "2024-10-21"

    if azure_endpoint and azure_api_key:
        logger.info("Using Azure OpenAI client")
        return AzureOpenAI(
            api_key=azure_api_key,
            api_version=azure_api_version,
            azure_endpoint=azure_endpoint,
        )

    logger.info("Using regular OpenAI client")
    return OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def _get_neo4j_driver():
    neo4j_hook = Neo4jHook(conn_id=neo4j_conn_id)
    return neo4j_hook.get_conn()


def _summarize_video_with_ai(client, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns dict:
      {
        "video_summary": "<=1000 chars",
        "topics": [{"topic": "...", "weight": 0.9}, ...]
      }
    """
    model = AZURE_CHAT_DEPLOYMENT or "gpt-4o-mini"

    prompt = """You are enriching a YouTube video knowledge base.

Given the fields below (some may be empty), produce:
1) video_summary: a concise, factual summary of the VIDEO content and context (not just the comments),
   MAX 1000 characters, English. Use available signals: title, description, thumbnail description/keywords,
   comment summary, frequent comment topics, tags, topic categories, channel title, and video URL context.
2) topics: 5-15 relevant topics for this video, each with a weight 0.0-1.0 (relative importance).
   Topics should be short noun phrases in English (e.g. "football contracts", "day-in-the-life vlog").

Return ONLY valid JSON:
{
  "video_summary": "...",
  "topics": [{"topic": "...", "weight": 0.85}, ...]
}
"""

    user_content = json.dumps(payload, ensure_ascii=False)

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Return JSON only."},
                {"role": "user", "content": prompt + "\n\nINPUT:\n" + user_content},
            ],
            response_format={"type": "json_object"},
        )
        data = json.loads(resp.choices[0].message.content)
        summary = _safe_str(data.get("video_summary", ""))[:1000]
        topics = data.get("topics", []) or []

        clean_topics: List[Dict[str, Any]] = []
        max_w = 0.0
        for t in topics:
            if not isinstance(t, dict):
                continue
            name = _safe_str(t.get("topic"))
            if not name:
                continue
            try:
                w = float(t.get("weight", 0.0))
            except Exception:
                w = 0.0
            w = max(0.0, min(1.0, w))
            max_w = max(max_w, w)
            clean_topics.append({"topic": name, "weight": w})

        if max_w > 0:
            for t in clean_topics:
                t["weight"] = round(float(t["weight"]) / max_w, 3)

        return {"video_summary": summary, "topics": clean_topics[:20]}
    except Exception as e:
        logger.error("AI summarize error (%s): %s", model, e)
        return {"video_summary": "", "topics": []}


def _fetch_video_enrichment_inputs(driver, video_id: str) -> Optional[Dict[str, Any]]:
    query = """
    MATCH (v:YouTubeVideo {video_id: $video_id})
    WITH v LIMIT 1
    // Don't pin the label here: if YoutubeVideoTag doesn't exist yet, Neo4j emits UnknownLabelWarning
    OPTIONAL MATCH (v)-[:HAS_TAG]->(tag)
    RETURN
      v.video_id AS video_id,
      v.video_title AS video_title,
      v.video_description AS video_description,
      v.video_url AS video_url,
      v.thumbnail_url AS thumbnail_url,
      v.thumbnail_description AS thumbnail_description,
      v.thumbnail_keywords AS thumbnail_keywords,
      v.topic_categories AS topic_categories,
      v.tags AS tags_prop,
      collect(DISTINCT tag.name) AS tags_rel,
      v.channel_title AS channel_title,
      v.comment_summary_description AS comment_summary_description,
      v.comments_frequent_topics_json AS comments_frequent_topics_json,
      v.video_summary_description AS video_summary_description
    """
    with driver.session() as session:
        rec = session.run(query, video_id=video_id).single()
        return dict(rec) if rec else None


def _get_video_ids_to_enrich(
    driver,
    limit: Optional[int],
    offset: int,
    skip_if_present: bool,
) -> List[str]:
    where_parts = [
        "("
        "coalesce(v.video_title,'') <> '' OR "
        "coalesce(v.video_description,'') <> '' OR "
        "coalesce(v.video_url,'') <> '' OR "
        "coalesce(v.thumbnail_url,'') <> '' OR "
        "coalesce(v.thumbnail_description,'') <> '' OR "
        "v.thumbnail_keywords IS NOT NULL OR "
        "v.topic_categories IS NOT NULL OR "
        "v.tags IS NOT NULL OR "
        "coalesce(v.channel_title,'') <> '' OR "
        "coalesce(v.comment_summary_description,'') <> '' OR "
        "coalesce(v.comments_frequent_topics_json,'') <> ''"
        ")"
    ]
    if skip_if_present:
        where_parts.append("(v.video_summary_description IS NULL OR v.video_summary_description = '')")

    query = f"""
    MATCH (v:YouTubeVideo)
    WHERE {' AND '.join(where_parts)}
    RETURN v.video_id AS video_id
    ORDER BY v.video_id
    """
    if limit:
        query += f" SKIP {int(offset)} LIMIT {int(limit)}"

    with driver.session() as session:
        return [r["video_id"] for r in session.run(query)]


def _update_video_summary_in_neo4j(
    driver,
    video_id: str,
    summary: str,
    topics: List[Dict[str, Any]],
) -> bool:
    topic_names: List[str] = []
    topic_weights: List[float] = []
    for t in topics or []:
        if not isinstance(t, dict):
            continue
        name = _safe_str(t.get("topic"))
        if not name:
            continue
        try:
            w = float(t.get("weight", 0.0))
        except Exception:
            w = 0.0
        topic_names.append(name)
        topic_weights.append(w)

    topics_json = json.dumps(
        [{"topic": n, "weight": w} for n, w in zip(topic_names, topic_weights)],
        ensure_ascii=False,
    )

    q = """
    MATCH (v:YouTubeVideo {video_id: $video_id})
    SET v.video_summary_description = $summary,
        v.video_summary_topics = $topic_names,
        v.video_summary_topic_weights = $topic_weights,
        v.video_summary_topics_json = $topics_json,
        v.video_summary_enriched_at = datetime()

    WITH v, $topic_names AS topic_names, $topic_weights AS topic_weights
    // Keep relationships in sync with the latest enrichment run
    OPTIONAL MATCH (v)-[old:HAS_VIDEO_TOPIC]->(:Topic)
    DELETE old

    WITH v, topic_names, topic_weights
    CALL (v, topic_names, topic_weights) {
      UNWIND range(0, size(topic_names) - 1) AS i
      WITH
        v,
        topic_names[i] AS topic,
        topic_weights[i] AS weight
      MERGE (t:Topic {name: topic})
      MERGE (v)-[r:HAS_VIDEO_TOPIC]->(t)
      SET r.weight = weight,
          r.platform = "YouTube",
          r.source = "youtube_video_enrichment",
          r.analyzed_at = v.video_summary_enriched_at
      RETURN count(*) AS _
    }

    RETURN v.video_id AS video_id
    """

    with driver.session() as session:
        rec = session.run(
            q,
            video_id=video_id,
            summary=summary,
            topic_names=topic_names,
            topic_weights=topic_weights,
            topics_json=topics_json,
        ).single()
        return bool(rec)


def enrich_youtube_video_summary_task(**context):
    params = context.get("params", {}) or {}
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    merged = {**params, **conf}  # conf wins

    limit = merged.get("limit")
    offset = int(merged.get("offset", 0) or 0)
    video_ids = merged.get("video_ids")
    skip_if_present = bool(merged.get("skip_if_present", True))

    if isinstance(video_ids, str):
        video_ids = [v.strip() for v in video_ids.split(",") if v.strip()]

    driver = _get_neo4j_driver()
    client = _get_openai_client()

    try:
        if video_ids:
            ids = list(video_ids)
        else:
            ids = _get_video_ids_to_enrich(driver, limit=limit, offset=offset, skip_if_present=skip_if_present)

        logger.info("Found %d YouTube videos to enrich", len(ids))

        ok = 0
        bad = 0
        for i, vid in enumerate(ids, 1):
            logger.info("Enriching %d/%d: %s", i, len(ids), vid)

            row = _fetch_video_enrichment_inputs(driver, vid)
            if not row:
                bad += 1
                continue

            tags = _as_str_list(row.get("tags_prop")) + _as_str_list(row.get("tags_rel"))
            # de-dupe while preserving order
            seen = set()
            tags = [t for t in tags if not (t in seen or seen.add(t))]

            payload = {
                "video_id": row.get("video_id"),
                "video_title": _safe_str(row.get("video_title")),
                "video_description": _safe_str(row.get("video_description")),
                "video_url": _safe_str(row.get("video_url")),
                "thumbnail_url": _safe_str(row.get("thumbnail_url")),
                "thumbnail_description": _safe_str(row.get("thumbnail_description")),
                "thumbnail_keywords": _as_str_list(row.get("thumbnail_keywords")),
                "topic_categories": _as_str_list(row.get("topic_categories")),
                "tags": tags,
                "channel_title": _safe_str(row.get("channel_title")),
                "comment_summary_description": _safe_str(row.get("comment_summary_description")),
                "comments_frequent_topics_json": _safe_str(row.get("comments_frequent_topics_json")),
            }

            out = _summarize_video_with_ai(client, payload)
            summary = _safe_str(out.get("video_summary", ""))[:1000]
            topics = out.get("topics", []) or []

            if not summary:
                logger.warning("No summary produced for %s; skipping Neo4j update", vid)
                bad += 1
                continue

            if _update_video_summary_in_neo4j(driver, vid, summary, topics):
                ok += 1
            else:
                bad += 1

        logger.info("Done. success=%d failed=%d", ok, bad)
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
    dag_id="youtube_video_enrichment_dag",
    default_args=default_args,
    description="Enrich YouTubeVideo nodes with video-level summary + topics (manual-only)",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 21, tz=local_tz),
    catchup=False,
    tags=["youtube", "enrichment", "ai_analysis", "neo4j"],
    params={
        "limit": None,
        "offset": 0,
        "video_ids": None,  # comma-separated string or list
        "skip_if_present": True,  # skip if v.video_summary_description already exists
    },
) as dag:
    enrich = PythonOperator(
        task_id="enrich_youtube_video_summary",
        python_callable=enrich_youtube_video_summary_task,
    )

    enrich

