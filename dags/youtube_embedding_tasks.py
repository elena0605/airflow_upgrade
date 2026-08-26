"""
YouTube Neo4j embedding helpers (ported from notebooks/youtube_prod_comment_summary_embeddings.ipynb).

Used by:
- youtube_comments_analysis_dag: comment summary + comment topic sync/embeddings
- youtube_thumbnail_analysis_dag: video content embeddings
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from airflow.providers.mongo.hooks.mongo import MongoHook  # pyright: ignore[reportMissingImports]
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook  # pyright: ignore[reportMissingImports]
from openai import AzureOpenAI, OpenAI  # pyright: ignore[reportMissingImports]

logger = logging.getLogger("airflow.task")

YOUTUBE_PLATFORM = "youtube"

PAGE_SIZE = int(os.getenv("YT_SUMMARY_EMBED_PAGE_SIZE", "500"))
EMBED_BATCH = int(os.getenv("YT_SUMMARY_EMBED_BATCH", "128"))
SUMMARY_VECTOR_WRITE_BATCH = int(
    os.getenv("YT_SUMMARY_VECTOR_WRITE_BATCH", os.getenv("YT_SUMMARY_WRITE_BATCH", "32"))
)
CONTENT_VECTOR_WRITE_BATCH = int(os.getenv("YT_CONTENT_VECTOR_WRITE_BATCH", "32"))
TOPIC_UPSERT_BATCH = int(os.getenv("YT_TOPIC_UPSERT_BATCH", "200"))
TOPIC_SYNC_LIMIT = int(os.getenv("YT_TOPIC_SYNC_LIMIT", "0"))
TOPIC_EMBED_WRITE_BATCH = int(os.getenv("YT_TOPIC_EMBED_WRITE_BATCH", "32"))
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

AZURE_OPENAI_EMBEDDING_ENDPOINT = os.getenv("AZURE_OPENAI_EMBEDDING_ENDPOINT")
AZURE_OPENAI_EMBEDDING_API_KEY = os.getenv("AZURE_OPENAI_EMBEDDING_API_KEY")
AZURE_OPENAI_EMBEDDING_API_VERSION = os.getenv(
    "AZURE_OPENAI_EMBEDDING_API_VERSION", "2024-02-01"
)
AZURE_OPENAI_EMBEDDING_DEPLOYMENT = (
    os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT_MODEL_NAME")
    or os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT_NAME")
    or os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT")
    or "text-embedding-3-large"
)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-large")

YOUTUBE_TOPICS_MONGO_PROJECTION = {
    "video_id": 1,
    "comments_frequent_topics": 1,
    "comments_frequent_topic_categories": 1,
    "comments_frequent_topic_weights": 1,
}

FETCH_SUMMARY_CYPHER = """
MATCH (v:YouTubeVideo)
WHERE v.comment_summary_description IS NOT NULL
  AND v.comment_summary_embedding IS NULL
RETURN elementId(v) AS eid, v.comment_summary_description AS text
LIMIT $limit
"""

WRITE_SUMMARY_CYPHER = """
UNWIND $rows AS row
MATCH (v) WHERE elementId(v) = row.eid
CALL db.create.setNodeVectorProperty(v, 'comment_summary_embedding', row.embedding)
RETURN count(*) AS written
"""

WRITE_CONTENT_CYPHER = """
UNWIND $rows AS row
MATCH (v) WHERE elementId(v) = row.eid
SET v.video_content_text = row.text
WITH v, row
CALL db.create.setNodeVectorProperty(v, 'video_content_embedding', row.embedding)
RETURN count(*) AS written
"""

FETCH_CONTENT_CYPHER = """
MATCH (v:YouTubeVideo)
WHERE v.video_content_embedding IS NULL
  AND (
    v.video_title IS NOT NULL
    OR v.video_description IS NOT NULL
    OR v.thumbnail_description IS NOT NULL
    OR size(coalesce(v.thumbnail_keywords, [])) > 0
    OR size(coalesce(v.tags, [])) > 0
  )
RETURN elementId(v) AS eid,
       v.video_title AS video_title,
       v.video_description AS video_description,
       v.thumbnail_description AS thumbnail_description,
       coalesce(v.thumbnail_keywords, []) AS thumbnail_keywords,
       coalesce(v.tags, []) AS tags
LIMIT $limit
"""

SYNC_YOUTUBE_COMMENT_TOPICS_CYPHER = """
UNWIND $rows AS row
OPTIONAL MATCH (v:YouTubeVideo {video_id: row.video_id})
WITH row, v
WHERE v IS NOT NULL
OPTIONAL MATCH (v)-[r:HAS_COMMENT_TOPIC]->()
DELETE r
WITH row, v
UNWIND row.topics AS topic
WITH row, v, topic
WHERE topic.name IS NOT NULL
MERGE (ct:YouTubeCommentTopic {video_id: row.video_id, name: topic.name})
SET ct.category = topic.category,
    ct.platform = $youtube_platform
MERGE (v)-[rel:HAS_COMMENT_TOPIC]->(ct)
SET rel.weight = topic.weight,
    rel.position = topic.position,
    rel.platform = $youtube_platform
"""

YOUTUBE_TOPIC_FETCH_CYPHER = """
MATCH (t:YouTubeCommentTopic)
WHERE coalesce(t.platform, 'youtube') = $platform
  AND t.embedding IS NULL AND t.name IS NOT NULL
RETURN elementId(t) AS eid, t.name AS name
LIMIT $limit
"""

YOUTUBE_TOPIC_WRITE_CYPHER = """
UNWIND $rows AS row
MATCH (t) WHERE elementId(t) = row.eid
CALL db.create.setNodeVectorProperty(t, 'embedding', row.embedding)
RETURN count(*) AS written
"""


def batched(iterable: Iterable[Any], n: int) -> Iterator[list]:
    bucket: list = []
    for item in iterable:
        bucket.append(item)
        if len(bucket) >= n:
            yield bucket
            bucket = []
    if bucket:
        yield bucket


def build_embedding_client() -> Tuple[str, Any, str]:
    if AZURE_OPENAI_EMBEDDING_ENDPOINT and AZURE_OPENAI_EMBEDDING_API_KEY:
        client = AzureOpenAI(
            azure_endpoint=AZURE_OPENAI_EMBEDDING_ENDPOINT,
            api_key=AZURE_OPENAI_EMBEDDING_API_KEY,
            api_version=AZURE_OPENAI_EMBEDDING_API_VERSION,
        )
        return "azure", client, AZURE_OPENAI_EMBEDDING_DEPLOYMENT
    if OPENAI_API_KEY:
        return "openai", OpenAI(api_key=OPENAI_API_KEY), OPENAI_EMBEDDING_MODEL
    raise RuntimeError(
        "No embedding credentials found. Set Azure embedding vars or OPENAI_API_KEY."
    )


def get_embedding_dim(embed_client: Any, embed_model_name: str) -> int:
    probe = embed_client.embeddings.create(model=embed_model_name, input=["ping"])
    return len(probe.data[0].embedding)


def embed_texts(
    embed_client: Any,
    embed_model_name: str,
    texts: List[str],
    max_retries: int = 5,
) -> List[List[float]]:
    out: List[List[float]] = []
    for chunk in batched(texts, EMBED_BATCH):
        attempt = 0
        while True:
            try:
                resp = embed_client.embeddings.create(model=embed_model_name, input=chunk)
                out.extend([d.embedding for d in resp.data])
                break
            except Exception as e:
                attempt += 1
                if attempt > max_retries:
                    raise
                wait = min(2 ** attempt, 30)
                logger.warning("Embed retry %d after %ss: %s", attempt, wait, e)
                time.sleep(wait)
    return out


def _write_summary_vector_chunk(driver, rows_chunk: List[Dict[str, Any]]) -> int:
    from neo4j.exceptions import Neo4jError, ServiceUnavailable

    attempt = 0
    while True:
        try:
            with driver.session(database=NEO4J_DATABASE) as s:
                rec = s.run(WRITE_SUMMARY_CYPHER, rows=rows_chunk).single()
                return int(rec["written"]) if rec else 0
        except (Neo4jError, ServiceUnavailable, TimeoutError, OSError) as e:
            attempt += 1
            if attempt > 8:
                raise
            wait = min(2 ** attempt, 120)
            logger.warning("Neo4j summary-vector write retry %d after %ss: %s", attempt, wait, e)
            time.sleep(wait)


def _write_content_vector_chunk(driver, rows_chunk: List[Dict[str, Any]]) -> int:
    from neo4j.exceptions import Neo4jError, ServiceUnavailable

    attempt = 0
    while True:
        try:
            with driver.session(database=NEO4J_DATABASE) as s:
                rec = s.run(WRITE_CONTENT_CYPHER, rows=rows_chunk).single()
                return int(rec["written"]) if rec else 0
        except (Neo4jError, ServiceUnavailable, TimeoutError, OSError) as e:
            attempt += 1
            if attempt > 8:
                raise
            wait = min(2 ** attempt, 120)
            logger.warning("Neo4j content-vector write retry %d after %ss: %s", attempt, wait, e)
            time.sleep(wait)


def _write_topic_embedding_chunk(driver, rows_chunk: List[Dict[str, Any]]) -> int:
    from neo4j.exceptions import Neo4jError

    attempt = 0
    while True:
        try:
            with driver.session(database=NEO4J_DATABASE) as s:
                rec = s.run(YOUTUBE_TOPIC_WRITE_CYPHER, rows=rows_chunk).single()
                return int(rec["written"]) if rec else 0
        except Neo4jError as e:
            attempt += 1
            if attempt > 8:
                raise
            wait = min(2 ** attempt, 120)
            logger.warning("Neo4j topic-vector write retry %d after %ss: %s", attempt, wait, e)
            time.sleep(wait)


def ensure_summary_vector_index(driver, dim: int) -> None:
    stmt = f"""CREATE VECTOR INDEX video_summary_embedding_index IF NOT EXISTS
    FOR (v:YouTubeVideo) ON (v.comment_summary_embedding)
    OPTIONS {{ indexConfig: {{ `vector.dimensions`: {dim}, `vector.similarity_function`: 'cosine' }} }}"""
    with driver.session(database=NEO4J_DATABASE) as s:
        s.run(stmt)
    logger.info("Ensured video_summary_embedding_index (dim=%d)", dim)


def ensure_content_vector_index(driver, dim: int) -> None:
    stmt = f"""CREATE VECTOR INDEX video_content_embedding_index IF NOT EXISTS
    FOR (v:YouTubeVideo) ON (v.video_content_embedding)
    OPTIONS {{ indexConfig: {{ `vector.dimensions`: {dim}, `vector.similarity_function`: 'cosine' }} }}"""
    with driver.session(database=NEO4J_DATABASE) as s:
        s.run(stmt)
    logger.info("Ensured video_content_embedding_index (dim=%d)", dim)


def ensure_youtube_topic_constraint(driver) -> None:
    stmt = (
        "CREATE CONSTRAINT youtube_comment_topic_video_name IF NOT EXISTS "
        "FOR (t:YouTubeCommentTopic) REQUIRE (t.video_id, t.name) IS UNIQUE"
    )
    with driver.session(database=NEO4J_DATABASE) as s:
        s.run(stmt)
    logger.info("Ensured constraint on (YouTubeCommentTopic.video_id, YouTubeCommentTopic.name)")


def ensure_youtube_topic_vector_index(driver, dim: int) -> None:
    stmt = f"""CREATE VECTOR INDEX youtube_comment_topic_embedding_index IF NOT EXISTS
    FOR (t:YouTubeCommentTopic) ON (t.embedding)
    OPTIONS {{ indexConfig: {{ `vector.dimensions`: {dim}, `vector.similarity_function`: 'cosine' }} }}"""
    with driver.session(database=NEO4J_DATABASE) as s:
        s.run(stmt)
    logger.info("Ensured youtube_comment_topic_embedding_index (dim=%d)", dim)


def build_youtube_video_content_text(
    title: Optional[str],
    description: Optional[str],
    thumbnail_description: Optional[str],
    thumbnail_keywords: List[str],
    tags: List[str],
) -> Optional[str]:
    parts: List[str] = []
    if title:
        parts.append(f"Title: {title}")
    if description:
        parts.append(f"Description: {description}")
    if thumbnail_description:
        parts.append(f"Thumbnail: {thumbnail_description}")
    if thumbnail_keywords:
        parts.append(f"Thumbnail keywords: {', '.join(thumbnail_keywords)}")
    if tags:
        parts.append(f"Tags: {', '.join(tags)}")
    return "\n".join(parts).strip() if parts else None


def embed_youtube_comment_summaries(
    driver,
    embed_client: Any,
    embed_model_name: str,
    page_size: int = PAGE_SIZE,
) -> int:
    total_written = 0
    start = time.time()
    while True:
        with driver.session(database=NEO4J_DATABASE) as s:
            pending = list(s.run(FETCH_SUMMARY_CYPHER, limit=page_size))
        if not pending:
            break

        texts = [r["text"] for r in pending]
        embeddings = embed_texts(embed_client, embed_model_name, texts)
        rows = [
            {"eid": pending[i]["eid"], "embedding": embeddings[i]}
            for i in range(len(pending))
        ]

        for chunk in batched(rows, SUMMARY_VECTOR_WRITE_BATCH):
            total_written += _write_summary_vector_chunk(driver, chunk)

        logger.info(
            "Comment summary embeddings written=%d elapsed=%.1fs",
            total_written,
            time.time() - start,
        )
    return total_written


def embed_youtube_video_content(
    driver,
    embed_client: Any,
    embed_model_name: str,
    page_size: int = PAGE_SIZE,
) -> int:
    total_written = 0
    start = time.time()
    while True:
        with driver.session(database=NEO4J_DATABASE) as s:
            pending = list(s.run(FETCH_CONTENT_CYPHER, limit=page_size))
        if not pending:
            break

        rows_to_embed: List[Dict[str, Any]] = []
        texts: List[str] = []
        for record in pending:
            thumbnail_keywords = [
                k for k in (record["thumbnail_keywords"] or []) if isinstance(k, str) and k
            ]
            tag_names = [t for t in (record["tags"] or []) if isinstance(t, str) and t]
            text = build_youtube_video_content_text(
                record["video_title"],
                record["video_description"],
                record["thumbnail_description"],
                thumbnail_keywords,
                tag_names,
            )
            if text:
                rows_to_embed.append({"eid": record["eid"], "text": text})
                texts.append(text)

        if not rows_to_embed:
            break

        embeddings = embed_texts(embed_client, embed_model_name, texts)
        write_rows = [
            {
                "eid": rows_to_embed[i]["eid"],
                "text": rows_to_embed[i]["text"],
                "embedding": embeddings[i],
            }
            for i in range(len(rows_to_embed))
        ]

        for chunk in batched(write_rows, CONTENT_VECTOR_WRITE_BATCH):
            total_written += _write_content_vector_chunk(driver, chunk)

        logger.info(
            "Video content embeddings written=%d elapsed=%.1fs",
            total_written,
            time.time() - start,
        )
    return total_written


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def topic_row_from_mongo_doc(doc: dict) -> Optional[Dict[str, Any]]:
    raw_id = doc.get("video_id")
    if raw_id is None:
        return None
    video_id = str(raw_id).strip()
    if not video_id:
        return None

    names = doc.get("comments_frequent_topics") or []
    categories = doc.get("comments_frequent_topic_categories") or []
    weights = doc.get("comments_frequent_topic_weights") or []
    topics: List[Dict[str, Any]] = []
    for i, name in enumerate(names):
        name = clean_text(name)
        if not name:
            continue
        topics.append(
            {
                "name": name,
                "category": clean_text(categories[i] if i < len(categories) else None),
                "weight": float(weights[i])
                if i < len(weights) and weights[i] is not None
                else None,
                "position": i,
            }
        )
    return {"video_id": video_id, "topics": topics}


def sync_youtube_comment_topics_from_mongo(
    driver,
    collection,
    batch_size: int = TOPIC_UPSERT_BATCH,
) -> Tuple[int, int]:
    cur = collection.find({}, YOUTUBE_TOPICS_MONGO_PROJECTION)
    if TOPIC_SYNC_LIMIT > 0:
        cur = cur.limit(TOPIC_SYNC_LIMIT)

    total_docs = 0
    total_links = 0
    for batch in batched(cur, batch_size):
        rows = []
        for doc in batch:
            row = topic_row_from_mongo_doc(doc)
            if row:
                rows.append(row)
        if not rows:
            continue
        total_docs += len(rows)
        batch_topic_slots = sum(len(r["topics"]) for r in rows)
        with driver.session(database=NEO4J_DATABASE) as s:
            s.run(
                SYNC_YOUTUBE_COMMENT_TOPICS_CYPHER,
                rows=rows,
                youtube_platform=YOUTUBE_PLATFORM,
            )
        total_links += batch_topic_slots
        logger.info(
            "Topic sync batch: mongo_rows=%d topic_slots_synced=%d",
            total_docs,
            total_links,
        )
    return total_docs, total_links


def embed_youtube_topic_nodes(
    driver,
    embed_client: Any,
    embed_model_name: str,
    page_size: int = PAGE_SIZE,
) -> int:
    total_written = 0
    start = time.time()
    while True:
        with driver.session(database=NEO4J_DATABASE) as s:
            pending = list(
                s.run(
                    YOUTUBE_TOPIC_FETCH_CYPHER,
                    limit=page_size,
                    platform=YOUTUBE_PLATFORM,
                )
            )
        if not pending:
            break

        texts = [r["name"] for r in pending]
        embeddings = embed_texts(embed_client, embed_model_name, texts)
        rows = [
            {"eid": pending[i]["eid"], "embedding": embeddings[i]}
            for i in range(len(pending))
        ]
        for chunk in batched(rows, TOPIC_EMBED_WRITE_BATCH):
            total_written += _write_topic_embedding_chunk(driver, chunk)
        logger.info(
            "YouTubeCommentTopic embeddings written=%d elapsed=%.1fs",
            total_written,
            time.time() - start,
        )
    return total_written


def _summary_embedding_stats(driver) -> Dict[str, int]:
    with driver.session(database=NEO4J_DATABASE) as s:
        return {
            "videos_with_summary": s.run(
                "MATCH (v:YouTubeVideo) WHERE v.comment_summary_description IS NOT NULL RETURN count(v) AS c"
            ).single()["c"],
            "videos_with_summary_embedding": s.run(
                "MATCH (v:YouTubeVideo) WHERE v.comment_summary_embedding IS NOT NULL RETURN count(v) AS c"
            ).single()["c"],
            "summary_pending": s.run(
                "MATCH (v:YouTubeVideo) WHERE v.comment_summary_description IS NOT NULL "
                "AND v.comment_summary_embedding IS NULL RETURN count(v) AS c"
            ).single()["c"],
        }


def _content_embedding_stats(driver) -> Dict[str, int]:
    with driver.session(database=NEO4J_DATABASE) as s:
        return {
            "videos_with_content_embedding": s.run(
                "MATCH (v:YouTubeVideo) WHERE v.video_content_embedding IS NOT NULL RETURN count(v) AS c"
            ).single()["c"],
            "content_pending": s.run(
                "MATCH (v:YouTubeVideo) WHERE v.video_content_embedding IS NULL AND ("
                "v.video_title IS NOT NULL OR v.video_description IS NOT NULL "
                "OR v.thumbnail_description IS NOT NULL "
                "OR size(coalesce(v.thumbnail_keywords, [])) > 0 "
                "OR size(coalesce(v.tags, [])) > 0) RETURN count(v) AS c"
            ).single()["c"],
        }


def embed_comment_summary_embeddings_task(neo4j_conn_id: str, **context):
    provider, embed_client, embed_model_name = build_embedding_client()
    embed_dim = get_embedding_dim(embed_client, embed_model_name)
    logger.info(
        "Embedding provider=%s model=%s dim=%d",
        provider,
        embed_model_name,
        embed_dim,
    )

    driver = Neo4jHook(conn_id=neo4j_conn_id).get_conn()
    try:
        ensure_summary_vector_index(driver, embed_dim)
        written = embed_youtube_comment_summaries(driver, embed_client, embed_model_name)
        stats = _summary_embedding_stats(driver)
        logger.info(
            "Comment summary embedding pass done: newly_written=%d stats=%s",
            written,
            stats,
        )
        return {"newly_written": written, **stats}
    finally:
        driver.close()


def sync_comment_topics_from_mongo_task(
    neo4j_conn_id: str,
    mongo_conn_id: str,
    mongo_db_name: str,
    mongo_collection: str,
    **context,
):
    mongo = MongoHook(mongo_conn_id=mongo_conn_id).get_conn()
    collection = mongo[mongo_db_name][mongo_collection]
    driver = Neo4jHook(conn_id=neo4j_conn_id).get_conn()
    try:
        ensure_youtube_topic_constraint(driver)
        mongo_docs, topic_links = sync_youtube_comment_topics_from_mongo(driver, collection)
        logger.info(
            "Topic sync done: mongo_docs=%d topic_slots=%d",
            mongo_docs,
            topic_links,
        )
        return {"mongo_docs": mongo_docs, "topic_slots": topic_links}
    finally:
        driver.close()


def embed_comment_topic_embeddings_task(neo4j_conn_id: str, **context):
    provider, embed_client, embed_model_name = build_embedding_client()
    embed_dim = get_embedding_dim(embed_client, embed_model_name)
    logger.info(
        "Embedding provider=%s model=%s dim=%d",
        provider,
        embed_model_name,
        embed_dim,
    )

    driver = Neo4jHook(conn_id=neo4j_conn_id).get_conn()
    try:
        ensure_youtube_topic_vector_index(driver, embed_dim)
        written = embed_youtube_topic_nodes(driver, embed_client, embed_model_name)
        logger.info("Comment topic embedding pass done: newly_written=%d", written)
        return {"newly_written": written}
    finally:
        driver.close()


def embed_video_content_embeddings_task(neo4j_conn_id: str, **context):
    provider, embed_client, embed_model_name = build_embedding_client()
    embed_dim = get_embedding_dim(embed_client, embed_model_name)
    logger.info(
        "Embedding provider=%s model=%s dim=%d",
        provider,
        embed_model_name,
        embed_dim,
    )

    driver = Neo4jHook(conn_id=neo4j_conn_id).get_conn()
    try:
        ensure_content_vector_index(driver, embed_dim)
        written = embed_youtube_video_content(driver, embed_client, embed_model_name)
        stats = _content_embedding_stats(driver)
        logger.info(
            "Video content embedding pass done: newly_written=%d stats=%s",
            written,
            stats,
        )
        return {"newly_written": written, **stats}
    finally:
        driver.close()
