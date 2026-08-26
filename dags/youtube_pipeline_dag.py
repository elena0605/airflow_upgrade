"""
YouTube end-to-end pipeline orchestrator.

Triggers existing YouTube DAGs in dependency order. Does not duplicate their logic.
Run this DAG once to execute the full platform pipeline; individual DAGs remain
runnable on their own from the Airflow UI.

Order:
  1. youtube_channel_stats_dag
  2. youtube_channel_videos
  3. youtube_video_comments
  4. youtube_comments_to_neo4j
  5. youtube_thumbnail_openai_analysis_dag
  6. youtube_comments_openai_analysis_dag
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG  # pyright: ignore[reportMissingImports]
from airflow.providers.standard.operators.trigger_dagrun import (  # pyright: ignore[reportMissingImports]
    TriggerDagRunOperator,
)
from callbacks import task_failure_callback, task_success_callback

PIPELINE_STEPS = [
    ("trigger_channel_stats", "youtube_channel_stats_dag"),
    ("trigger_channel_videos", "youtube_channel_videos"),
    ("trigger_video_comments", "youtube_video_comments"),
    ("trigger_comments_to_neo4j", "youtube_comments_to_neo4j"),
    ("trigger_thumbnail_analysis", "youtube_thumbnail_openai_analysis_dag"),
    ("trigger_comments_analysis", "youtube_comments_openai_analysis_dag"),
]

DOC_MD = """
# YouTube pipeline

Triggers child DAGs **sequentially** (each must succeed before the next starts).

| Step | Child DAG | Purpose |
|------|-----------|---------|
| 1 | `youtube_channel_stats_dag` | Fetch channel statistics → Mongo + Neo4j |
| 2 | `youtube_channel_videos` | Fetch videos → Mongo + Neo4j |
| 3 | `youtube_video_comments` | Fetch top-level comments → Mongo |
| 4 | `youtube_comments_to_neo4j` | Transform comments → Neo4j graph |
| 5 | `youtube_thumbnail_openai_analysis_dag` | Thumbnail AI + content embeddings |
| 6 | `youtube_comments_openai_analysis_dag` | Comment AI + summary/topic embeddings |

Child DAGs can still be triggered individually. Ensure `data/input/youtube_influencers.csv`
is populated and API credentials are configured before running.
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": task_failure_callback,
    "on_success_callback": task_success_callback,
}

with DAG(
    dag_id="youtube_pipeline",
    default_args=default_args,
    description="Orchestrates the full YouTube ingest → comments → analysis pipeline",
    schedule=None,
    start_date=datetime(2025, 1, 15),
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "pipeline", "orchestrator"],
    doc_md=DOC_MD,
) as dag:
    previous_task = None
    for task_id, trigger_dag_id in PIPELINE_STEPS:
        task = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id=trigger_dag_id,
            wait_for_completion=True,
            poke_interval=60,
            deferrable=True,
            reset_dag_run=False,
        )
        if previous_task is not None:
            previous_task >> task
        previous_task = task
