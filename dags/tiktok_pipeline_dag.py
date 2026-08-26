"""
TikTok end-to-end pipeline orchestrator.

Triggers existing TikTok DAGs in dependency order. Does not duplicate their logic.
Run this DAG once to execute the full platform pipeline; individual DAGs remain
runnable on their own from the Airflow UI.

Order:
  1. tiktok_user_info_dag
  2. tiktok_video_dag
  3. tiktok_video_comments_dag
  4. tiktok_thumbnail_openai_analysis_dag
  5. tiktok_comments_openai_analysis_dag
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG  # pyright: ignore[reportMissingImports]
from airflow.providers.standard.operators.trigger_dagrun import (  # pyright: ignore[reportMissingImports]
    TriggerDagRunOperator,
)
from helpers.callbacks import task_failure_callback, task_success_callback

PIPELINE_STEPS = [
    ("trigger_user_info", "tiktok_user_info_dag"),
    ("trigger_videos", "tiktok_video_dag"),
    ("trigger_video_comments", "tiktok_video_comments_dag"),
    ("trigger_thumbnail_analysis", "tiktok_thumbnail_openai_analysis_dag"),
    ("trigger_comments_analysis", "tiktok_comments_openai_analysis_dag"),
]

DOC_MD = """
# TikTok pipeline

Triggers child DAGs **sequentially** (each must succeed before the next starts).

| Step | Child DAG | Purpose |
|------|-----------|---------|
| 1 | `tiktok_user_info_dag` | Fetch user profiles → Mongo + Neo4j |
| 2 | `tiktok_video_dag` | Fetch videos → Mongo + Neo4j |
| 3 | `tiktok_video_comments_dag` | Fetch comments → Mongo |
| 4 | `tiktok_thumbnail_openai_analysis_dag` | Thumbnail AI + content embeddings |
| 5 | `tiktok_comments_openai_analysis_dag` | Comment AI + summary/topic embeddings |

Child DAGs can still be triggered individually. Ensure `data/input/tiktok_influencers.csv`
is populated and TikTok Research API credentials are configured before running.
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
    dag_id="tiktok_pipeline",
    default_args=default_args,
    description="Orchestrates the full TikTok ingest → comments → analysis pipeline",
    schedule=None,
    start_date=datetime(2024, 12, 1),
    catchup=False,
    max_active_runs=1,
    tags=["tiktok", "pipeline", "orchestrator"],
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
