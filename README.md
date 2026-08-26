# Airflow YouTube & TikTok Pipeline

Apache Airflow 3 project that ingests YouTube and TikTok creator data, stores it in MongoDB, builds a Neo4j graph, and runs OpenAI/Azure analysis and embedding jobs.

**Stack:** Airflow 3.0.4 (LocalExecutor) · PostgreSQL · MongoDB · Neo4j · Docker Compose

---

## Table of contents

1. [Architecture](#architecture)
2. [Prerequisites](#prerequisites)
3. [Local setup (first time)](#local-setup-first-time)
4. [Configuration](#configuration)
5. [Running the pipelines](#running-the-pipelines)
6. [DAG reference](#dag-reference)
7. [API quotas & failures](#api-quotas--failures)
8. [GitLab CI/CD (auto-deploy on push)](#gitlab-cicd-auto-deploy-on-push)
9. [Troubleshooting](#troubleshooting)
10. [Project layout](#project-layout)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Pipeline DAGs (orchestrators)                                  │
│  youtube_pipeline · tiktok_pipeline                             │
└───────────────────────────┬─────────────────────────────────────┘
                            │ TriggerDagRunOperator (sequential)
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
   Ingest DAGs        Transform DAGs      Analysis DAGs
   (API → Mongo)      (Mongo → Neo4j)     (OpenAI + embeddings)
```

**Environment switch:** `AIRFLOW_ENV` in `.env`

| Value | Mongo connection | Neo4j connection | Mongo DB name |
|-------|------------------|------------------|---------------|
| `development` | `mongo_default` | `neo4j_default` | `airflow_db` |
| `production` | `mongo_prod` | `neo4j_prod` | `rbl` |

Platform API keys (YouTube, TikTok) are read from **Airflow Variables**, not from `.env`.

---

## Prerequisites

| Requirement | Notes |
|-------------|--------|
| Docker Desktop / Docker Engine | ≥ 4 GB RAM recommended (Neo4j is memory-heavy) |
| Docker Compose v2 | `docker compose` command |
| Git | Clone this repository |
| API access | YouTube Data API key, TikTok Research API client credentials |
| Optional (production) | Azure Cosmos DB, remote Neo4j, Azure OpenAI |

**Ports used locally**

| Service | Port |
|---------|------|
| Airflow UI | 8080 |
| MongoDB | 27017 |
| Neo4j Browser | 7474 |
| Neo4j Bolt | 7687 |

---

## Local setup (first time)

### 1. Clone and configure environment

```bash
git clone <your-gitlab-repo-url> airflow_upgrade
cd airflow_upgrade
cp .env.example .env
```

Edit `.env`:

- Set `AIRFLOW_UID` to your user id (`id -u` on macOS/Linux).
- Keep `AIRFLOW_ENV=development` for local Docker Mongo/Neo4j.
- Add OpenAI / Azure keys if you will run analysis DAGs.

### 2. Prepare data inputs

Edit the influencer CSV files (mounted into containers at `/opt/airflow/data/input/`):

| File | Columns | Used by |
|------|---------|---------|
| `data/input/youtube_influencers.csv` | `username`, `channel_id` | YouTube ingest DAGs |
| `data/input/tiktok_influencers.csv` | `username` | TikTok ingest DAGs |

### 3. Start the stack

```bash
mkdir -p logs config data/tmp_openai_batches
docker compose up airflow-init    # one-time DB migration + admin user
docker compose up -d              # start all services
```

Open **http://localhost:8080** — login with `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` from `.env` (defaults: `airflow` / `airflow`).

### 4. Configure Airflow (required before running ingest)

#### Admin → Variables

| Variable | Description |
|----------|-------------|
| `YOUTUBE_API_KEY` | Google Cloud YouTube Data API v3 key |
| `TIKTOK_CLIENT_KEY` | TikTok Research API client key |
| `TIKTOK_CLIENT_SECRET` | TikTok Research API client secret |
| `TIKTOK_TOKEN` | Optional; refreshed automatically if expired |
| `TIKTOK_TOKEN_EXPIRES_AT` | Optional Unix timestamp |

#### Admin → Connections

Create four connections (Conn Id → Conn Type → Host / login / password / extras):

**Development (`AIRFLOW_ENV=development`)**

| Conn Id | Type | Example |
|---------|------|---------|
| `mongo_default` | Mongo | `mongodb://airflow:<MONGO_PASSWORD>@mongodb:27017/` |
| `neo4j_default` | Neo4j | Host `neo4j`, port `7687`, login `neo4j`, password from `.env` (`NEO4J_PASSWORD_DEV`) |
| `mongo_prod` | Mongo | Your Cosmos/Atlas URI (same as production if you test against prod DB) |
| `neo4j_prod` | Neo4j | Production Neo4j URI (`NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`) |

Use `mongo_default` + `neo4j_default` when `AIRFLOW_ENV=development`.

### 5. Unpause DAGs

New DAGs are paused by default (`DAGS_ARE_PAUSED_AT_CREATION=true`). In the Airflow UI, unpause the DAGs you need, or run a pipeline DAG once.

---

## Configuration

### `.env` file (Docker / runtime)

| Variable | Purpose |
|----------|---------|
| `AIRFLOW_UID` | File ownership inside containers |
| `AIRFLOW_ENV` | `development` or `production` — selects Mongo/Neo4j connections |
| `_AIRFLOW_WWW_USER_*` | Airflow UI login |
| `MONGODB_URI` | Production Mongo URI (Cosmos) |
| `NEO4J_URI*` / `NEO4J_USER*` / `NEO4J_PASSWORD*` | Dev vs prod Neo4j |
| `OPENAI_*` / `AZURE_OPENAI_*` | Analysis & embedding DAGs |

Never commit `.env` — it is gitignored. Use `.env.example` as a template.

### Volumes (docker-compose.yaml)

| Host path | Container path | Purpose |
|-----------|----------------|---------|
| `./dags` | `/opt/airflow/dags` | DAG code (live reload) |
| `./logs` | `/opt/airflow/logs` | Task logs |
| `./config` | `/opt/airflow/config` | Airflow config |
| `./data` | `/opt/airflow/data` | CSV inputs, OpenAI batch temp files |

---

## Running the pipelines

### Full platform run (recommended)

Trigger **one** orchestrator DAG from the Airflow UI (▶):

| DAG | Steps |
|-----|--------|
| `youtube_pipeline` | channel stats → videos → comments → Neo4j transform → thumbnail AI → comment AI |
| `tiktok_pipeline` | user info → videos → comments → thumbnail AI → comment AI |

Each step waits for the previous child DAG to finish (`wait_for_completion=True`, deferrable triggerer).

### Individual DAGs

Child DAGs can still be triggered manually — useful for retries after quota resets or partial failures.

### Typical workflow

1. Ensure CSV inputs and Airflow Variables/Connections are configured.
2. Trigger `youtube_pipeline` or `tiktok_pipeline`.
3. Monitor task logs in the UI.
4. If a task fails on **API quota**, wait for the daily reset and re-run the failed DAG 

---

## DAG reference

### YouTube

| DAG ID | Purpose |
|--------|---------|
| `youtube_pipeline` | Orchestrator (runs all steps below in order) |
| `youtube_channel_stats_dag` | Channel statistics → Mongo + Neo4j |
| `youtube_channel_videos` | Videos (2023 date range) → Mongo + Neo4j |
| `youtube_video_comments` | Top-level comments → Mongo |
| `youtube_comments_to_neo4j` | Comments → Neo4j |
| `youtube_thumbnail_openai_analysis_dag` | Thumbnail analysis + content embeddings |
| `youtube_comments_openai_analysis_dag` | Comment analysis + summary/topic embeddings |

### TikTok

| DAG ID | Purpose |
|--------|---------|
| `tiktok_pipeline` | Orchestrator |
| `tiktok_user_info_dag` | User profiles → Mongo + Neo4j |
| `tiktok_video_dag` | Videos → Mongo + Neo4j |
| `tiktok_video_comments_dag` | Comments → Mongo + Neo4j |
| `tiktok_thumbnail_openai_analysis_dag` | Thumbnail analysis + content embeddings |
| `tiktok_comments_openai_analysis_dag` | Comment analysis + summary/topic embeddings |

---

## API quotas & failures

Ingest tasks **fail immediately** when platform quota/rate limits are hit (no silent partial success).

| Platform | Daily limit | Reset time (UTC+2) |
|----------|-------------|---------------------|
| TikTok Research API | 1,000 requests/day | ~02:00 (midnight UTC) |
| YouTube Data API | 10,000 units/day (default) | ~09:00 summer / ~10:00 winter (midnight Pacific) |

When a fetch task fails:

- Downstream tasks in that run are **skipped**.
- Re-trigger the failed DAG after the quota resets.

---

## GitLab CI/CD (auto-deploy on push)

Every **push** runs validation. Pushes to the **default branch** (`main`) also **deploy** to your host.

### Pipeline stages

| Stage | What it does |
|-------|----------------|
| `validate` | `python3 -m compileall dags/` + `docker compose config` |
| `deploy` | SSH to server → `git pull` → `docker compose build` → `docker compose up -d` |

Config file: [`.gitlab-ci.yml`](.gitlab-ci.yml)

### One-time server setup

On the machine that will **host** Airflow:

```bash
# Install Docker + Docker Compose, then:
sudo mkdir -p /opt/airflow_upgrade
sudo chown $USER:$USER /opt/airflow_upgrade
git clone <your-gitlab-repo-url> /opt/airflow_upgrade
cd /opt/airflow_upgrade
cp .env.example .env
# Edit .env for production (AIRFLOW_ENV=production, secrets, etc.)
bash scripts/deploy.sh   # first manual deploy
```

Configure Airflow Variables and Connections on that host (same as [local setup](#4-configure-airflow-required-before-running-ingest)).

Ensure the deploy user can run Docker without sudo (add user to `docker` group).

### GitLab CI/CD variables

In GitLab: **Settings → CI/CD → Variables**

| Variable | Required | Description |
|----------|----------|-------------|
| `DEPLOY_HOST` | Yes | Server hostname or IP |
| `DEPLOY_USER` | Yes | SSH user (must access `DEPLOY_PATH` and Docker) |
| `DEPLOY_PATH` | Yes | e.g. `/opt/airflow_upgrade` |
| `SSH_PRIVATE_KEY` | Yes | Private key for `DEPLOY_USER` (masked) |
| `DEPLOY_BRANCH` | No | Branch to deploy (default: `main`) |
| `SKIP_DEPLOY` | No | Set `true` to validate only, skip deploy |

Add the matching **public** key to `~/.ssh/authorized_keys` on the deploy host.

### What happens on push

```
git push origin main
    → GitLab runs validate (compile DAGs + compose check)
    → GitLab SSHs to DEPLOY_HOST
    → scripts/deploy.sh: git pull, docker compose build, up -d
    → Airflow available at http://DEPLOY_HOST:8080
```

### Manual deploy (without CI)

```bash
bash scripts/deploy.sh
```

### Validate only (local)

```bash
bash scripts/validate.sh
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| DAGs not visible | Scheduler / dag-processor not running | `docker compose ps` — all Airflow services should be `healthy` |
| Tasks stuck in `queued` | LocalExecutor slot + blocking trigger | Ensure `airflow-triggerer` is running; pipeline uses `deferrable=True` |
| TikTok `KeyError: expires_in` | Invalid client credentials | Fix `TIKTOK_CLIENT_KEY` / `TIKTOK_CLIENT_SECRET` in Airflow Variables |
| YouTube 403 `quotaExceeded` | Daily quota used | Wait for Pacific midnight reset; re-run failed DAG |
| TikTok HTTP 429 | Daily 1,000 request limit | Wait until ~02:00 UTC+2; re-run failed DAG |
| Mongo connection errors | Wrong connection for `AIRFLOW_ENV` | Match `mongo_default` vs `mongo_prod` to your `.env` |
| Permission errors on `logs/` | Wrong `AIRFLOW_UID` | Set `AIRFLOW_UID=$(id -u)` in `.env`, re-run `airflow-init` |

**Useful commands**

```bash
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-triggerer
docker compose restart airflow-scheduler airflow-dag-processor
docker compose down          # stop stack
docker compose down -v       # stop + delete DB volumes (destructive)
```

---

## Project layout

```
.
├── dags/                    # Airflow DAGs and ETL modules
│   ├── youtube_pipeline_dag.py
│   ├── tiktok_pipeline_dag.py
│   ├── youtube_etl.py
│   ├── tiktok_etl.py
│   └── api_rate_limits.py
├── data/
│   └── input/               # Influencer CSV inputs
├── scripts/
│   ├── deploy.sh            # Used by GitLab CI and manual deploys
│   └── validate.sh
├── docker-compose.yaml
├── Dockerfile
├── .env.example
├── .gitlab-ci.yml
└── README.md
```

---

## Security notes

- Do **not** commit `.env`, API keys, or `environment_variables.json`.
- Rotate any credentials that were ever committed to git history.
- The bundled `docker-compose.yaml` is oriented toward development; harden passwords, TLS, and network exposure before public production use.
