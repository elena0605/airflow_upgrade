# Run MongoDB and Neo4j Scripts in Docker

This guide shows how to execute scripts from `mongodb_scripts` and `neo4j_scripts` inside your Docker containers.

## Preconditions

From repo root:

```bash
docker compose ps
```

Make sure at least these services are running:
- `airflow-apiserver` (or another Airflow service you use for script execution)
- `mongodb` (for local Mongo shell scripts)
- `neo4j` (for Cypher execution)

Both script folders are mounted into containers:
- `./mongodb_scripts` -> `/opt/airflow/mongodb_scripts`
- `./neo4j_scripts` -> `/opt/airflow/neo4j_scripts`

---

## 1) MongoDB Scripts

### A) Python Mongo scripts (recommended for Cosmos/Atlas/remote Mongo)

Run in Airflow container so `.env` is available:

```bash
docker compose exec airflow-apiserver python /opt/airflow/mongodb_scripts/<script_name>.py
```

Example:

```bash
docker compose exec airflow-apiserver python /opt/airflow/mongodb_scripts/backfill_tiktok_comment_categories.py --dry-run
docker compose exec airflow-apiserver python /opt/airflow/mongodb_scripts/backfill_tiktok_comment_categories.py
```

### B) JavaScript Mongo scripts (run with `mongosh`)

Run against local `mongodb` service:

```bash
docker compose exec mongodb mongosh "mongodb://airflow:tiktok@localhost:27017/admin" --file /opt/airflow/mongodb_scripts/<script_name>.js
```

Example:

```bash
docker compose exec mongodb mongosh "mongodb://airflow:tiktok@localhost:27017/admin" --file /opt/airflow/mongodb_scripts/deduplicate_youtube.js
```

---

## 2) Neo4j Scripts

### A) Python Neo4j scripts

Run in Airflow container:

```bash
docker compose exec airflow-apiserver python /opt/airflow/neo4j_scripts/<script_name>.py
```

Example:

```bash
docker compose exec airflow-apiserver python /opt/airflow/neo4j_scripts/create_person_nodes.py
```

### B) Cypher files (`.cypher`)

Run with Neo4j shell inside `neo4j` container:

```bash
docker compose exec neo4j cypher-shell -u neo4j -p airflowneo4j -f /opt/airflow/neo4j_scripts/<script_name>.cypher
```

Example:

```bash
docker compose exec neo4j cypher-shell -u neo4j -p airflowneo4j -f /opt/airflow/neo4j_scripts/create_follows_cypher.cypher
```

---

## Useful checks

### List scripts inside containers

```bash
docker compose exec airflow-apiserver ls /opt/airflow/mongodb_scripts
docker compose exec airflow-apiserver ls /opt/airflow/neo4j_scripts
```

### If `.env` changes are not reflected

Recreate only the service you run scripts in:

```bash
docker compose up -d --force-recreate --no-deps airflow-apiserver
```

### Follow logs while executing

```bash
docker compose logs -f airflow-apiserver
docker compose logs -f neo4j
docker compose logs -f mongodb
```
