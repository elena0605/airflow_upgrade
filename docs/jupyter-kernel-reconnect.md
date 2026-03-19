# Jupyter Kernel Reconnect After `.env` Changes

Use this when your notebook still shows old environment values (for example old `MONGODB_URI`) or kernel restart fails with errors like `Kernel does not exist`.

## Why this happens

- Editing `.env` on the host does not always update values inside an already-created container.
- A simple `docker compose restart jupyter` may keep the old environment.
- Cursor/VS Code can also keep a stale kernel ID after server/container changes.

## Step 1: Recreate Jupyter container (recommended)

From the project root:

```bash
docker compose up -d --force-recreate --no-deps jupyter
docker compose ps jupyter
```

Expected: `jupyter` is `Up` on port `8888`.

## Step 2: Reload Cursor and reconnect kernel

1. Close the notebook tab.
2. Open Command Palette: `Cmd + Shift + P`
3. Run: `Developer: Reload Window`
4. Reopen your notebook.
5. Click `Select Kernel`.
6. Choose `Existing Jupyter Server...`
7. Type `http://localhost:8888`
8. Pick a new kernel from that server.

## Step 3: Verify `.env` changes are active

Run this as the first notebook cell:

```python
import os
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env", override=True)
print(os.getenv("MONGODB_URI"))
```

Then quick connection test:

```python
from pymongo import MongoClient
client = MongoClient(os.getenv("MONGODB_URI"), serverSelectionTimeoutMS=8000)
print(client.admin.command("ping"))
```

## Troubleshooting

- If kernel restart gives 500 with `Kernel does not exist`, it is usually a stale client session; reconnect with a fresh kernel as above.
- If `MONGODB_URI` is still old after recreate, confirm no later cell overrides it in code.
- If hostname fails DNS resolution, verify the host in URI is correct (for example `rbtl.mongo.cosmos.azure.com` vs old values).
