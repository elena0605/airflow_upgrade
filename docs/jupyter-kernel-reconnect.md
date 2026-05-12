# Jupyter Kernel Reconnect After `.env` Changes

Use this when your notebook still shows old environment values (for example old `MONGODB_URI`) or kernel restart fails with errors like `Kernel does not exist`.

## Why this happens

- Editing `.env` on the host does not always update values inside an already-created container.
- A simple `docker compose restart jupyter` may keep the old environment.
- Cursor/VS Code can also keep a stale kernel ID after server/container changes.

## Step 1: Recreate Jupyter container (recommended)

From the project root:

The `jupyter` service uses the **`notebook` Compose profile**. If you omit it, the container may never start and the editor will sit on “loading” forever.

```bash
docker compose --profile notebook up -d --force-recreate --no-deps jupyter
docker compose --profile notebook ps jupyter
```

Expected: `jupyter` is `Up` on port `8888`.

Sanity-check that the server answers (should return JSON, not HTML for `/lab`):

```bash
curl -sS "http://127.0.0.1:8888/api/status" | head -c 200
echo
```

## Step 2: Reload Cursor and reconnect kernel

1. Close the notebook tab.
2. Open Command Palette: `Cmd + Shift + P`
3. Run: `Developer: Reload Window`
4. Reopen your notebook.
5. Click `Select Kernel`.
6. Choose `Existing Jupyter Server...`
7. Paste **only** the server root (no `/lab`, no query string), for example:
   - `http://127.0.0.1:8888`  
   or  
   - `http://localhost:8888`  

   Cursor talks to the **Jupyter Server HTTP API** at that origin. A URL like `http://localhost:8888/lab` is the **browser UI** path and often makes “Existing Jupyter Server” hang or fail.

8. Pick a new kernel from that server (e.g. **Python 3 (ipykernel)** from the container).

## Step 3: Verify `.env` changes are active

Run this as the first notebook cell:

```python
import os
from dotenv import load_dotenv

# Optional: reload from file if you mount `.env` into the container; otherwise vars
# already come from Compose `env_file` when the container started.
load_dotenv(override=True)
print(os.getenv("MONGODB_URI"))
```

Then quick connection test:

```python
from pymongo import MongoClient
client = MongoClient(os.getenv("MONGODB_URI"), serverSelectionTimeoutMS=8000)
print(client.admin.command("ping"))
```

## Troubleshooting

- **“Existing Jupyter Server” never finishes loading:** Use `http://127.0.0.1:8888` (no `/lab`, no `?`). Confirm the container is up with the **`notebook` profile** and `curl` above returns JSON.
- If kernel restart gives 500 with `Kernel does not exist`, it is usually a stale client session; reconnect with a fresh kernel as above.
- If `MONGODB_URI` is still old after recreate, confirm no later cell overrides it in code.
- If hostname fails DNS resolution, verify the host in URI is correct (for example `rbtl.mongo.cosmos.azure.com` vs old values).
