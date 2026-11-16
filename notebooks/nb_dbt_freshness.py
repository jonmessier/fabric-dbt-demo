# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_dbt_freshness
#
# Runs `dbt source freshness` for the pipeline `pl_daily_load`, between the bronze ingest and the dbt job.
# The Fabric dbt job cannot run freshness (it is not one of its operations, docs/SPIKE.md item 9), so this
# Python notebook installs the same dbt version and runs it:
#
# 1. Refresh the `lh_bronze` SQL endpoint metadata, so the warehouse sees the rows just ingested
#    (otherwise they arrive ~45 s later, SPIKE item 18). dbt reads bronze through that endpoint.
# 2. Take the dbt project from the dbt job item (`dbt_lpb`), so this runs exactly what the job builds.
# 3. Install dbt-core / dbt-fabric pinned like `requirements.txt`, and run `dbt source freshness` against
#    `wh_analytics` with the notebook's own identity (`authentication: fabricnotebook`).
# 4. Exit with a JSON summary. **Warnings pass** (the late distributor in "act 1" is a warning);
#    **any source in error fails the notebook**, which stops the pipeline before the dbt build.
#
# Everything is looked up by name in the current workspace; no IDs are stored here.
# No default lakehouse is needed; `scripts/pipeline.py` deploys it.

# PARAMETERS CELL ********************

lakehouse_name = "lh_bronze"
warehouse_name = "wh_analytics"
dbt_job_name = "dbt_lpb"
run_id = ""  # set by scripts: the summary is also written to Files/_runs/<run_id>.json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Keep in step with requirements.txt (scripts/pipeline.py checks this line before deploying).
DBT_PACKAGES = ["dbt-core~=1.11.0", "dbt-fabric==1.10.0"]

import subprocess
import sys

subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "--disable-pip-version-check", *DBT_PACKAGES],
               check=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import base64
import json
import time
from pathlib import Path

import notebookutils
import requests

API = "https://api.fabric.microsoft.com/v1"
started = time.time()


def api(method, path, body=None):
    """Fabric REST call as the notebook's identity; follows long-running operations."""
    headers = {"Authorization": f"Bearer {notebookutils.credentials.getToken('pbi')}"}
    resp = requests.request(method, f"{API}/{path}", headers=headers, json=body, timeout=120)
    if resp.status_code == 202 and resp.headers.get("Location"):
        location = resp.headers["Location"]
        for _ in range(120):
            time.sleep(int(resp.headers.get("Retry-After", 2)))
            resp = requests.get(location, headers=headers, timeout=120)
            state = resp.json().get("status") if resp.content else None
            if state in ("Succeeded", "Failed", "Cancelled") or resp.status_code != 200:
                break
        if state != "Succeeded":
            raise RuntimeError(f"{method} {path}: operation {state}: {resp.text[:500]}")
        resp = requests.get(location.rstrip("/") + "/result", headers=headers, timeout=120)
    if resp.status_code >= 400:
        raise RuntimeError(f"{method} {path} -> {resp.status_code}: {resp.text[:500]}")
    return resp.json() if resp.content else {}


def item(kind, name):
    items = api("GET", f"workspaces/{workspace_id}/items?type={kind}").get("value", [])
    match = [i for i in items if i["displayName"] == name]
    if not match:
        raise RuntimeError(f"{kind} {name} not found in this workspace")
    return match[0]


workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
lakehouse = api("GET", f"workspaces/{workspace_id}/lakehouses/{item('Lakehouse', lakehouse_name)['id']}")
warehouse = api("GET", f"workspaces/{workspace_id}/warehouses/{item('Warehouse', warehouse_name)['id']}")
job = item("DataBuildToolJob", dbt_job_name)

# 1. Make the newest bronze rows visible to the warehouse.
endpoint_id = lakehouse["properties"]["sqlEndpointProperties"]["id"]
sync = api("POST", f"workspaces/{workspace_id}/sqlEndpoints/{endpoint_id}/refreshMetadata", {})
failed_sync = [t.get("tableName") for t in sync.get("value", []) if t.get("status") == "Failure"]
if failed_sync:
    raise RuntimeError(f"SQL endpoint metadata sync failed for {failed_sync}")
print(f"SQL endpoint refreshed: {len(sync.get('value', []))} tables")

# 2. The dbt project, from the dbt job's definition (parts Code/dbt/<file>).
project = Path("/tmp/dbt_freshness/project")
definition = api("POST", f"workspaces/{workspace_id}/items/{job['id']}/getDefinition")
n_files = 0
for part in definition["definition"]["parts"]:
    if part["path"].startswith("Code/dbt/"):
        target = project / part["path"][len("Code/dbt/"):]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(base64.b64decode(part["payload"]))
        n_files += 1
print(f"dbt project: {n_files} files from {dbt_job_name}")

(project / "profiles.yml").write_text(json.dumps({"lpb": {"target": "fabric", "outputs": {"fabric": {
    "type": "fabric",
    "driver": "ODBC Driver 18 for SQL Server",  # dbt-fabric 1.10 uses pyodbc
    "server": warehouse["properties"]["connectionString"],
    "port": 1433,
    "database": warehouse_name,
    "schema": "dbo",
    "authentication": "fabricnotebook",
    "threads": 4,
    "retries": 2,
}}}}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 3. dbt source freshness.
import pyodbc

if "ODBC Driver 18 for SQL Server" not in pyodbc.drivers():
    raise RuntimeError(f"ODBC Driver 18 is not installed in this runtime; drivers: {pyodbc.drivers()}")

from dbt.cli.main import dbtRunner

res = dbtRunner().invoke(["source", "freshness", "--project-dir", str(project), "--profiles-dir", str(project),
                          "--target-path", str(project / "target"), "--log-path", str(project / "logs")])

sources_json = project / "target" / "sources.json"
results = json.loads(sources_json.read_text())["results"] if sources_json.exists() else []
by_status = {}
for r in results:
    by_status.setdefault(r["status"], []).append(r["unique_id"].split(".", 2)[-1])

problems = {s: v for s, v in by_status.items() if s != "pass"}
result = {
    "ok": res.success or (bool(results) and not ({"error", "runtime error"} & set(by_status))),
    "sources": len(results),
    "counts": {s: len(v) for s, v in by_status.items()},
    "problems": problems,
    "dbt_exception": str(res.exception) if res.exception else None,
    "seconds": int(time.time() - started),
}
print(json.dumps(result, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 4. Hand the summary back: exit value for the pipeline, Files/_runs for scripts (the job API does not
#    return exit values). Fail on errors so the pipeline stops before the dbt build.
if run_id:
    runs = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse['id']}/Files/_runs"
    notebookutils.fs.put(f"{runs}/{run_id}.json", json.dumps(result), True)
if not result["ok"]:
    raise RuntimeError(f"source freshness failed: {json.dumps(result)}")
notebookutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
