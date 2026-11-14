"""Upload the landing zone to lh_bronze and run the bronze ingest notebook.

    python scripts/load_bronze.py --env dev [--reload-all] [--skip-upload] [--no-run]

Steps (all looked up by name from config/environments.yml):
  1. Upload data/landing/** to lh_bronze  Files/landing/**  through OneLake. Files already there
     with the same size are skipped, so a rerun after advance-day only sends the new day.
  2. Create or update notebook nb_ingest_bronze from notebooks/nb_ingest_bronze.py, with
     lh_bronze attached as its default lakehouse (IDs are filled in here, never stored in git).
  3. Run the notebook (job scheduler API), wait for it, and print the summary it wrote to
     Files/_runs/<run_id>.json (files and rows loaded per bronze table).

Ends with a one-line JSON summary on stdout. Progress goes to stderr.
"""
from __future__ import annotations

import argparse
import base64
import json
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from fabric_common import (REPO_ROOT, FabricClient, FabricError, OneLake, TokenCache, env_config,
                           finish, log)

LANDING_DIR = REPO_ROOT / "data" / "landing"
NOTEBOOK_SRC = REPO_ROOT / "notebooks" / "nb_ingest_bronze.py"


def upload_landing(lake: OneLake, lakehouse_id: str, landing_path: str) -> dict:
    if not LANDING_DIR.exists():
        raise FabricError("data/landing not found. Run: .\\tasks.ps1 data")
    local = {p.relative_to(LANDING_DIR).as_posix(): p for p in LANDING_DIR.rglob("*")
             if p.is_file() and not p.name.startswith("_")}  # _state.json is the generator's own
    prefix = f"{lakehouse_id}/{landing_path.strip('/')}"
    remote = lake.list_files(prefix)
    todo = [rel for rel, p in local.items() if remote.get(f"{prefix}/{rel}") != p.stat().st_size]
    log(f"Landing zone: {len(local)} local files, {len(local) - len(todo)} already uploaded, "
        f"{len(todo)} to upload")

    def send(rel: str) -> None:
        lake.upload(f"{prefix}/{rel}", local[rel].read_bytes())

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(send, sorted(todo)))
    return {"local_files": len(local), "uploaded": len(todo),
            "bytes": sum(local[r].stat().st_size for r in todo)}


def notebook_source(ws_id: str, lakehouse: dict) -> str:
    """The notebook source with lh_bronze set as default lakehouse in its METADATA block."""
    src = NOTEBOOK_SRC.read_text(encoding="utf-8")
    deps = {"lakehouse": {"default_lakehouse": lakehouse["id"],
                          "default_lakehouse_name": lakehouse["displayName"],
                          "default_lakehouse_workspace_id": ws_id}}
    meta = {"kernel_info": {"name": "synapse_pyspark"}, "dependencies": deps}
    meta_lines = "\n".join(f"# META {line}" for line in json.dumps(meta, indent=2).splitlines())
    # Replace the first (notebook-level) META block only.
    new, n = re.subn(r"(# METADATA \*+\n\n)((?:# META.*\n)+)", lambda m: m.group(1) + meta_lines + "\n",
                     src, count=1)
    if n != 1:
        raise FabricError(f"{NOTEBOOK_SRC.name}: notebook METADATA block not found")
    return new


def deploy_notebook(client: FabricClient, ws_id: str, name: str, source: str) -> tuple[dict, str]:
    definition = {
        "format": "fabricGitSource",
        "parts": [{"path": "notebook-content.py", "payloadType": "InlineBase64",
                   "payload": base64.b64encode(source.encode("utf-8")).decode("ascii")}],
    }
    nb = client.item(ws_id, "notebooks", name, required=False)
    if nb is None:
        log(f"Creating notebook {name}")
        client.request("POST", f"workspaces/{ws_id}/notebooks",
                       json_body={"displayName": name, "definition": definition})
        return client.item(ws_id, "notebooks", name), "created"
    log(f"Updating notebook {name}")
    client.request("POST", f"workspaces/{ws_id}/notebooks/{nb['id']}/updateDefinition",
                   json_body={"definition": definition})
    return nb, "updated"


def run_notebook(client: FabricClient, ws_id: str, nb_id: str, params: dict, timeout_s: int) -> dict:
    body = {"executionData": {"parameters": {
        k: {"value": v, "type": "bool" if isinstance(v, bool) else "string"} for k, v in params.items()}}}
    resp = client.request("POST", f"workspaces/{ws_id}/items/{nb_id}/jobs/instances",
                          params={"jobType": "RunNotebook"}, json_body=body, lro=False)
    location = resp.get("_location")
    if not location:
        raise FabricError(f"notebook run returned no job location: {resp}")
    started = time.time()
    status = "NotStarted"
    while time.time() - started < timeout_s:
        time.sleep(15)
        job = client.request("GET", location, lro=False)
        if job.get("status") != status:
            status = job.get("status")
            log(f"  notebook {status} after {int(time.time() - started)}s")
        if status in ("Completed", "Failed", "Cancelled", "Deduped"):
            job["_seconds"] = int(time.time() - started)
            return job
    raise FabricError(f"notebook still {status} after {timeout_s}s (job {location})")


def sync_sql_endpoint(client: FabricClient, ws_id: str, lakehouse_id: str) -> dict:
    """Make new Delta data visible to the warehouse now. Without this the lakehouse SQL endpoint
    picks up new rows on its own after ~45 s (docs/SPIKE.md item 18) and dbt could read stale bronze."""
    lh = client.request("GET", f"workspaces/{ws_id}/lakehouses/{lakehouse_id}", lro=False)
    endpoint_id = ((lh.get("properties") or {}).get("sqlEndpointProperties") or {}).get("id")
    if not endpoint_id:
        raise FabricError("lakehouse has no SQL endpoint yet")
    started = time.time()
    result = client.request("POST", f"workspaces/{ws_id}/sqlEndpoints/{endpoint_id}/refreshMetadata",
                            json_body={})
    tables = result.get("value", []) if isinstance(result, dict) else []
    failed = [t.get("tableName") for t in tables if t.get("status") == "Failure"]
    if failed:
        raise FabricError(f"SQL endpoint metadata sync failed for: {failed}")
    return {"seconds": int(time.time() - started), "tables": len(tables)}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True, choices=["dev", "test", "prod"])
    ap.add_argument("--reload-all", action="store_true", help="drop and reload every bronze table")
    ap.add_argument("--skip-upload", action="store_true", help="don't upload data/landing")
    ap.add_argument("--no-run", action="store_true", help="upload and deploy the notebook, don't run it")
    ap.add_argument("--timeout", type=int, default=1800, help="seconds to wait for the notebook")
    args = ap.parse_args()

    cfg = env_config(args.env)
    summary: dict = {"ok": False, "env": args.env, "workspace": cfg["workspace"]}
    try:
        tokens = TokenCache()
        client = FabricClient(tokens)
        ws = client.workspace(cfg["workspace"])
        lh = client.item(ws["id"], "lakehouses", cfg["items"]["lakehouse"])
        lake = OneLake(ws["id"], tokens)

        if not args.skip_upload:
            summary["upload"] = upload_landing(lake, lh["id"], cfg["landing_path"])

        nb, action = deploy_notebook(client, ws["id"], cfg["items"]["ingest_notebook"],
                                     notebook_source(ws["id"], lh))
        summary["notebook"] = action

        if not args.no_run:
            run_id = uuid.uuid4().hex
            log(f"Running {cfg['items']['ingest_notebook']} (run_id {run_id})")
            job = run_notebook(client, ws["id"], nb["id"],
                               {"run_id": run_id, "reload_all": args.reload_all}, args.timeout)
            summary["run"] = {"status": job.get("status"), "seconds": job["_seconds"]}
            if job.get("status") != "Completed":
                raise FabricError(f"notebook {job.get('status')}: {json.dumps(job.get('failureReason'))}")
            raw = lake.read(f"{lh['id']}/Files/_runs/{run_id}.json")
            if raw is None:
                raise FabricError("notebook completed but wrote no run summary (Files/_runs)")
            result = json.loads(raw)
            summary["ingest"] = result
            summary["sql_endpoint_sync"] = sync_sql_endpoint(client, ws["id"], lh["id"])
            summary["ok"] = bool(result.get("ok"))
        else:
            summary["ok"] = True
    except FabricError as e:
        summary["error"] = str(e)
    finish(summary)


if __name__ == "__main__":
    main()
