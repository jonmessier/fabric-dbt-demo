"""Deploy the daily pipeline pl_daily_load (and the notebooks it runs), and optionally run it.

    python scripts/pipeline.py --env dev [--run] [--skip-upload] [--timeout 3600]

Pipeline (docs/DESIGN.md section 4):

    Ingest bronze (nb_ingest_bronze) -> Check source freshness (nb_dbt_freshness) -> Build with dbt (dbt_lpb)

Deploys, by name (IDs are looked up at run time, never stored in git):
  * nb_ingest_bronze  from notebooks/nb_ingest_bronze.py, lh_bronze as default lakehouse
  * nb_dbt_freshness  from notebooks/nb_dbt_freshness.py (refreshes the SQL endpoint, runs
                      dbt source freshness with the project taken from dbt_lpb; fails on errors)
  * pl_daily_load     the three activities above. The dbt one (InvokeDataBuildToolJob, selector nightly) needs
                      a Fabric connection of type DataBuildToolJob, which is created in the UI (OAuth) or
                      for a service principal. It is found by name from config environments.<env>.dbt_connection,
                      or else the connection the pipeline already uses is kept. Activities this script does
                      not own (added in the UI) are kept and run after freshness.
The dbt job itself is deployed by scripts/dbt_job.py.

With --run: upload new landing files first (as load_bronze.py does, unless --skip-upload), run the
pipeline, wait, and print each activity's status and the notebooks' exit values.

Ends with a one-line JSON summary on stdout. Progress goes to stderr.
"""
from __future__ import annotations

import argparse
import base64
import json
import re
import time
from datetime import datetime, timedelta, timezone

from fabric_common import (REPO_ROOT, FabricClient, FabricError, OneLake, TokenCache, env_config,
                           finish, log)
from load_bronze import NOTEBOOK_SRC, deploy_notebook, notebook_source, upload_landing

FRESHNESS_SRC = REPO_ROOT / "notebooks" / "nb_dbt_freshness.py"
FRESHNESS_NOTEBOOK = "nb_dbt_freshness"
INGEST_ACTIVITY = "Ingest bronze"
FRESHNESS_ACTIVITY = "Check source freshness"
DBT_ACTIVITY = "Build with dbt"
DBT_ACTIVITY_TYPE = "InvokeDataBuildToolJob"
DBT_SELECTOR = "nightly"
OWNED = (INGEST_ACTIVITY, FRESHNESS_ACTIVITY, DBT_ACTIVITY)


def check_dbt_pins() -> None:
    """nb_dbt_freshness installs dbt itself; its pins must match requirements.txt."""
    req = {re.split(r"[=~<>!]", line)[0].strip(): line.strip()
           for line in (REPO_ROOT / "requirements.txt").read_text().splitlines()
           if line.strip() and not line.startswith("#")}
    nb = re.search(r"DBT_PACKAGES = \[(.*)\]", FRESHNESS_SRC.read_text(encoding="utf-8"))
    pins = re.findall(r'"([^"]+)"', nb.group(1)) if nb else []
    for pin in pins:
        name = re.split(r"[=~<>!]", pin)[0]
        if req.get(name) != pin:
            raise FabricError(f"{FRESHNESS_SRC.name} pins {pin} but requirements.txt has {req.get(name)}")
    if not pins:
        raise FabricError(f"{FRESHNESS_SRC.name}: DBT_PACKAGES not found")


def notebook_activity(name: str, ws_id: str, notebook_id: str, depends_on: list[str], timeout: str) -> dict:
    return {
        "name": name,
        "type": "TridentNotebook",
        "dependsOn": [{"activity": d, "dependencyConditions": ["Succeeded"]} for d in depends_on],
        "policy": {"timeout": timeout, "retry": 0, "retryIntervalInSeconds": 30,
                   "secureOutput": False, "secureInput": False},
        "typeProperties": {"notebookId": notebook_id, "workspaceId": ws_id},
    }


def dbt_activity(ws_id: str, dbt_job_id: str, connection_id: str) -> dict:
    """The dbt job activity, as the pipeline editor writes it (read back 2026-11-15, docs/SPIKE.md item 22)."""
    return {
        "name": DBT_ACTIVITY,
        "type": DBT_ACTIVITY_TYPE,
        "dependsOn": [{"activity": FRESHNESS_ACTIVITY, "dependencyConditions": ["Succeeded"]}],
        "policy": {"timeout": "0.02:00:00", "retry": 0, "retryIntervalInSeconds": 30,
                   "secureOutput": False, "secureInput": False},
        "typeProperties": {"dataBuildToolJobId": dbt_job_id, "workspaceId": ws_id, "operation": "build",
                           "threads": 4, "selectorName": DBT_SELECTOR},
        "externalReferences": {"connection": connection_id},
    }


def dbt_connection_id(client: FabricClient, cfg: dict, existing: list[dict]) -> tuple[str, str]:
    """(connection id, how it was found). Connections are tenant-level and created outside this script."""
    name = cfg.get("dbt_connection")
    if name:
        conn = client.find("connections", name)
        if conn is None:
            raise FabricError(f"connection '{name}' (config dbt_connection) not found or not shared with you")
        return conn["id"], f"config: {name}"
    for act in existing:
        conn_id = (act.get("externalReferences") or {}).get("connection")
        if act.get("type") == DBT_ACTIVITY_TYPE and conn_id:
            return conn_id, "kept from the existing pipeline"
    raise FabricError("no connection for the dbt job activity. Create a 'DataBuildToolJob' connection in Fabric "
                      "(pipeline editor > dbt job activity > Connection > New) and set dbt_connection in "
                      "config/environments.yml, or add the activity once in the UI")


def pipeline_definition(ws_id: str, ingest_id: str, freshness_id: str, dbt_job_id: str, connection_id: str,
                        existing: list[dict]) -> dict:
    activities = [
        notebook_activity(INGEST_ACTIVITY, ws_id, ingest_id, [], "0.01:00:00"),
        notebook_activity(FRESHNESS_ACTIVITY, ws_id, freshness_id, [INGEST_ACTIVITY], "0.00:30:00"),
        dbt_activity(ws_id, dbt_job_id, connection_id),
    ]
    # Any dbt job activity already there (whatever its name) is replaced by ours.
    replaced = {a.get("name") for a in existing if a.get("type") == DBT_ACTIVITY_TYPE}
    for act in existing:
        if act.get("name") in OWNED or act.get("name") in replaced:
            continue
        deps = [{**d, "activity": DBT_ACTIVITY} if d.get("activity") in replaced else d
                for d in act.get("dependsOn", [])]
        if not deps:
            # Other first-level activities (added in the UI) run after freshness.
            deps.append({"activity": FRESHNESS_ACTIVITY, "dependencyConditions": ["Succeeded"]})
        activities.append({**act, "dependsOn": deps})
    content = {"properties": {"description": "Daily load: bronze ingest, source freshness, dbt build. "
                                             "Deployed by scripts/pipeline.py.",
                              "activities": activities}}
    return {"parts": [{"path": "pipeline-content.json", "payloadType": "InlineBase64",
                       "payload": base64.b64encode(json.dumps(content, indent=2).encode()).decode()}]}


def existing_activities(client: FabricClient, ws_id: str, pipeline: dict | None) -> list[dict]:
    if pipeline is None:
        return []
    d = client.request("POST", f"workspaces/{ws_id}/items/{pipeline['id']}/getDefinition", json_body={})
    for part in d["definition"]["parts"]:
        if part["path"] == "pipeline-content.json":
            return json.loads(base64.b64decode(part["payload"]))["properties"].get("activities", [])
    return []


def ensure_pipeline(client: FabricClient, ws_id: str, name: str, ingest_id: str, freshness_id: str,
                    dbt_job_id: str, cfg: dict) -> tuple[dict, str, list, str]:
    pl = client.item(ws_id, "dataPipelines", name, required=False)
    existing = existing_activities(client, ws_id, pl)
    connection_id, found = dbt_connection_id(client, cfg, existing)
    definition = pipeline_definition(ws_id, ingest_id, freshness_id, dbt_job_id, connection_id, existing)
    names = [a["name"] for a in json.loads(base64.b64decode(definition["parts"][0]["payload"]))["properties"]["activities"]]
    if pl is None:
        log(f"Creating pipeline {name}")
        client.request("POST", f"workspaces/{ws_id}/dataPipelines",
                       json_body={"displayName": name, "definition": definition})
        return client.item(ws_id, "dataPipelines", name), "created", names, found
    log(f"Updating pipeline {name}")
    client.request("POST", f"workspaces/{ws_id}/dataPipelines/{pl['id']}/updateDefinition",
                   json_body={"definition": definition})
    return pl, "updated", names, found


def run_pipeline(client: FabricClient, ws_id: str, pl_id: str, timeout_s: int) -> dict:
    resp = client.request("POST", f"workspaces/{ws_id}/items/{pl_id}/jobs/instances",
                          params={"jobType": "Pipeline"}, lro=False)
    location = resp.get("_location")
    if not location:
        raise FabricError(f"pipeline run returned no job location: {resp}")
    run_id = location.rstrip("/").split("/")[-1]
    started = time.time()
    status = "NotStarted"
    while time.time() - started < timeout_s:
        time.sleep(20)
        job = client.request("GET", location, lro=False)
        if job.get("status") != status:
            status = job.get("status")
            log(f"  pipeline {status} after {int(time.time() - started)}s")
        if status in ("Completed", "Failed", "Cancelled", "Deduped"):
            job["_seconds"] = int(time.time() - started)
            job["_run_id"] = run_id
            return job
    raise FabricError(f"pipeline still {status} after {timeout_s}s (job {location})")


def activity_runs(client: FabricClient, ws_id: str, run_id: str) -> list[dict]:
    now = datetime.now(timezone.utc)
    body = {"filters": [], "orderBy": [{"orderBy": "ActivityRunStart", "order": "ASC"}],
            "lastUpdatedAfter": (now - timedelta(days=1)).isoformat(),
            "lastUpdatedBefore": (now + timedelta(hours=1)).isoformat()}
    resp = client.request("POST", f"workspaces/{ws_id}/datapipelines/pipelineruns/{run_id}/queryactivityruns",
                          json_body=body, lro=False)
    rows = resp.get("value", resp if isinstance(resp, list) else [])
    out = []
    for r in rows:
        row = {"activity": r.get("activityName"), "status": r.get("status"),
               "seconds": round((r.get("durationInMs") or 0) / 1000)}
        output = r.get("output") or {}
        exit_value = (output.get("result") or {}).get("exitValue") if isinstance(output, dict) else None
        if exit_value:
            try:
                row["exit_value"] = json.loads(exit_value)
            except (TypeError, json.JSONDecodeError):
                row["exit_value"] = exit_value
        err = r.get("error") or {}
        if err.get("message"):
            row["error"] = err["message"][:400]
        out.append(row)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True, choices=["dev", "test", "prod"])
    ap.add_argument("--run", action="store_true", help="run the pipeline after deploying it, and wait")
    ap.add_argument("--skip-upload", action="store_true", help="with --run: don't upload data/landing first")
    ap.add_argument("--timeout", type=int, default=3600, help="seconds to wait for the run")
    args = ap.parse_args()

    cfg = env_config(args.env)
    summary: dict = {"ok": False, "env": args.env, "workspace": cfg["workspace"]}
    try:
        check_dbt_pins()
        tokens = TokenCache()
        client = FabricClient(tokens)
        ws = client.workspace(cfg["workspace"])
        lh = client.item(ws["id"], "lakehouses", cfg["items"]["lakehouse"])
        dbt_job = client.item(ws["id"], "items", cfg["items"]["dbt_job"], required=False)
        if dbt_job is None:
            raise FabricError(f"dbt job {cfg['items']['dbt_job']} not found. Run: .\\tasks.ps1 dbt-job -Env {args.env}")

        ingest, a1 = deploy_notebook(client, ws["id"], cfg["items"]["ingest_notebook"],
                                     notebook_source(NOTEBOOK_SRC, ws["id"], lh))
        fresh, a2 = deploy_notebook(client, ws["id"], FRESHNESS_NOTEBOOK, notebook_source(FRESHNESS_SRC, ws["id"]))
        pl, a3, names, conn = ensure_pipeline(client, ws["id"], cfg["items"]["pipeline"], ingest["id"], fresh["id"],
                                              dbt_job["id"], cfg)
        summary["deployed"] = {cfg["items"]["ingest_notebook"]: a1, FRESHNESS_NOTEBOOK: a2,
                               cfg["items"]["pipeline"]: a3, "activities": names, "dbt_connection": conn}

        if args.run:
            if not args.skip_upload:
                summary["upload"] = upload_landing(OneLake(ws["id"], tokens), lh["id"], cfg["landing_path"])
            log(f"Running {cfg['items']['pipeline']}")
            job = run_pipeline(client, ws["id"], pl["id"], args.timeout)
            summary["run"] = {"status": job.get("status"), "seconds": job["_seconds"],
                              "activities": activity_runs(client, ws["id"], job["_run_id"])}
            summary["ok"] = job.get("status") == "Completed"
            if not summary["ok"]:
                summary["error"] = f"pipeline {job.get('status')}: {json.dumps(job.get('failureReason'))[:400]}"
        else:
            summary["ok"] = True
    except FabricError as e:
        summary["error"] = str(e)
    finish(summary)


if __name__ == "__main__":
    main()
