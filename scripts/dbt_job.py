"""Create or update the Fabric dbt job (dbt_lpb) from the repo's dbt/ folder, and optionally run it.

    python scripts/dbt_job.py --env dev [--operation build] [--select ...] [--exclude ...] [--run]

The job item (DataBuildToolJob) carries the dbt project inside its own definition, in the layout the
Fabric UI writes (docs/SPIKE.md item 12):

    dbt-content.json      project location, warehouse connection, command
    Code/dbt/<files>      the dbt project: dbt_project.yml, selectors.yml, models/, macros/, ...

Workspace, warehouse IDs and the SQL endpoint are looked up by name at run time and never stored
in git. deploy.py (phase 7) imports job_definition() and ensure_dbt_job() to publish the same item
to Test and Prod.

The job only passes select / exclude / threads to dbt: a `selector` argument is silently dropped
(docs/SPIKE.md item 12). Without --select it builds everything, which is what selector nightly means.

Steps:
  1. Build the definition from dbt/ (profiles.yml, target/, logs/, dbt_packages/ are left out).
  2. Create dbt_lpb, or replace its definition (the whole project is replaced, so deleted files go).
  3. With --run: run it (job scheduler, jobType=Execute), wait, and read the run's dbt.log and
     run_results.json from the item's OneLake folder Output/<job instance id>/.

The job reports Failed whenever dbt exits non-zero, which includes failing tests. With the planted
defects ("act 1") a build therefore ends Failed with 2 test failures; the summary shows the counts.

Ends with a one-line JSON summary on stdout. Progress goes to stderr.
"""
from __future__ import annotations

import argparse
import base64
import collections
import json
import time

from fabric_common import (REPO_ROOT, FabricClient, FabricError, OneLake, TokenCache, env_config,
                           finish, log)

DBT_DIR = REPO_ROOT / "dbt"
PROJECT_FOLDER = "dbt"                      # Code/<folder> inside the item
PROJECT_FILES = ["dbt_project.yml", "selectors.yml", "packages.yml"]
PROJECT_DIRS = ["models", "macros", "seeds", "snapshots", "tests", "analyses"]
OPERATIONS = ["build", "run", "test", "seed", "snapshot", "compile", "show"]   # what the job accepts


def project_files() -> dict[str, bytes]:
    """dbt project files by path relative to dbt/, without profiles, build output or placeholders."""
    files = {name: (DBT_DIR / name).read_bytes() for name in PROJECT_FILES if (DBT_DIR / name).exists()}
    for d in PROJECT_DIRS:
        for p in sorted((DBT_DIR / d).rglob("*")) if (DBT_DIR / d).exists() else []:
            if p.is_file() and p.name != ".gitkeep":
                files[p.relative_to(DBT_DIR).as_posix()] = p.read_bytes()
    if "dbt_project.yml" not in files:
        raise FabricError("dbt/dbt_project.yml not found")
    return files


def _part(path: str, data: bytes) -> dict:
    return {"path": path, "payloadType": "InlineBase64", "payload": base64.b64encode(data).decode("ascii")}


def job_content(ws_id: str, warehouse: dict, operation: str, arguments: dict) -> dict:
    """dbt-content.json, in the shape the Fabric UI writes. `warehouse` is the full item (with
    properties.connectionString)."""
    return {
        "project": {"projectType": "OneLake", "folderPath": PROJECT_FOLDER},
        "profile": {
            "profileType": "DataWarehouse",
            "schema": "dbo",                # same as dbt/profiles.yml; models set their own schemas
            "externalReferences": {},
            "connectionSettings": {
                "name": warehouse["displayName"],
                "properties": {
                    "type": "DataWarehouse",
                    "typeProperties": {
                        "workspaceId": ws_id,
                        "artifactId": warehouse["id"],
                        "endPoint": warehouse["properties"]["connectionString"],
                    },
                },
            },
        },
        "command": {"operation": operation, "arguments": arguments},
    }


def job_definition(ws_id: str, warehouse: dict, operation: str = "build", arguments: dict | None = None) -> dict:
    content = job_content(ws_id, warehouse, operation, arguments or {})
    parts = [_part("dbt-content.json", json.dumps(content, indent=2).encode("utf-8"))]
    parts += [_part(f"Code/{PROJECT_FOLDER}/{rel}", data) for rel, data in project_files().items()]
    return {"parts": parts}


def ensure_dbt_job(client: FabricClient, ws_id: str, name: str, definition: dict) -> tuple[dict, str]:
    job = client.item(ws_id, "items", name, required=False)
    if job is not None and job.get("type") != "DataBuildToolJob":
        raise FabricError(f"item {name} exists but is a {job.get('type')}, not a DataBuildToolJob")
    if job is None:
        log(f"Creating dbt job {name}")
        client.request("POST", f"workspaces/{ws_id}/items",
                       json_body={"displayName": name, "type": "DataBuildToolJob", "definition": definition})
        return client.item(ws_id, "items", name), "created"
    log(f"Updating dbt job {name}")
    client.request("POST", f"workspaces/{ws_id}/items/{job['id']}/updateDefinition",
                   json_body={"definition": definition})
    return job, "updated"


def run_dbt_job(client: FabricClient, ws_id: str, job_id: str, timeout_s: int) -> dict:
    resp = client.request("POST", f"workspaces/{ws_id}/items/{job_id}/jobs/instances",
                          params={"jobType": "Execute"}, lro=False)
    location = resp.get("_location")
    if not location:
        raise FabricError(f"dbt job run returned no job location: {resp}")
    started = time.time()
    status = "NotStarted"
    while time.time() - started < timeout_s:
        time.sleep(15)
        job = client.request("GET", location, lro=False)
        if job.get("status") != status:
            status = job.get("status")
            log(f"  dbt job {status} after {int(time.time() - started)}s")
        if status in ("Completed", "Failed", "Cancelled", "Deduped"):
            job["_seconds"] = int(time.time() - started)
            job["_instance_id"] = location.rstrip("/").split("/")[-1]
            return job
    raise FabricError(f"dbt job still {status} after {timeout_s}s (job {location})")


def read_run_output(lake: OneLake, job_id: str, instance_id: str) -> dict:
    """dbt version and result counts from Output/<instance>/ (logs/dbt.log, target/run_results.json)."""
    out: dict = {"output_folder": f"Output/{instance_id}"}
    log_raw = lake.read(f"{job_id}/Output/{instance_id}/logs/dbt.log")
    if log_raw:
        for line in log_raw.decode("utf-8", "replace").splitlines():
            try:
                msg = json.loads(line).get("info", {}).get("msg", "")
            except json.JSONDecodeError:
                continue
            if msg.startswith("Running with dbt="):
                out["dbt"] = msg.split("=", 1)[1]
            elif msg.startswith("Registered adapter:"):
                out["adapter"] = msg.split(":", 1)[1].strip()
    rr_raw = lake.read(f"{job_id}/Output/{instance_id}/target/run_results.json")
    if rr_raw:
        rr = json.loads(rr_raw)
        out["command"] = rr.get("args", {}).get("invocation_command")
        out["elapsed_s"] = round(rr.get("elapsed_time", 0), 1)
        out["results"] = dict(collections.Counter(r["status"] for r in rr.get("results", [])))
        out["problems"] = [{"status": r["status"], "node": r["unique_id"], "message": (r.get("message") or "")[:200]}
                           for r in rr.get("results", []) if r["status"] in ("error", "fail", "runtime error")]
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True, choices=["dev", "test", "prod"])
    ap.add_argument("--operation", default="build", choices=OPERATIONS)
    ap.add_argument("--select", default="", help="dbt --select (default: everything, like selector nightly)")
    ap.add_argument("--exclude", default="", help="dbt --exclude")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--run", action="store_true", help="run the job after deploying it, and wait")
    ap.add_argument("--timeout", type=int, default=1800, help="seconds to wait for the run")
    args = ap.parse_args()

    cfg = env_config(args.env)
    summary: dict = {"ok": False, "env": args.env, "workspace": cfg["workspace"]}
    try:
        tokens = TokenCache()
        client = FabricClient(tokens)
        ws = client.workspace(cfg["workspace"])
        wh_item = client.item(ws["id"], "warehouses", cfg["items"]["warehouse"])
        warehouse = client.request("GET", f"workspaces/{ws['id']}/warehouses/{wh_item['id']}", lro=False)
        if not (warehouse.get("properties") or {}).get("connectionString"):
            raise FabricError("warehouse has no SQL endpoint yet")

        arguments: dict = {"threads": args.threads}
        if args.select:
            arguments["select"] = args.select
        arguments["exclude"] = args.exclude      # the UI always writes it, empty by default
        definition = job_definition(ws["id"], warehouse, args.operation, arguments)
        name = cfg["items"]["dbt_job"]
        job, action = ensure_dbt_job(client, ws["id"], name, definition)
        summary["job"] = {"name": name, "action": action, "files": len(definition["parts"]) - 1,
                          "command": {"operation": args.operation, "arguments": arguments}}

        if args.run:
            log(f"Running {name}")
            run = run_dbt_job(client, ws["id"], job["id"], args.timeout)
            summary["run"] = {"status": run.get("status"), "seconds": run["_seconds"]}
            summary["run"].update(read_run_output(OneLake(ws["id"], tokens), job["id"], run["_instance_id"]))
            if run.get("status") != "Completed":
                reason = (run.get("failureReason") or {}).get("message", "")
                summary["error"] = f"dbt job {run.get('status')}: {reason[:300]}"
            summary["ok"] = run.get("status") == "Completed"
        else:
            summary["ok"] = True
    except FabricError as e:
        summary["error"] = str(e)
    finish(summary)


if __name__ == "__main__":
    main()
