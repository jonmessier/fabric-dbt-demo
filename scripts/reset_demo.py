"""Put an environment's Fabric data back to the demo's starting state ("act 1").

    python scripts/reset_demo.py --env dev --confirm [--allow-prod]

Run `python scripts/generate_data.py backfill --force` first, so data/landing holds only the backfill
(`.\\tasks.ps1 reset` does both, then rebuilds with dbt). This script:

  1. Deletes lh_bronze Files/landing and Files/_runs, and uploads data/landing again, so the
     days added with advance-day disappear from the lakehouse too.
  2. Runs nb_ingest_bronze with reload_all: every bronze table and _ingest_log is dropped and the
     landing zone loaded again. Then refreshes the SQL endpoint metadata.
  3. Drops every table and view in the warehouse schemas dbt builds (config warehouse_schemas: ref, silver,
     snapshots, gold, audit), keeping the schemas. This removes snapshot history, the incremental fact and
     audit.dbt_run_log, so the next build starts from scratch.

Fabric items (notebooks, dbt job, pipeline) are left as they are. Destructive: requires --confirm,
and refuses prod without --allow-prod. Ends with a one-line JSON summary on stdout.
"""
from __future__ import annotations

import argparse
import json
import uuid

from fabric_common import (REPO_ROOT, FabricClient, FabricError, OneLake, TokenCache, env_config,
                           finish, log, sql_connect)
from load_bronze import (NOTEBOOK_SRC, deploy_notebook, notebook_source, run_notebook,
                         sync_sql_endpoint, upload_landing)

STATE = REPO_ROOT / "data" / "landing" / "_state.json"


def check_landing() -> dict:
    if not STATE.exists():
        raise FabricError("data/landing not found. Run: python scripts/generate_data.py backfill --force")
    state = json.loads(STATE.read_text())
    if state["business_date"] != state["backfill_date"]:
        raise FabricError(f"data/landing has advanced days (business date {state['business_date']}, backfill "
                          f"{state['backfill_date']}). Run: python scripts/generate_data.py backfill --force")
    return state


def reset_lakehouse_files(lake: OneLake, lakehouse_id: str, landing_path: str) -> list[str]:
    removed = []
    for folder in (landing_path.strip("/"), "Files/_runs"):
        if lake.delete(f"{lakehouse_id}/{folder}"):
            removed.append(folder)
    return removed


def drop_warehouse_objects(server: str, database: str, schemas: list[str], tokens: TokenCache) -> dict:
    conn = sql_connect(server, database, tokens)
    cur = conn.cursor()
    marks = ",".join("?" * len(schemas))
    cur.execute(f"""select s.name, o.name, o.type from sys.objects o join sys.schemas s on s.schema_id = o.schema_id
                    where o.type in ('U', 'V') and s.name in ({marks})""", schemas)
    objects = cur.fetchall()
    dropped = {"views": 0, "tables": 0}
    for kind, sql_kind, key in (("V", "view", "views"), ("U", "table", "tables")):
        for schema, name, typ in objects:
            if typ.strip() == kind:
                cur.execute(f"drop {sql_kind} if exists [{schema}].[{name}]")
                dropped[key] += 1
    conn.close()
    return dropped


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True, choices=["dev", "test", "prod"])
    ap.add_argument("--confirm", action="store_true", help="required: this deletes data")
    ap.add_argument("--allow-prod", action="store_true", help="required as well for --env prod")
    ap.add_argument("--timeout", type=int, default=1800, help="seconds to wait for the notebook")
    args = ap.parse_args()

    cfg = env_config(args.env)
    summary: dict = {"ok": False, "env": args.env, "workspace": cfg["workspace"]}
    try:
        if not args.confirm:
            raise FabricError("reset deletes bronze data and every dbt-built table; pass --confirm")
        if args.env == "prod" and not args.allow_prod:
            raise FabricError("refusing to reset prod without --allow-prod")
        state = check_landing()
        summary["business_date"] = state["business_date"]

        tokens = TokenCache()
        client = FabricClient(tokens)
        ws = client.workspace(cfg["workspace"])
        lh = client.item(ws["id"], "lakehouses", cfg["items"]["lakehouse"])
        wh_item = client.item(ws["id"], "warehouses", cfg["items"]["warehouse"])
        wh = client.request("GET", f"workspaces/{ws['id']}/warehouses/{wh_item['id']}", lro=False)
        lake = OneLake(ws["id"], tokens)

        log("Replacing the lakehouse landing zone")
        summary["deleted_folders"] = reset_lakehouse_files(lake, lh["id"], cfg["landing_path"])
        summary["upload"] = upload_landing(lake, lh["id"], cfg["landing_path"])

        nb, _ = deploy_notebook(client, ws["id"], cfg["items"]["ingest_notebook"],
                                notebook_source(NOTEBOOK_SRC, ws["id"], lh))
        run_id = uuid.uuid4().hex
        log(f"Reloading bronze (reload_all, run_id {run_id})")
        job = run_notebook(client, ws["id"], nb["id"], {"run_id": run_id, "reload_all": True}, args.timeout)
        if job.get("status") != "Completed":
            raise FabricError(f"notebook {job.get('status')}: {json.dumps(job.get('failureReason'))}")
        raw = lake.read(f"{lh['id']}/Files/_runs/{run_id}.json")
        ingest = json.loads(raw) if raw else {}
        summary["bronze"] = {"files": ingest.get("files"), "rows": ingest.get("rows"), "seconds": job["_seconds"]}
        summary["sql_endpoint_sync"] = sync_sql_endpoint(client, ws["id"], lh["id"])

        log("Dropping dbt-built tables and views")
        summary["warehouse"] = drop_warehouse_objects(wh["properties"]["connectionString"], wh["displayName"],
                                                      cfg["warehouse_schemas"], tokens)
        summary["ok"] = bool(ingest.get("ok"))
    except FabricError as e:
        summary["error"] = str(e)
    finish(summary)


if __name__ == "__main__":
    main()
