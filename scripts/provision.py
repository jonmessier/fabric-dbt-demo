"""Create (or confirm) the Fabric workspace and items for one environment.

    python scripts/provision.py --env dev [--capacity "<capacity display name>"]

Creates, by name, whatever is missing:
  * workspace  (config/environments.yml: environments.<env>.workspace) on the capacity
               FABRIC_CAPACITY_NAME from .env (or --capacity, which is then saved to .env)
  * lakehouse  items.lakehouse  (lh_bronze), with lakehouse schemas OFF
  * warehouse  items.warehouse  (wh_analytics)
  * warehouse schemas  warehouse_schemas (ref, silver, snapshots, gold, audit)

Idempotent: a second run changes nothing and reports every step as "exists".
For --env dev it writes the warehouse SQL endpoint into .env as FABRIC_SQL_ENDPOINT (and
FABRIC_TENANT_ID if it is empty). Other environments only print it.

Ends with a one-line JSON summary on stdout. Progress goes to stderr.
"""
from __future__ import annotations

import argparse
import time

from fabric_common import (FabricClient, FabricError, TokenCache, env_config, finish, log,
                           read_env_file, set_env_value, sql_connect)


def resolve_capacity(client: FabricClient, name: str | None) -> dict:
    caps = list(client.list("capacities"))
    if not name:
        listing = ", ".join(f"{c['displayName']} ({c['sku']}, {c['region']}, {c['state']})" for c in caps)
        raise FabricError(f"FABRIC_CAPACITY_NAME is not set in .env. Pass --capacity or set it. "
                          f"Available: {listing}")
    match = [c for c in caps if c["displayName"] == name]
    if not match:
        raise FabricError(f"capacity '{name}' not found or not visible to you. "
                          f"Available: {[c['displayName'] for c in caps]}")
    if match[0]["state"] != "Active":
        raise FabricError(f"capacity '{name}' is {match[0]['state']}, not Active")
    return match[0]


def ensure_workspace(client: FabricClient, name: str, capacity: dict, steps: dict) -> dict:
    ws = client.workspace(name, required=False)
    if ws is None:
        log(f"Creating workspace {name}")
        ws = client.request("POST", "workspaces", json_body={
            "displayName": name,
            "description": "Larkspur & Pine dbt-on-Fabric demo. Managed by scripts/provision.py.",
            "capacityId": capacity["id"]})
        steps["workspace"] = "created"
    else:
        steps["workspace"] = "exists"
        if ws.get("capacityId", "").lower() != capacity["id"].lower():
            log(f"Assigning workspace {name} to capacity {capacity['displayName']}")
            client.request("POST", f"workspaces/{ws['id']}/assignToCapacity",
                           json_body={"capacityId": capacity["id"]})
            steps["workspace"] = "exists, reassigned to capacity"
    return ws


def ensure_lakehouse(client: FabricClient, ws_id: str, name: str, steps: dict) -> dict:
    lh = client.item(ws_id, "lakehouses", name, required=False)
    if lh is None:
        log(f"Creating lakehouse {name} (schemas off)")
        # No creationPayload => a lakehouse without schemas (enableSchemas would turn them on).
        client.request("POST", f"workspaces/{ws_id}/lakehouses", json_body={"displayName": name})
        steps["lakehouse"] = "created"
    else:
        steps["lakehouse"] = "exists"
    lh = client.request("GET", f"workspaces/{ws_id}/lakehouses/"
                               f"{client.item(ws_id, 'lakehouses', name)['id']}", lro=False)
    if (lh.get("properties") or {}).get("defaultSchema"):
        raise FabricError(f"lakehouse {name} exists with schemas enabled; the bronze contract "
                          f"(docs/BRONZE.md) and dbt sources expect schemas off")
    return lh


def ensure_warehouse(client: FabricClient, ws_id: str, name: str, steps: dict) -> dict:
    wh = client.item(ws_id, "warehouses", name, required=False)
    if wh is None:
        log(f"Creating warehouse {name}")
        client.request("POST", f"workspaces/{ws_id}/warehouses", json_body={"displayName": name})
        steps["warehouse"] = "created"
    else:
        steps["warehouse"] = "exists"
    wh_id = client.item(ws_id, "warehouses", name)["id"]
    for _ in range(40):  # the connection string can lag a little behind creation
        wh = client.request("GET", f"workspaces/{ws_id}/warehouses/{wh_id}", lro=False)
        if (wh.get("properties") or {}).get("connectionString"):
            return wh
        time.sleep(5)
    raise FabricError(f"warehouse {name} has no SQL connection string yet; rerun in a minute")


def ensure_schemas(server: str, database: str, schemas: list[str], tokens: TokenCache) -> dict:
    result = {}
    last_error = None
    for attempt in range(6):  # a brand-new warehouse can refuse logins for a short while
        try:
            conn = sql_connect(server, database, tokens)
            break
        except Exception as e:  # noqa: BLE001  (driver raises several types)
            last_error = e
            log(f"  warehouse not accepting connections yet ({type(e).__name__}); retrying")
            time.sleep(15)
    else:
        raise FabricError(f"cannot connect to {database} at {server}: {last_error}")
    try:
        cur = conn.cursor()
        for schema in schemas:
            cur.execute("SELECT 1 FROM sys.schemas WHERE name = ?", (schema,))
            if cur.fetchone():
                result[schema] = "exists"
            else:
                cur.execute(f"CREATE SCHEMA [{schema}]")
                result[schema] = "created"
    finally:
        conn.close()
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True, choices=["dev", "test", "prod"])
    ap.add_argument("--capacity", help="capacity display name; saved to .env as FABRIC_CAPACITY_NAME")
    args = ap.parse_args()

    cfg = env_config(args.env)
    steps: dict = {}
    summary: dict = {"ok": False, "env": args.env, "workspace": cfg["workspace"], "steps": steps}
    try:
        dotenv = read_env_file()
        if args.capacity and args.capacity != dotenv.get("FABRIC_CAPACITY_NAME"):
            set_env_value("FABRIC_CAPACITY_NAME", args.capacity)
            steps["env_capacity"] = "written"
        capacity_name = args.capacity or dotenv.get("FABRIC_CAPACITY_NAME")

        tokens = TokenCache()
        client = FabricClient(tokens)
        capacity = resolve_capacity(client, capacity_name)
        summary["capacity"] = capacity["displayName"]

        ws = ensure_workspace(client, cfg["workspace"], capacity, steps)
        ensure_lakehouse(client, ws["id"], cfg["items"]["lakehouse"], steps)
        wh = ensure_warehouse(client, ws["id"], cfg["items"]["warehouse"], steps)
        endpoint = wh["properties"]["connectionString"]
        summary["sql_endpoint"] = endpoint

        steps["schemas"] = ensure_schemas(endpoint, cfg["items"]["warehouse"],
                                          cfg["warehouse_schemas"], tokens)

        if args.env == "dev":
            changed = set_env_value("FABRIC_SQL_ENDPOINT", endpoint)
            steps["env_sql_endpoint"] = "written" if changed else "unchanged"
            if not dotenv.get("FABRIC_TENANT_ID"):
                set_env_value("FABRIC_TENANT_ID", tokens.tenant_id())
                steps["env_tenant_id"] = "written"
        summary["ok"] = True
    except FabricError as e:
        summary["error"] = str(e)
    finish(summary)


if __name__ == "__main__":
    main()
