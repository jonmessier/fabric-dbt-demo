"""Deploy the Power BI layer: the Direct Lake semantic model and the report on top of it.

    python scripts/powerbi.py --env dev                 # deploy model + report, refresh (frame) the model
    python scripts/powerbi.py --env dev --skip-refresh
    python scripts/powerbi.py --env dev --export        # pull the report back from Fabric into powerbi/

Source files (the source of truth, in git):
  powerbi/LPB Analytics.SemanticModel/      TMDL: tables, measures, relationships. Direct Lake on OneLake over
                                            the gold schema of wh_analytics. expressions.tmdl holds the
                                            placeholders {{WORKSPACE_ID}} and {{WAREHOUSE_ID}}, filled in here.
  powerbi/LPB Sales and Distribution.Report/  PBIR: one JSON file per page and per visual, plus the theme in
                                            StaticResources/RegisteredResources/LarkspurPine.json.

Deploy steps (everything looked up by name; no IDs in git):
  1. Create or update the semantic model from its TMDL files.
  2. Refresh it. For Direct Lake that is "framing": point the model at the latest version of each Delta
     table. It takes seconds. Fabric also reframes automatically when the tables change.
  3. Create or update the report. In git, definition.pbir points at the model by folder (byPath, what
     Power BI Desktop writes); the Fabric API needs a connection to the deployed model (byConnection), so
     that file is rewritten on the way up.

--export: style the report in the browser (Fabric > report > Edit), then run this to write the service's
version back into powerbi/<report>.Report so the change lands in git. definition.pbir keeps its byPath form.

Ends with a one-line JSON summary on stdout. Progress goes to stderr.
"""
from __future__ import annotations

import argparse
import base64
import json
import shutil
import time
from pathlib import Path

from fabric_common import REPO_ROOT, FabricClient, FabricError, TokenCache, env_config, finish, log

POWERBI_DIR = REPO_ROOT / "powerbi"
MODEL_NAME = "LPB Analytics"
REPORT_NAME = "LPB Sales and Distribution"
MODEL_DIR = POWERBI_DIR / f"{MODEL_NAME}.SemanticModel"
REPORT_DIR = POWERBI_DIR / f"{REPORT_NAME}.Report"

PBI_API = "https://api.powerbi.com/v1.0/myorg"
PBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"


# --------------------------------------------------------------------------- definition parts

def read_parts(folder: Path, replace: dict[str, str] | None = None, skip: set[str] | None = None) -> list[dict]:
    """Every file under folder as a Fabric definition part (path relative to the folder, base64 payload).
    replace: text substitutions applied to every text file (the ID placeholders)."""
    parts = []
    for f in sorted(folder.rglob("*")):
        rel = f.relative_to(folder).as_posix()
        if not f.is_file() or rel in (skip or set()) or f.name.startswith(".") and f.name != ".platform":
            continue
        data = f.read_bytes()
        if replace and f.suffix in (".tmdl", ".json", ".pbism", ".pbir"):
            text = data.decode("utf-8")
            for k, v in replace.items():
                text = text.replace(k, v)
            data = text.encode("utf-8")
        parts.append({"path": rel, "payload": base64.b64encode(data).decode("ascii"), "payloadType": "InlineBase64"})
    return parts


def text_part(path: str, content: dict) -> dict:
    return {"path": path, "payloadType": "InlineBase64",
            "payload": base64.b64encode(json.dumps(content, indent=2).encode("utf-8")).decode("ascii")}


def upsert(client: FabricClient, ws_id: str, collection: str, name: str, definition: dict) -> tuple[dict, str]:
    """Create the item with this definition, or replace the definition of the existing one."""
    item = client.item(ws_id, collection, name, required=False)
    if item is None:
        log(f"Creating {collection[:-1]} {name}")
        client.request("POST", f"workspaces/{ws_id}/{collection}",
                       json_body={"displayName": name, "definition": definition})
        return client.item(ws_id, collection, name), "created"
    log(f"Updating {collection[:-1]} {name}")
    client.request("POST", f"workspaces/{ws_id}/{collection}/{item['id']}/updateDefinition",
                   json_body={"definition": definition})
    return item, "updated"


# --------------------------------------------------------------------------- refresh (Power BI REST API)

def refresh_model(client: FabricClient, ws_id: str, model_id: str, timeout_s: int = 600) -> dict:
    """Start a refresh and wait for it. For Direct Lake this only reframes; no data is copied."""
    def headers() -> dict:
        return {"Authorization": f"Bearer {client.tokens.get(PBI_SCOPE)}"}

    url = f"{PBI_API}/groups/{ws_id}/datasets/{model_id}/refreshes"
    resp = client.session.post(url, headers=headers(), json={"notifyOption": "NoNotification"}, timeout=60)
    if resp.status_code not in (200, 202):
        raise FabricError(f"refresh request -> {resp.status_code}: {resp.text[:600]}")
    # The refresh history also lists earlier refreshes; follow ours by its request id.
    request_id = resp.headers.get("RequestId") or resp.headers.get("x-ms-request-id")
    started = time.time()
    while time.time() - started < timeout_s:
        time.sleep(5)
        resp = client.session.get(url, headers=headers(), params={"$top": 5}, timeout=60)
        runs = resp.json().get("value", []) if resp.ok else []
        run = next((r for r in runs if request_id and r.get("requestId") == request_id), None)
        if run is None and (not request_id or time.time() - started > 60):
            run = runs[0] if runs else None
        status = (run or {}).get("status", "Unknown")
        if status in ("Completed", "Failed", "Cancelled", "Disabled"):
            out = {"status": status, "seconds": int(time.time() - started)}
            if status != "Completed":
                out["error"] = (run.get("serviceExceptionJson") or "")[:600]
            return out
    return {"status": "StillRunning", "seconds": timeout_s}


# --------------------------------------------------------------------------- export

def export_report(client: FabricClient, ws_id: str) -> dict:
    """Write the report's definition from Fabric back into powerbi/<report>.Report."""
    report = client.item(ws_id, "reports", REPORT_NAME, required=False)
    if report is None:
        raise FabricError(f"report {REPORT_NAME} not found. Deploy it first: .\\tasks.ps1 powerbi -Env dev")
    d = client.request("POST", f"workspaces/{ws_id}/reports/{report['id']}/getDefinition",
                       params={"format": "PBIR"}, json_body={})
    parts = d["definition"]["parts"]
    if not any(p["path"].startswith("definition/") for p in parts):
        raise FabricError("Fabric returned the old single-file format (PBIR-Legacy), not PBIR. Nothing written.")
    keep_pbir = (REPORT_DIR / "definition.pbir").read_text(encoding="utf-8")
    # Replace the folder wholesale so pages and visuals deleted in the browser disappear here too.
    shutil.rmtree(REPORT_DIR / "definition", ignore_errors=True)
    written = 0
    for p in parts:
        if p["path"] in ("definition.pbir", ".platform"):
            continue  # keep the byPath reference used in git
        target = REPORT_DIR / p["path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(base64.b64decode(p["payload"]))
        written += 1
    (REPORT_DIR / "definition.pbir").write_text(keep_pbir, encoding="utf-8")
    return {"report": REPORT_NAME, "files_written": written, "folder": str(REPORT_DIR.relative_to(REPO_ROOT))}


# --------------------------------------------------------------------------- main

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True, choices=["dev", "test", "prod"])
    ap.add_argument("--skip-refresh", action="store_true", help="don't refresh (frame) the model after deploying")
    ap.add_argument("--export", action="store_true",
                    help="download the report from Fabric into powerbi/ instead of deploying")
    args = ap.parse_args()

    cfg = env_config(args.env)
    summary: dict = {"ok": False, "env": args.env, "workspace": cfg["workspace"]}
    try:
        client = FabricClient(TokenCache())
        ws = client.workspace(cfg["workspace"])

        if args.export:
            summary["export"] = export_report(client, ws["id"])
            summary["ok"] = True
            finish(summary)

        wh = client.item(ws["id"], "warehouses", cfg["items"]["warehouse"])

        # 1. Semantic model, with the workspace and warehouse IDs filled into expressions.tmdl.
        model_parts = read_parts(MODEL_DIR, replace={"{{WORKSPACE_ID}}": ws["id"], "{{WAREHOUSE_ID}}": wh["id"]})
        model, a1 = upsert(client, ws["id"], "semanticModels", MODEL_NAME,
                           {"format": "TMDL", "parts": model_parts})
        summary["semantic_model"] = {"name": MODEL_NAME, "action": a1, "files": len(model_parts)}

        # 2. Frame it, so the report has data straight away.
        if not args.skip_refresh:
            log(f"Refreshing {MODEL_NAME}")
            summary["semantic_model"]["refresh"] = refresh_model(client, ws["id"], model["id"])

        # 3. Report, bound to the deployed model by connection.
        connection = (f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{ws['displayName']};"
                      f"initial catalog={MODEL_NAME};access mode=readonly;integrated security=ClaimsToken;"
                      f"semanticmodelid={model['id']}")
        pbir = json.loads((REPORT_DIR / "definition.pbir").read_text(encoding="utf-8"))
        pbir["datasetReference"] = {"byConnection": {"connectionString": connection}}
        report_parts = read_parts(REPORT_DIR, skip={"definition.pbir"}) + [text_part("definition.pbir", pbir)]
        report, a2 = upsert(client, ws["id"], "reports", REPORT_NAME, {"parts": report_parts})
        summary["report"] = {"name": REPORT_NAME, "action": a2, "files": len(report_parts),
                             "url": f"https://app.fabric.microsoft.com/groups/{ws['id']}/reports/{report['id']}"}

        refresh = summary["semantic_model"].get("refresh", {})
        summary["ok"] = args.skip_refresh or refresh.get("status") == "Completed"
        if not summary["ok"]:
            summary["error"] = (f"model refresh {refresh.get('status')}. If it mentions credentials, open the model "
                                "in Fabric > Settings > Data source credentials > Edit credentials (OAuth2), then "
                                "run this again.")
    except FabricError as e:
        summary["error"] = str(e)
    finish(summary)


if __name__ == "__main__":
    main()
