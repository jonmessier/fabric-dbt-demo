"""Build an interactive lineage page for the Larkspur & Pine dbt project.

Reads dbt's own artifacts instead of parsing SQL:
  dbt/target/manifest.json        every node, its parents, tests, unit tests and the exposure
  dbt/target/run_results*.json    latest status of each model, seed, snapshot and test
  dbt/target/run_history/*.json   (written by tasks.ps1 after every dbt command)
  dbt/target/sources.json         source freshness results, if `dbt source freshness` has run

and writes one self-contained HTML file (no CDN, works offline and on a projector):
  docs/lineage/dbt_lineage.html

Usage:
  python scripts/build_lineage.py
  python scripts/build_lineage.py --target dbt/target --out docs/lineage/dbt_lineage.html
  python scripts/build_lineage.py --no-results        # structure only, no test status

Run `dbt parse` first if target/manifest.json is missing (.\\tasks.ps1 lineage does that).
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PLACEHOLDER = "/*DATA*/"

# Source systems as the business knows them. Keys are the dbt source names.
SYSTEMS = {
    "erp": "ERP (orders, invoices, shipments)",
    "pos": "Taproom POS",
    "dist": "Distributor depletion reports",
    "prod": "Brewhouse production",
}

# Column order on the page. Each node is placed by kind and folder.
COLUMNS = [
    ("bronze", "Bronze", "lh_bronze (raw, append-only)"),
    ("staging", "Silver: staging", "one view per source table"),
    ("intermediate", "Silver: intermediate", "joins, unions, mapping, snapshot"),
    ("dims", "Gold: dimensions", "contracts enforced"),
    ("facts", "Gold: facts", "contracts enforced"),
    ("exposure", "Report layer", "semantic model"),
]

# Worst-first ranking used to roll test results up to a model.
STATUS_RANK = {"error": 5, "fail": 4, "runtime error": 4, "warn": 3, "skipped": 2, "pass": 1, "success": 1}


def load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def column_for(node: dict) -> str:
    rt = node["resource_type"]
    if rt in ("source", "seed"):
        return "bronze"
    if rt == "snapshot":
        return "intermediate"
    if rt == "exposure":
        return "exposure"
    folders = node.get("fqn", [])[1:-1]
    if "staging" in folders:
        return "staging"
    if "intermediate" in folders:
        return "intermediate"
    if node["name"].startswith("dim_"):
        return "dims"
    return "facts"


def group_for(node: dict) -> str:
    rt = node["resource_type"]
    if rt == "source":
        return node["source_name"]
    if rt == "seed":
        return "seeds"
    folders = node.get("fqn", [])[1:-1]
    return folders[-1] if folders else rt


def selector_for(node: dict) -> str:
    """The name dbt's --select accepts for this node."""
    if node["resource_type"] == "source":
        return f"source:{node['source_name']}.{node['name']}"
    if node["resource_type"] == "exposure":
        return f"exposure:{node['name']}"
    return node["name"]


def collect_results(target: Path) -> tuple[dict, list]:
    """Latest result per unique_id across run_results.json and run_history/*.json."""
    files = []
    if (target / "run_results.json").exists():
        files.append(target / "run_results.json")
    hist = target / "run_history"
    if hist.is_dir():
        files.extend(sorted(hist.glob("*.json")))
    latest: dict[str, dict] = {}
    runs = []
    seen = set()
    for f in files:
        try:
            rr = load_json(f)
        except (OSError, json.JSONDecodeError) as e:
            print(f"  skipped {f.name}: {e}", file=sys.stderr)
            continue
        at = rr["metadata"].get("generated_at", "")
        cmd = rr.get("args", {}).get("invocation_command") or rr.get("args", {}).get("which", "")
        key = (at, cmd)
        if key in seen:  # run_results.json is also the newest history file
            continue
        seen.add(key)
        runs.append({"at": at, "command": cmd, "file": f.name, "results": len(rr.get("results", []))})
        for r in rr.get("results", []):
            uid = r["unique_id"]
            if uid not in latest or latest[uid]["at"] <= at:
                latest[uid] = {
                    "status": r["status"],
                    "failures": r.get("failures"),
                    "message": (r.get("message") or "")[:400],
                    "at": at,
                }
    runs.sort(key=lambda r: r["at"])
    return latest, runs


def collect_freshness(target: Path) -> dict:
    f = target / "sources.json"
    if not f.exists():
        return {}
    data = load_json(f)
    out = {}
    for r in data.get("results", []):
        out[r["unique_id"]] = {
            "status": r.get("status"),  # pass / warn / error / runtime error
            "max_loaded_at": r.get("max_loaded_at"),
            "age_hours": round(r["max_loaded_at_time_ago_in_s"] / 3600, 1) if r.get("max_loaded_at_time_ago_in_s") else None,
            "at": data["metadata"].get("generated_at", ""),
        }
    return out


def build_graph(manifest: dict, results: dict, freshness: dict) -> dict:
    nodes: dict[str, dict] = {}

    def add(uid: str, n: dict, parents: list[str]) -> None:
        cfg = n.get("config", {})
        raw = n.get("raw_code") or ""
        nodes[uid] = {
            "id": uid,
            "name": n.get("label") if n["resource_type"] == "exposure" and n.get("label") else n["name"],
            # exposure boxes show the dbt name; the label goes in the side panel
            "short": n["name"],
            "kind": n["resource_type"],
            "col": column_for(n),
            "group": group_for(n),
            "system": n.get("source_name") if n["resource_type"] == "source" else None,
            "select": selector_for(n),
            "materialized": cfg.get("materialized") or ("table" if n["resource_type"] == "source" else n["resource_type"]),
            "relation": n.get("relation_name") or "",
            "path": n.get("original_file_path", ""),
            "description": (n.get("description") or "").strip(),
            "columns": len(n.get("columns") or {}),
            "contract": bool((cfg.get("contract") or {}).get("enforced")),
            "incremental": cfg.get("materialized") == "incremental",
            "strategy": cfg.get("incremental_strategy") or cfg.get("strategy"),
            "freshness": n.get("freshness") if n["resource_type"] == "source" else None,
            "fresh_result": freshness.get(uid),
            "sql": raw if n["resource_type"] in ("model", "snapshot") else "",
            "lines": raw.count("\n") + 1 if raw else 0,
            "parents": sorted(set(parents)),
            "result": results.get(uid),
            "tests": [],
            "unit_tests": [],
        }

    for uid, s in manifest["sources"].items():
        add(uid, s, [])
    for uid, n in manifest["nodes"].items():
        if n["resource_type"] in ("model", "seed", "snapshot"):
            add(uid, n, n.get("depends_on", {}).get("nodes", []))
    for uid, e in manifest.get("exposures", {}).items():
        add(uid, e, e.get("depends_on", {}).get("nodes", []))

    # Data tests: generic tests have attached_node; singular tests attach to every model/seed they read.
    for uid, t in manifest["nodes"].items():
        if t["resource_type"] != "test":
            continue
        if t.get("attached_node"):
            targets = [t["attached_node"]]
        else:
            # Singular test: show it on the models it reads; seeds only when it reads nothing else.
            parents = t.get("depends_on", {}).get("nodes", [])
            targets = [p for p in parents if not p.startswith("seed.")] or parents
        meta = t.get("test_metadata") or {}
        entry = {
            "id": uid,
            "name": t["name"],
            "type": meta.get("name") or "singular",
            "column": t.get("column_name"),
            "severity": (t.get("config", {}).get("severity") or "error").lower(),
            "result": results.get(uid),
            "description": (t.get("description") or "").strip(),
        }
        for tgt in targets:
            if tgt in nodes:
                nodes[tgt]["tests"].append(entry)

    for uid, u in manifest.get("unit_tests", {}).items():
        model_uid = next((p for p in u.get("depends_on", {}).get("nodes", []) if p in nodes), None)
        # depends_on lists the model's parents in some dbt versions; resolve by model name instead.
        model_uid = next((k for k, v in nodes.items() if v["kind"] == "model" and v["short"] == u.get("model")), model_uid)
        if model_uid:
            nodes[model_uid]["unit_tests"].append({
                "id": uid, "name": u["name"], "description": (u.get("description") or "").strip(),
                "result": results.get(uid),
            })

    # Drop dangling parents (macros, disabled nodes) and build children lists.
    for n in nodes.values():
        n["parents"] = [p for p in n["parents"] if p in nodes]
        n["children"] = []
    for n in nodes.values():
        for p in n["parents"]:
            nodes[p]["children"].append(n["id"])
    for n in nodes.values():
        n["children"].sort()
        n["orphan"] = not n["parents"] and not n["children"]

    return nodes


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--target", type=Path, default=REPO / "dbt" / "target", help="dbt target folder")
    ap.add_argument("--template", type=Path, default=REPO / "docs" / "lineage" / "lineage_template.html")
    ap.add_argument("--out", type=Path, default=REPO / "docs" / "lineage" / "dbt_lineage.html")
    ap.add_argument("--no-results", action="store_true", help="ignore run results and freshness")
    ap.add_argument("--title", default="Larkspur & Pine Brewing: dbt lineage")
    args = ap.parse_args()

    manifest_path = args.target / "manifest.json"
    if not manifest_path.exists():
        print(f"{manifest_path} not found. Run `dbt parse` (or .\\tasks.ps1 lineage) first.", file=sys.stderr)
        return 1
    manifest = load_json(manifest_path)
    results, runs = ({}, []) if args.no_results else collect_results(args.target)
    freshness = {} if args.no_results else collect_freshness(args.target)
    nodes = build_graph(manifest, results, freshness)

    meta = manifest["metadata"]
    data = {
        "title": args.title,
        "project": meta.get("project_name") or next(iter(manifest.get("nodes", {}).values()), {}).get("package_name", ""),
        "dbt_version": meta.get("dbt_version"),
        "manifest_at": meta.get("generated_at"),
        "built_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "runs": runs,
        "systems": SYSTEMS,
        "columns": [{"key": k, "title": t, "sub": s} for k, t, s in COLUMNS],
        "nodes": list(nodes.values()),
    }

    template = args.template.read_text(encoding="utf-8")
    if PLACEHOLDER not in template:
        print(f"{args.template} has no {PLACEHOLDER} placeholder.", file=sys.stderr)
        return 1
    # </script> inside SQL or descriptions would end the script block early.
    payload = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")
    html = template.replace(PLACEHOLDER, payload, 1).replace("{{TITLE}}", args.title.replace("&", "&amp;"))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(html, encoding="utf-8")

    kinds = {}
    for n in nodes.values():
        kinds[n["kind"]] = kinds.get(n["kind"], 0) + 1
    ntests = sum(1 for n in manifest["nodes"].values() if n["resource_type"] == "test")
    with_status = sum(1 for n in manifest["nodes"].values() if n["unique_id"] in results)
    print(f"Lineage: {', '.join(f'{v} {k}s' for k, v in sorted(kinds.items()))}, "
          f"{ntests} data tests, {len(manifest.get('unit_tests', {}))} unit tests")
    print(f"Results: {len(runs)} run file(s), status known for {with_status} node(s)/test(s)"
          + (f", freshness for {len(freshness)} source(s)" if freshness else ""))
    print(f"Wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
