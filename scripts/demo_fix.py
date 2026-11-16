"""Act 2 of the demo: fix the planted reference-data problems, one at a time, then undo them for a rehearsal.

    python scripts/demo_fix.py status      which fixes are in the seeds right now
    python scripts/demo_fix.py skus        map Cranberry Bog Sour for Northland and Hawkeye (ref_distributor_sku_xref)
    python scripts/demo_fix.py excise      add North Dakota's excise rate (ref_excise_tax_rate)
    python scripts/demo_fix.py undo        take both out again (back to act 1)

Each fix edits one seed CSV under dbt/seeds and prints the dbt command that rebuilds only what depends on
it, for example `dbt build -s ref_distributor_sku_xref+`. That is the point of the demo: a reference-data
change rebuilds its own path, not the whole warehouse.

The third act 1 error, the 138% batch yield, is fixed by source data, not a seed: the brewery's corrected
packaging run arrives in the next day's production feed (`.\\tasks.ps1 advance-day`, then
`.\\tasks.ps1 bronze -Env dev`, then `.\\tasks.ps1 build -Select "source:prod+"`).

The rows are worked out from the data generator's world (same seed as data/landing/_state.json), so they
always match the generated distributor reports. Ends with a one-line JSON summary on stdout.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SEEDS = REPO_ROOT / "dbt" / "seeds"
STATE = REPO_ROOT / "data" / "landing" / "_state.json"
XREF = SEEDS / "ref_distributor_sku_xref.csv"
EXCISE = SEEDS / "ref_excise_tax_rate.csv"
ND_RATE = {"state_code": "ND", "rate_per_barrel": "4.96", "effective_from": "2025-01-01"}

sys.path.insert(0, str(Path(__file__).resolve().parent))
import generate_data as gen  # noqa: E402  (same folder; standard library only)


def read(path: Path) -> tuple[list[str], list[dict]]:
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return list(r.fieldnames or []), list(r)


def write(path: Path, fields: list[str], rows: list[dict]) -> None:
    # Same writer as the generator (csv module, \r\n line endings), so git shows only the rows that changed.
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def missing_sku_rows() -> list[dict]:
    """The distributor product codes for the late-launched brand, for the two distributors that never got them."""
    if not STATE.exists():
        raise SystemExit(json.dumps({"ok": False, "error": "data/landing/_state.json not found. Run: .\\tasks.ps1 data"}))
    st = json.loads(STATE.read_text())
    world = gen.World(st["seed"], date.fromisoformat(st["start_date"]), date.fromisoformat(st["backfill_date"]),
                      st["clean"])
    rows = []
    for code in ("NORTHLAND", "HAWKEYE"):
        for item_no, sku in sorted(world.dist_sku[code].items()):
            if world.items[item_no]["brand_code"] == gen.NEW_BRAND:
                rows.append({"distributor_code": code, "distributor_sku": sku, "item_no": item_no})
    return rows


def key(r: dict) -> tuple:
    return r["distributor_code"], r["distributor_sku"]


def status() -> dict:
    _, xref = read(XREF)
    have = {key(r) for r in xref}
    need = missing_sku_rows()
    _, excise = read(EXCISE)
    return {"skus": sum(key(r) in have for r in need) == len(need), "skus_rows": len(need),
            "excise": any(r["state_code"] == "ND" for r in excise)}


def fix_skus() -> dict:
    fields, xref = read(XREF)
    have = {key(r) for r in xref}
    add = [r for r in missing_sku_rows() if key(r) not in have]
    write(XREF, fields, xref + add)
    return {"fix": "skus", "file": "dbt/seeds/ref_distributor_sku_xref.csv", "rows_added": len(add),
            "what": "Cranberry Bog Sour mapped for Northland and Hawkeye",
            "next": "dbt build -s ref_distributor_sku_xref+",
            "expect": "warnings gone: unmapped Northland SKUs, unmapped SKU groups in gold"}


def fix_excise() -> dict:
    fields, rows = read(EXCISE)
    added = 0
    if not any(r["state_code"] == "ND" for r in rows):
        rows.append(ND_RATE)
        added = 1
    write(EXCISE, fields, rows)
    return {"fix": "excise", "file": "dbt/seeds/ref_excise_tax_rate.csv", "rows_added": added,
            "what": "North Dakota excise rate added (4.96 per barrel, illustrative)",
            "next": "dbt build -s ref_excise_tax_rate+",
            "expect": "error gone: missing excise rate in gold; warning gone: excise check in silver"}


def undo() -> dict:
    fields, xref = read(XREF)
    drop = {key(r) for r in missing_sku_rows()}
    kept = [r for r in xref if key(r) not in drop]
    write(XREF, fields, kept)
    efields, excise = read(EXCISE)
    ekept = [r for r in excise if r["state_code"] != "ND"]
    write(EXCISE, efields, ekept)
    return {"fix": "undo", "rows_removed": (len(xref) - len(kept)) + (len(excise) - len(ekept)),
            "next": "dbt build -s ref_distributor_sku_xref+ ref_excise_tax_rate+  (or .\\tasks.ps1 reset -Env dev -Yes "
                    "for a full rehearsal reset, which also removes advanced days)"}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["status", "skus", "excise", "undo"])
    args = ap.parse_args()
    out = {"status": lambda: {"applied": status()}, "skus": fix_skus, "excise": fix_excise, "undo": undo}[args.action]()
    print(json.dumps({"ok": True, **out}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
