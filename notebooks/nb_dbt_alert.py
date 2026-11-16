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

# # nb_dbt_alert
#
# Posts the result of the latest dbt run to a Microsoft Teams channel.
#
# **Where it runs:** last step of the pipeline `pl_daily_load`, after the dbt job:
#
#     Ingest bronze -> Check source freshness -> Build with dbt -> Alert Teams   (this notebook)
#
# The alert step runs when the dbt step has *completed*, whether it succeeded or failed. That matters:
# a dbt build with a failing test ends in "Failed", and that is exactly the run we want to hear about.
#
# **Where the results come from:** every dbt run, local or in Fabric, ends with the `on-run-end` hook
# `log_run_results` (`dbt/macros/log_run_results.sql`). It writes one row per model, seed, snapshot
# and test into `wh_analytics.audit.dbt_run_log`. This notebook reads the newest run from that table,
# so the table is the record and Teams is only the notification.
#
# **What it sends:** one Adaptive Card with
# * a headline: `dbt build: 2 errors, 6 warnings, 7 skipped` (or `dbt build passed`)
# * the counts, when the run started and how long it took
# * the errors and warnings, each with what failed (`stg_erp__customer.state_text · relationships · 1 row`),
#   what the test checks (its dbt description; dbt's own message if it has none) and where it is declared
#   with its severity. The hook logs those details with every result.
# * a button that opens the workspace in Fabric.
#
# **Where the Teams address comes from:** a Teams *Workflows* webhook (Teams > channel > ... > Workflows >
# "Send webhook alerts to a channel"). The URL is a secret, so it is not in git or in the pipeline
# definition: `scripts/pipeline.py` copies `TEAMS_WEBHOOK_URL` from `.env` to the lakehouse file
# `Files/_config/teams_webhook_url.txt`, and this notebook reads it from there.
# **No URL = dry run:** the card is built and saved to `Files/_alerts/` but nothing is posted. Handy for
# rehearsing the demo before Teams is set up.
#
# **What it never does:** fail the pipeline because dbt found problems. The dbt step already reports
# that. This notebook only fails if it cannot read the log or Teams rejects the message.
#
# Everything is looked up by name in the current workspace; no IDs are stored here.

# PARAMETERS CELL ********************

# These are the defaults. The pipeline (or scripts/pipeline.py) can override any of them.
warehouse_name = "wh_analytics"          # where audit.dbt_run_log lives
lakehouse_name = "lh_bronze"             # holds Files/_config (webhook URL) and Files/_alerts (dry runs)
notify = "problems"                      # "problems" = only when something warned, failed or was skipped
                                         # "always"   = also post a short "all green" message
max_age_hours = 6                        # a "latest run" older than this means the dbt step never logged
                                         # its results (it failed before on-run-end); we say so loudly
dry_run = False                          # True = build and save the card, but don't post it
pipeline_run_id = ""                     # set by the pipeline (@pipeline().RunId); shown on the card

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---------------------------------------------------------------------------------------------------
# 1. Setup
# ---------------------------------------------------------------------------------------------------
# pyodbc talks to the warehouse through "ODBC Driver 18 for SQL Server", which the Fabric Python runtime
# already has (nb_dbt_freshness relies on the same driver). The pyodbc package itself may not be
# installed, so make sure it is. It is a small wheel: this takes a few seconds.
import subprocess
import sys

subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "--disable-pip-version-check", "pyodbc"],
               check=True)

import json
import struct
import time
from datetime import datetime, timezone
from itertools import chain, repeat

import notebookutils  # Fabric's helper library: tokens, file system, runtime context
import pyodbc
import requests

started = time.time()
API = "https://api.fabric.microsoft.com/v1"
workspace_id = notebookutils.runtime.context["currentWorkspaceId"]


def fabric_api(path):
    """GET a Fabric REST endpoint as this notebook's identity (the pipeline's owner)."""
    headers = {"Authorization": f"Bearer {notebookutils.credentials.getToken('pbi')}"}
    resp = requests.get(f"{API}/{path}", headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json()


def find_item(item_type, name):
    """Look an item up by its display name in this workspace (we never hard-code IDs)."""
    items = fabric_api(f"workspaces/{workspace_id}/items?type={item_type}").get("value", [])
    match = [i for i in items if i["displayName"] == name]
    if not match:
        raise RuntimeError(f"{item_type} '{name}' not found in this workspace")
    return match[0]


# The warehouse's SQL connection string (xxxx.datawarehouse.fabric.microsoft.com) and the lakehouse ID
# (for the OneLake paths below).
warehouse = fabric_api(f"workspaces/{workspace_id}/warehouses/{find_item('Warehouse', warehouse_name)['id']}")
sql_server = warehouse["properties"]["connectionString"]
lakehouse_id = find_item("Lakehouse", lakehouse_name)["id"]
onelake_files = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files"

print(f"Warehouse {warehouse_name}: {sql_server}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---------------------------------------------------------------------------------------------------
# 2. Read the newest dbt run from audit.dbt_run_log
# ---------------------------------------------------------------------------------------------------
def sql_connection():
    """Connect to the warehouse with an Entra token, the same way dbt-fabric does in a notebook.

    ODBC wants the token as a length-prefixed UTF-16-LE byte string, passed in the connection attribute
    SQL_COPT_SS_ACCESS_TOKEN (1256). No passwords anywhere.
    """
    token = notebookutils.credentials.getToken("https://database.windows.net/").encode("utf-8")
    encoded = bytes(chain.from_iterable(zip(token, repeat(0))))
    attrs = {1256: struct.pack("<i", len(encoded)) + encoded}
    conn_str = (f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_server},1433;"
                f"Database={warehouse_name};Encrypt=yes;TrustServerCertificate=no")
    return pyodbc.connect(conn_str, attrs_before=attrs, timeout=60)


with sql_connection() as conn:
    cur = conn.cursor()

    # The latest run = the invocation with the newest start time. dbt stamps every row of a run with the
    # same invocation_id and run_started_at (UTC).
    cur.execute("""
        select top 1 invocation_id, target_name, run_started_at
        from audit.dbt_run_log
        order by run_started_at desc
    """)
    latest = cur.fetchone()
    if latest is None:
        raise RuntimeError("audit.dbt_run_log is empty: has dbt run in this workspace yet?")
    invocation_id, target_name, run_started_at = latest

    # The columns that say what a test is about (parent_model, column_name, test_kind, severity,
    # declared_in, description) were added to the table later; log_run_results adds them on its next run.
    # Until then, and for rows written before that, they read as null and the card falls back to dbt's
    # node name and message.
    DETAIL_COLUMNS = ["parent_model", "column_name", "test_kind", "severity", "declared_in", "description"]
    cur.execute("select name from sys.columns where object_id = object_id('audit.dbt_run_log')")
    present = {r[0] for r in cur.fetchall()}
    details = ", ".join(c if c in present else f"cast(null as varchar(10)) as {c}" for c in DETAIL_COLUMNS)

    # Every node of that run. Hooks (on-run-start/end) are not in the table; tests, models, seeds and
    # snapshots are.
    cur.execute(f"""
        select resource_type, node_name, status, failures, message, execution_time_seconds, {details}
        from audit.dbt_run_log
        where invocation_id = ?
    """, invocation_id)
    columns = [c[0] for c in cur.description]
    rows = [dict(zip(columns, r)) for r in cur.fetchall()]

run_started_utc = run_started_at.replace(tzinfo=timezone.utc)
age_hours = (datetime.now(timezone.utc) - run_started_utc).total_seconds() / 3600
print(f"Latest run {invocation_id} (target {target_name}), started {run_started_utc:%Y-%m-%d %H:%M} UTC, "
      f"{age_hours:.1f} h ago, {len(rows)} nodes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---------------------------------------------------------------------------------------------------
# 3. Summarize the run
# ---------------------------------------------------------------------------------------------------
# dbt's statuses, and how we group them:
#   models / seeds / snapshots: success | error | skipped
#   tests:                      pass | warn | fail | error | skipped
# "fail" is a test that found bad rows with severity error; "error" is something that could not run at all
# (bad SQL, missing table). Both stop the downstream models, so both count as errors here.
# "warn" is a test with severity warn: logged, the run carries on.
# "skipped" is a model or test that did not run because something upstream errored.
ERROR_STATUSES = {"error", "fail", "runtime error"}


def summarize(rows):
    """Count the run's results and pick out what needs attention. Pure Python: easy to test."""
    errors = [r for r in rows if r["status"] in ERROR_STATUSES]
    warnings = [r for r in rows if r["status"] == "warn"]
    skipped = [r for r in rows if r["status"] == "skipped"]
    passed = [r for r in rows if r["status"] in ("pass", "success")]
    seconds = sum(float(r["execution_time_seconds"] or 0) for r in rows)
    return {
        "nodes": len(rows),
        "passed": len(passed),
        "errors": errors,
        "warnings": warnings,
        "skipped": skipped,
        "models_built": sum(1 for r in rows if r["resource_type"] in ("model", "seed", "snapshot")
                            and r["status"] == "success"),
        "tests_run": sum(1 for r in rows if r["resource_type"] in ("test", "unit_test")),
        "seconds": round(seconds),
    }


summary = summarize(rows)
stale = age_hours > max_age_hours
print(json.dumps({k: (len(v) if isinstance(v, list) else v) for k, v in summary.items()}, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---------------------------------------------------------------------------------------------------
# 4. Build the Teams message (an Adaptive Card)
# ---------------------------------------------------------------------------------------------------
# Adaptive Cards are JSON layouts that Teams renders natively. Designer to play with them:
# https://adaptivecards.io/designer (pick "Microsoft Teams" as the host app).
# Version 1.4 is what Teams Workflows webhooks accept reliably.
MAX_LINES = 8  # per section; a card is not a log file. The full list is in audit.dbt_run_log.


def plain_name(node_name):
    """dbt names generic tests like 'relationships_stg_erp__invoice_line_shipment_no__shipment_no__ref_stg_'.
    Long and cryptic; keep it, but cut it to something that fits on a phone."""
    return node_name if len(node_name) <= 70 else node_name[:67] + "..."


def issue_title(r):
    """What failed, in words a reader can place: 'int_batch_summary.yield_pct · between'.

    Built from the columns log_run_results writes: the model the test is on, the tested column (generic
    tests only) and the kind of test. Singular tests have no kind worth showing ("singular"), so they show
    their own name, which is written to be read (assert_no_unmapped_skus_in_gold). Returns None for rows
    logged before those columns existed; the caller then shows dbt's node name as before."""
    kind, model = r.get("test_kind"), r.get("parent_model")
    if not kind:
        return None
    readable = {"singular": r["node_name"], "unit": f"unit test {r['node_name']}"}.get(kind, kind)
    if not model:
        return readable
    target = model + (f".{r['column_name']}" if r.get("column_name") else "")
    return f"{target} · {readable}"


def issue_lines(issues, color):
    """One small block per problem:
         line 1 (bold, colored): what failed, and how many rows
         line 2 (small):         what the test checks (its description), or dbt's message if it has none
         line 3 (small, subtle): where the test is declared and its severity
    Rows without the detail columns (older runs) show dbt's node name and message, as before."""
    blocks = []
    for r in issues[:MAX_LINES]:
        rows_txt = f" · {r['failures']} row{'s' if r['failures'] != 1 else ''}" if r.get("failures") else ""
        title = issue_title(r)
        items = [
            {"type": "RichTextBlock", "inlines": [
                {"type": "TextRun", "text": title or plain_name(r["node_name"]), "weight": "Bolder",
                 "color": color, "size": "Small"},
                {"type": "TextRun", "text": rows_txt, "color": color, "size": "Small"}]},
            {"type": "TextBlock", "text": (r.get("description") or r.get("message") or r["status"])[:400],
             "wrap": True, "size": "Small", "spacing": "None", "isSubtle": not r.get("description")},
        ]
        where = " · ".join(part for part in (
            f"declared in {r['declared_in']}" if r.get("declared_in") else "",
            f"severity {r['severity']}" if r.get("severity") else "") if part)
        if title and where:
            items.append({"type": "TextBlock", "text": where, "wrap": True, "isSubtle": True,
                          "size": "Small", "spacing": "None"})
        blocks.append({"type": "Container", "spacing": "Small", "items": items})
    if len(issues) > MAX_LINES:
        blocks.append({"type": "TextBlock", "size": "Small", "isSubtle": True,
                       "text": f"…and {len(issues) - MAX_LINES} more in audit.dbt_run_log"})
    return blocks


def build_card(summary, *, run_started_utc, invocation_id, target_name, stale, age_hours,
               workspace_url, pipeline_run_id=""):
    n_err, n_warn, n_skip = len(summary["errors"]), len(summary["warnings"]), len(summary["skipped"])

    # Headline and its color: red for errors (or a missing run), amber for warnings, green otherwise.
    if stale:
        headline, color = "dbt build: no new results", "Attention"
    elif n_err or n_warn or n_skip:
        parts = [f"{n} {word}{'s' if n != 1 and word != 'skipped' else ''}"
                 for n, word in ((n_err, "error"), (n_warn, "warning"), (n_skip, "skipped")) if n]
        headline, color = "dbt build: " + ", ".join(parts), "Attention" if n_err else "Warning"
    else:
        headline, color = "dbt build passed", "Good"

    body = [
        {"type": "TextBlock", "text": "Larkspur & Pine · daily load", "isSubtle": True, "size": "Small"},
        {"type": "TextBlock", "text": headline, "weight": "Bolder", "size": "Large", "color": color,
         "wrap": True, "spacing": "None"},
    ]

    if stale:
        # The newest row in the log is old: today's dbt step ended before its on-run-end hook ran
        # (connection failure, compile error, cancelled job). Say that plainly.
        body.append({"type": "TextBlock", "wrap": True, "text":
                     f"The newest run in audit.dbt_run_log started {age_hours:.0f} hours ago, so today's dbt "
                     "step did not log any results. Check the pipeline run in Fabric."})

    body.append({"type": "FactSet", "facts": [
        {"title": "Started", "value": f"{run_started_utc:%a %d %b %Y, %H:%M} UTC"},
        {"title": "Models built", "value": str(summary["models_built"])},
        {"title": "Tests run", "value": str(summary["tests_run"])},
        {"title": "Passed", "value": f"{summary['passed']} of {summary['nodes']}"},
        {"title": "dbt time", "value": f"{summary['seconds'] // 60} min {summary['seconds'] % 60} s"},
        {"title": "Target", "value": str(target_name)},
    ]})

    if n_err:
        body.append({"type": "TextBlock", "text": f"Errors ({n_err}): downstream models were skipped",
                     "weight": "Bolder", "color": "Attention", "spacing": "Medium"})
        body.extend(issue_lines(summary["errors"], "Attention"))
    if n_warn:
        body.append({"type": "TextBlock", "text": f"Warnings ({n_warn}): logged, the build carried on",
                     "weight": "Bolder", "color": "Warning", "spacing": "Medium"})
        body.extend(issue_lines(summary["warnings"], "Warning"))
    if n_skip:
        # Name the skipped models (and the semantic model exposure, if it was skipped); just count the tests.
        built = [r["node_name"] for r in summary["skipped"] if r["resource_type"] not in ("test", "unit_test")]
        n_tests = n_skip - len(built)
        text = ", ".join(built[:10]) + (f" and {len(built) - 10} more" if len(built) > 10 else "")
        if n_tests:
            text += f"{' plus ' if built else ''}{n_tests} test{'s' if n_tests != 1 else ''}"
        body.append({"type": "TextBlock", "text": f"Not built because of the errors: {text}", "wrap": True,
                     "size": "Small", "isSubtle": True, "spacing": "Medium"})

    footer = f"dbt invocation {invocation_id}"
    if pipeline_run_id:
        footer += f" · pipeline run {pipeline_run_id}"
    body.append({"type": "TextBlock", "text": footer, "size": "Small", "isSubtle": True, "wrap": True,
                 "spacing": "Medium"})

    card = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "msteams": {"width": "Full"},
        "body": body,
        "actions": [{"type": "Action.OpenUrl", "title": "Open in Fabric", "url": workspace_url}],
    }
    # A Teams Workflows webhook expects a "message" with the card as an attachment.
    return {"type": "message", "attachments": [
        {"contentType": "application/vnd.microsoft.card.adaptive", "contentUrl": None, "content": card}]}


message = build_card(summary, run_started_utc=run_started_utc, invocation_id=invocation_id,
                     target_name=target_name, stale=stale, age_hours=age_hours,
                     workspace_url=f"https://app.fabric.microsoft.com/groups/{workspace_id}",
                     pipeline_run_id=pipeline_run_id)
headline = message["attachments"][0]["content"]["body"][1]["text"]
print(headline)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---------------------------------------------------------------------------------------------------
# 5. Send it (or save it, on a dry run)
# ---------------------------------------------------------------------------------------------------
has_news = stale or summary["errors"] or summary["warnings"] or summary["skipped"]
should_post = has_news or notify == "always"


def read_webhook_url():
    """The Teams webhook URL from Files/_config/teams_webhook_url.txt, or '' if it isn't there."""
    path = f"{onelake_files}/_config/teams_webhook_url.txt"
    try:
        return notebookutils.fs.head(path, 4096).strip()
    except Exception:  # file missing: treated as "Teams not set up yet"
        return ""


webhook_url = "" if dry_run else read_webhook_url()
stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

if not should_post:
    outcome = "not sent: nothing to report (notify = problems)"
elif not webhook_url:
    # Dry run: keep the card so you can paste it into https://adaptivecards.io/designer to preview it.
    notebookutils.fs.put(f"{onelake_files}/_alerts/{stamp}.json", json.dumps(message, indent=2), True)
    outcome = f"dry run: card saved to Files/_alerts/{stamp}.json (no webhook URL configured)"
else:
    resp = requests.post(webhook_url, json=message, timeout=30)
    # Workflows webhooks answer 202 Accepted; the older connectors answered 200.
    if resp.status_code not in (200, 202):
        raise RuntimeError(f"Teams rejected the message: {resp.status_code} {resp.text[:300]}")
    outcome = f"posted to Teams ({resp.status_code})"

print(outcome)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---------------------------------------------------------------------------------------------------
# 6. Exit value for the pipeline run (visible in the activity's output in Fabric, and printed by
#    `.\tasks.ps1 pipeline -Env dev -Run`).
# ---------------------------------------------------------------------------------------------------
result = {
    "ok": True,
    "headline": headline,
    "outcome": outcome,
    "invocation_id": invocation_id,
    "errors": len(summary["errors"]),
    "warnings": len(summary["warnings"]),
    "skipped": len(summary["skipped"]),
    "stale": stale,
    "seconds": int(time.time() - started),
}
print(json.dumps(result, indent=2))
notebookutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
