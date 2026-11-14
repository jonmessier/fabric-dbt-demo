# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_ingest_bronze
#
# Loads new files from the landing zone (`Files/landing/<system>/<yyyy-mm-dd>/...`) into append-only
# Delta tables in `lh_bronze`, exactly as described in `docs/BRONZE.md`:
#
# * every value is loaded as a string; dbt staging does the typing
# * column names are the file headers or JSON keys in lower snake_case
# * every row gets `_source_file`, `_landed_date` and `_ingested_at`
# * each file is loaded once: `_ingest_log` records what has been loaded
#
# Nothing is cleaned or deduplicated here. A file that arrives twice is loaded twice, and staging
# keeps the newest copy of each row. Run it with `reload_all = True` to drop every bronze table and
# load the whole landing zone again.
#
# The default lakehouse must be `lh_bronze`; the provisioning script attaches it.

# PARAMETERS CELL ********************

landing_root = "Files/landing"
reload_all = False
table_format = "delta"
run_id = ""  # set by scripts/load_bronze.py: the summary is also written to Files/_runs/<run_id>.json

# CELL ********************

import json
import re
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

LOG_TABLE = "_ingest_log"

# bronze table -> (file pattern under the landing root, reader)
FEEDS = {
    "erp_customer": ("erp/*/customer.csv", "csv"),
    "erp_item": ("erp/*/item.csv", "csv"),
    "erp_price_list": ("erp/*/price_list.csv", "csv"),
    "erp_sales_header": ("erp/*/sales_header.csv", "csv"),
    "erp_sales_line": ("erp/*/sales_line.csv", "csv"),
    "erp_shipment": ("erp/*/shipment.csv", "csv"),
    "erp_invoice_line": ("erp/*/invoice_line.csv", "csv"),
    "prod_batch": ("prod/*/batches.json", "json"),
    "prod_ingredient_usage": ("prod/*/ingredient_usage.json", "json"),
    "prod_packaging_run": ("prod/*/packaging_runs.json", "json"),
    "prod_batch_cost": ("prod/*/batch_costs.json", "json"),
    "pos_transaction": ("pos/*/transactions_*.json", "pos_transaction"),
    "pos_line_item": ("pos/*/transactions_*.json", "pos_line_item"),
    "dist_depletion_northland": ("dist/*/northland_depletions_*.csv", "csv"),
    "dist_depletion_badger": ("dist/*/badger_ridge_sales_*.csv", "csv"),
    "dist_depletion_hawkeye": ("dist/*/HPW_DEPL_*.txt", "pipe"),
}


def snake(name: str) -> str:
    """Header or JSON key -> lower snake_case (docs/BRONZE.md rule 2)."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def list_files(pattern: str) -> list[str]:
    """Non-empty files matching a glob, through Hadoop so the same code works in Fabric and locally."""
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(pattern)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    statuses = fs.globStatus(path) or []
    return sorted(s.getPath().toString() for s in statuses if s.isFile() and s.getLen() > 0)


def table_exists(name: str) -> bool:
    return spark.catalog.tableExists(name)


def already_loaded(table: str) -> set[str]:
    if not table_exists(LOG_TABLE):
        return set()
    rows = spark.table(LOG_TABLE).where(F.col("bronze_table") == table).select("source_file").collect()
    return {r.source_file for r in rows}


def read_files(files: list[str], reader: str) -> DataFrame:
    if reader in ("csv", "pipe"):
        df = (spark.read
              .option("header", True)
              .option("inferSchema", False)
              .option("sep", "|" if reader == "pipe" else ",")
              .csv(files))
    else:
        # primitivesAsString keeps JSON values as strings, like CSV, so a column never changes type
        # between files (an all-null day, 60 vs 57.5, ...)
        df = spark.read.option("primitivesAsString", True).json(files)
    df = df.withColumn("_source_file", F.col("_metadata.file_path"))

    if reader == "pos_transaction":
        df = df.drop("line_items")
    elif reader == "pos_line_item":
        df = (df.select("transaction_id", "_source_file", F.explode("line_items").alias("li"))
                .select("transaction_id", "li.*", "_source_file"))

    for col in df.columns:
        if col != "_source_file" and snake(col) != col:
            df = df.withColumnRenamed(col, snake(col))
    return df


def add_metadata(df: DataFrame, ingested_at: datetime) -> DataFrame:
    return (df
            .withColumn("_landed_date",
                        F.to_date(F.regexp_extract("_source_file", r"/(\d{4}-\d{2}-\d{2})/", 1)))
            .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp")))


# CELL ********************

batch_id = str(uuid.uuid4())
ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)

if reload_all:
    for table in list(FEEDS) + [LOG_TABLE]:
        spark.sql(f"drop table if exists {table}")

summary = {}
log_rows = []
for table, (pattern, reader) in FEEDS.items():
    files = list_files(f"{landing_root.rstrip('/')}/{pattern}")
    new_files = [f for f in files if f not in already_loaded(table)]
    if not new_files:
        summary[table] = {"files": 0, "rows": 0}
        continue

    df = add_metadata(read_files(new_files, reader), ingested_at)
    (df.write
       .format(table_format)
       .mode("append")
       .option("mergeSchema", "true")
       .saveAsTable(table))

    counts = {r["_source_file"]: r["count"] for r in df.groupBy("_source_file").count().collect()}
    for f in new_files:
        log_rows.append((table, f, int(counts.get(f, 0)), ingested_at, batch_id))
    summary[table] = {"files": len(new_files), "rows": int(sum(counts.values()))}

if log_rows:
    (spark.createDataFrame(
        log_rows, "bronze_table string, source_file string, rows long, ingested_at timestamp, batch_id string")
        .write.format(table_format).mode("append").saveAsTable(LOG_TABLE))

result = {
    "ok": True,
    "batch_id": batch_id,
    "files": sum(v["files"] for v in summary.values()),
    "rows": sum(v["rows"] for v in summary.values()),
    "tables": {k: v for k, v in summary.items() if v["files"]},
}
print(json.dumps(result))

# CELL ********************

# Hand the summary back to the caller. A pipeline reads the exit value; the job API used by
# scripts/load_bronze.py does not return it, so the summary is also written to Files/_runs.
# Skipped outside Fabric.
try:
    import notebookutils  # noqa: F401  (available in Fabric notebooks)
except ImportError:
    notebookutils = None
if notebookutils is not None:
    if run_id:
        notebookutils.fs.put(f"Files/_runs/{run_id}.json", json.dumps(result), True)
    notebookutils.notebook.exit(json.dumps(result))
