# Bronze contract

What the staging models expect to find in `lh_bronze`. The ingest notebook (`nb_ingest_bronze`,
Phase 4) must produce exactly this; the dbt sources in `dbt/models/staging/*/_*__sources.yml` read it.

## Rules

1. **Append-only.** Every file that lands is appended. Nothing is updated, deduplicated or cleaned.
   A re-sent file therefore creates duplicate rows, and staging removes them.
2. **Column names** are the file's headers or JSON keys in lower snake_case: lower-case, every run of
   characters other than `a-z0-9` becomes `_`, leading and trailing `_` removed.
   `Acct #` becomes `acct`, `Supplier Item` becomes `supplier_item`, `RPT_YYYYMM` becomes `rpt_yyyymm`.
3. **Types.** CSV and text files load every column as string. JSON keeps the types Spark infers.
   Staging casts everything explicitly either way.
4. **Three metadata columns** on every table:

   | Column | Type | Meaning |
   |---|---|---|
   | `_source_file` | string | Path of the file the row came from |
   | `_landed_date` | date | The `<yyyy-mm-dd>` folder the file arrived in |
   | `_ingested_at` | timestamp | When the notebook loaded the row (drives source freshness and "latest wins") |

## Tables

| Bronze table | Files (`Files/landing/...`) | Key staging dedupes on |
|---|---|---|
| `erp_customer` | `erp/*/customer.csv` | `customer_no`, latest `updated_at` |
| `erp_item` | `erp/*/item.csv` | `item_no`, latest `updated_at` |
| `erp_price_list` | `erp/*/price_list.csv` | `item_no`, `price_tier`, `starting_date`, latest `updated_at` |
| `erp_sales_header` | `erp/*/sales_header.csv` | `order_no`, latest `updated_at` |
| `erp_sales_line` | `erp/*/sales_line.csv` | `order_no`, `line_no` |
| `erp_shipment` | `erp/*/shipment.csv` | `shipment_no` |
| `erp_invoice_line` | `erp/*/invoice_line.csv` | `invoice_no`, `invoice_line_no` |
| `prod_batch` | `prod/*/batches.json` | `batch_id`, latest `updated_at` |
| `prod_ingredient_usage` | `prod/*/ingredient_usage.json` | `batch_id`, `ingredient` |
| `prod_packaging_run` | `prod/*/packaging_runs.json` | `run_id` |
| `prod_batch_cost` | `prod/*/batch_costs.json` | `batch_id`, `cost_type` |
| `pos_transaction` | `pos/*/transactions_*.json`, every key except `line_items` | `transaction_id` |
| `pos_line_item` | same files, one row per element of `line_items`, plus `transaction_id` | `transaction_id`, `line_id` |
| `dist_depletion_northland` | `dist/*/northland_depletions_*.csv` | `period`, `acct`, `supplier_item` |
| `dist_depletion_badger` | `dist/*/badger_ridge_sales_*.csv` | `month_ending`, `retailer_id`, `product_code` |
| `dist_depletion_hawkeye` | `dist/*/HPW_DEPL_*.txt` (pipe-delimited) | `rpt_yyyymm`, `outlet_cd`, `brand_pkg` |

JSON files are JSON Lines (one object per line). Empty daily files (header only, or zero bytes) are normal.
