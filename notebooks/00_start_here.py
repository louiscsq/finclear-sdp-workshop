# Databricks notebook source
# MAGIC %md
# MAGIC # 🏁 Start here — FinClear SDP Medallion Workshop
# MAGIC
# MAGIC Welcome. This notebook is the guided front door to the workshop. It explains the **business
# MAGIC use case**, what **each table means**, and walks you **step by step** through generating data,
# MAGIC running the medallion pipeline, exploring each layer, and reading the dashboard.
# MAGIC
# MAGIC Everything runs on a simulation of your **Summit** data, wired the way your architecture
# MAGIC describes: **Artie CDC → bronze → silver → gold**.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. The business use case
# MAGIC
# MAGIC FinClear provides **trade execution, clearing and settlement** infrastructure for broker-dealers
# MAGIC and institutional clients. **Summit** is the operational data store (SQL Server) behind that
# MAGIC platform — accounts, the instruments they trade, the trades themselves, the resulting positions,
# MAGIC and the legal confirmations.
# MAGIC
# MAGIC The **Enterprise Data Platform** is the *analytical* side: a governed lakehouse that mirrors
# MAGIC Summit (via Artie CDC), refines it through a medallion, and serves:
# MAGIC
# MAGIC - **External clients** — their own trade/position data via Power BI Embedded (TradeCentre) & Delta Sharing
# MAGIC - **Internal teams** — management dashboards, ops/trading monitoring, compliance reporting (AI/BI)
# MAGIC
# MAGIC This workshop builds that analytical pipeline end-to-end, and highlights the decisions that
# MAGIC matter for FinClear: **CDC handling, incremental vs full-refresh cost, data quality, and governance.**

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. What each table means to the business
# MAGIC
# MAGIC ### Source entities (mirrored from Summit)
# MAGIC | Table | Business meaning | Why it matters |
# MAGIC |---|---|---|
# MAGIC | **accounts** | Client accounts on the platform (Individual, Joint, Trust, Company, SMSF), with adviser, risk profile, status | Who owns trades & positions; drives client reporting and entitlement |
# MAGIC | **securities** | Instrument reference master — ASX equities, ETFs, bonds, cash, managed funds | *What* is traded; enriches trades/holdings with asset class & sector |
# MAGIC | **trades** | Executed buy/sell transactions (quantity, price, brokerage, settlement date, status) | The core activity; drives **brokerage revenue**, settlement, and activity reporting |
# MAGIC | **holdings** | Current positions per account × security (units, cost base, market value, unrealised P&L) | What each client **currently owns**; drives portfolio valuation & statements |
# MAGIC | **contract_notes** | The legal confirmation issued for each trade (gross, brokerage, GST, net) | Client-facing confirmation + **regulatory record-keeping** (ASIC / Corporations Act) |
# MAGIC
# MAGIC ### Medallion layers (in business terms)
# MAGIC | Layer | What it is | Business value |
# MAGIC |---|---|---|
# MAGIC | **Bronze** (`bronze_*_cdc`) | Every change to the source, as it happened (Artie's CDC feed) | Audit-grade capture, no loss of granularity |
# MAGIC | **Silver** (`silver_*`) | Clean, deduplicated **current state** of each entity (SCD1) + full **history** (SCD2) | "What is true now" for reporting + point-in-time history for audit |
# MAGIC | **Gold** (`gold_*`) | Report-ready aggregates answering business questions | Directly feeds client statements, ops dashboards, compliance |

# COMMAND ----------

dbutils.widgets.text("catalog", "finclear_sdp_demo_catalog", "Catalog")
dbutils.widgets.text("schema", "sdp_workshop", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
C = f"{CATALOG}.{SCHEMA}"
spark.sql(f"USE {C}")
print("Using", C)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Generate test data  ("fake Artie") — two lanes at once
# MAGIC
# MAGIC The generator plays Artie, with ingestion set **per entity** so both delivery patterns run
# MAGIC side by side (this mirrors your real two-lane architecture):
# MAGIC - **Merge-in-place (CDF)** — accounts, trades, holdings, contract_notes: MERGEd into
# MAGIC   current-state `src_<entity>` Delta tables (Change Data Feed on); bronze reads their CDF.
# MAGIC - **Append-only files** — `securities` (instrument reference): landed as Parquet in a Volume;
# MAGIC   bronze reads it with Auto Loader.
# MAGIC
# MAGIC **Option A — one click here** (initial load + one CDC cycle, `securities` on the file lane):

# COMMAND ----------

# Uncomment to (re)generate data from this notebook:
# dbutils.notebook.run("../data_gen/finclear_datagen", 600,
#     {"mode": "both", "files_entities": "securities", "catalog": CATALOG, "schema": SCHEMA, "src_schema": SCHEMA})

# COMMAND ----------
# MAGIC %md
# MAGIC **Option B — from your terminal** (the repeatable, production-like way):
# MAGIC ```bash
# MAGIC databricks bundle run finclear_datagen -p finclear-sdp        # initial load + a CDC cycle
# MAGIC ```
# MAGIC Each run emits ~18% updates + a few deletes + duplicate/out-of-order events — the churn FinClear
# MAGIC quoted. Let's confirm Artie's current-state mirror landed:

# COMMAND ----------

display(spark.sql(f"""
  SELECT 'accounts' AS entity, COUNT(*) AS current_rows FROM src_accounts
  UNION ALL SELECT 'securities', COUNT(*) FROM src_securities
  UNION ALL SELECT 'trades', COUNT(*) FROM src_trades
  UNION ALL SELECT 'holdings', COUNT(*) FROM src_holdings
  UNION ALL SELECT 'contract_notes', COUNT(*) FROM src_contract_notes
  ORDER BY entity
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Run the medallion pipeline
# MAGIC
# MAGIC From your terminal:
# MAGIC ```bash
# MAGIC databricks bundle run finclear_sdp -p finclear-sdp
# MAGIC ```
# MAGIC Bronze reads Artie's **Change Data Feed**, then `APPLY CHANGES` builds current-state silver and
# MAGIC materialized views build gold. Watch it in the Pipelines UI (bronze → silver → gold graph).

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Explore each layer
# MAGIC
# MAGIC ### Bronze — the raw CDC change feed (audit-grade)
# MAGIC Every insert / update / delete Artie captured. Note the change types:

# COMMAND ----------

display(spark.sql("SELECT _change_type, COUNT(*) AS events FROM bronze_accounts_cdc GROUP BY 1 ORDER BY 1"))

# COMMAND ----------
# MAGIC %md
# MAGIC #### "Same shape, two doors in" — how each table arrived
# MAGIC `_source_file` reveals the lane: the **CDF lane** points at the source Delta table
# MAGIC (`src_accounts`); the **file lane** points at actual Parquet files in the Volume. Both feed the
# MAGIC identical bronze shape.

# COMMAND ----------

display(spark.sql("""
  SELECT 'accounts (CDF lane)'    AS entity, _source_file, COUNT(*) AS rows
  FROM bronze_accounts_cdc   GROUP BY _source_file
  UNION ALL
  SELECT 'securities (file lane)' AS entity, _source_file, COUNT(*) AS rows
  FROM bronze_securities_cdc GROUP BY _source_file
  ORDER BY entity, rows DESC
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Silver — current state (SCD1) + history (SCD2)
# MAGIC `silver_accounts` is the deduplicated current view (deletes applied). `silver_accounts_history`
# MAGIC keeps every version with `__START_AT` / `__END_AT` — point-in-time audit, the native equivalent
# MAGIC of Artie "history mode".

# COMMAND ----------

display(spark.sql("SELECT * FROM silver_accounts LIMIT 10"))

# COMMAND ----------

# An account's full change history (SCD Type 2). Pick any id present in your data.
display(spark.sql("""
  SELECT account_id, status, risk_profile, __START_AT, __END_AT
  FROM silver_accounts_history
  ORDER BY account_id, __START_AT
  LIMIT 20
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Gold — report-ready marts (what the business actually asks for)
# MAGIC | Mart | Business question it answers | Consumer |
# MAGIC |---|---|---|
# MAGIC | `gold_portfolio_valuation` | "What is each account worth, and its unrealised P&L, by asset class?" | Client statements, management dashboards, advisers |
# MAGIC | `gold_daily_trade_activity` | "How much are we trading each day, by asset class — and how much brokerage?" | Trading desk / ops, revenue tracking |
# MAGIC | `gold_contract_note_summary` | "How many contract notes, of what value & status, by day?" | Compliance, reconciliation |

# COMMAND ----------

display(spark.sql("""
  SELECT asset_class,
         COUNT(DISTINCT account_id) AS accounts,
         ROUND(SUM(total_market_value)/1e6, 1)   AS market_value_m,
         ROUND(SUM(total_unrealised_pnl)/1e6, 1) AS unrealised_pnl_m
  FROM gold_portfolio_valuation GROUP BY asset_class ORDER BY market_value_m DESC
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. See incremental processing — run a CDC cycle
# MAGIC
# MAGIC Emit another batch of changes (updates/deletes), then re-run the pipeline:
# MAGIC ```bash
# MAGIC # cycle-only (no re-initialise):
# MAGIC databricks jobs run-now --json '{"job_id": <DATAGEN_JOB_ID>, "notebook_params": {"mode":"cycle","files_entities":"securities","catalog":"finclear_sdp_demo_catalog","schema":"sdp_workshop","src_schema":"sdp_workshop"}}' -p finclear-sdp
# MAGIC databricks bundle run finclear_sdp -p finclear-sdp
# MAGIC ```
# MAGIC Then open `notebooks/20_measure_mv_vs_apply.py` to see the punchline: **`APPLY CHANGES` writes
# MAGIC only the rows that changed; the materialized-view dedup recomputes the whole table every time.**
# MAGIC Same result, very different cost at FinClear's scale + 18% churn.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. The dashboard — what it tells the business
# MAGIC
# MAGIC Open **FinClear — Summit Medallion (SDP workshop)** (deployed with the bundle). Panels:
# MAGIC
# MAGIC - **KPIs** — total market value, unrealised P&L, # accounts → platform-level health at a glance.
# MAGIC - **Market value & trades by asset class** → concentration and trading mix.
# MAGIC - **Daily trade value** → activity trend over time (ops & revenue).
# MAGIC - **Data quality (expectations)** → trust signal. The "large trades flagged" count is a **review
# MAGIC   signal, not an error** — a warn expectation surfaces unusually large trades; "dropped" would be
# MAGIC   rows that failed a hard rule. This is how you *monitor* data quality, live, from the event log.
# MAGIC - **Serverless cost (this pipeline)** → cost observability by SKU (populates as billing lands) —
# MAGIC   directly addresses FinClear's Databricks-cost concern.
# MAGIC
# MAGIC AI/BI dashboards carry **no per-user license cost** (compute only) — the internal-reporting story.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Make it yours
# MAGIC
# MAGIC - **Point at real Artie** — for CDF-lane tables enable Change Data Feed and set `src_schema`; for file-lane sources set `files_entities` + `source_volume`. Retire the generator.
# MAGIC - **Add entities / gold marts** — extend `data_gen/`, `pipeline/transformations/silver_apply_changes.sql`, and `gold.sql`.
# MAGIC - **Govern** — add UC row filters / column masks / ABAC on silver (e.g. mask `accounts.email`); add Delta Sharing with `CURRENT_RECIPIENT()` for per-client access.
# MAGIC - **Promote** — `databricks bundle deploy --target prod`; wire it into a PR pipeline (`docs/dab_conversion_walkthrough.md`).
# MAGIC
# MAGIC See `README.md` and `docs/workshop_guide.md` for the full guided narrative.
