# FinClear — SDP Medallion Demo

An end-to-end **Spark Declarative Pipelines (SDP)** demo built on *simulated* FinClear
(**Summit**) capital-markets data, mirroring FinClear's target architecture: **Artie CDC →
bronze → silver → gold**, served to Power BI (external) and AI/BI (internal).

It's a "teach to fish" artifact — runnable, parametrized, and shareable — that showcases the
SDP features most relevant to FinClear:

- **Expectations** (data-quality constraints, warn / drop / fail)
- **Incremental refresh** — streaming tables vs materialized views
- **`APPLY CHANGES` (AUTO CDC)** — SCD Type 1 (current state) and SCD Type 2 (history)
- **MV-dedup vs APPLY CHANGES** — a measured cost/performance comparison
- **Serverless usage tagging** for cost observability
- **Unity Catalog** governance + lineage, and the SDP **event log**

---

## How the CDC modelling maps to Artie

Artie can deliver changes two ways; **both feed `APPLY CHANGES` identically**:

| Artie mode | What lands | Bronze reads via | Need CDF? |
|---|---|---|---|
| **Change-feed / history mode** *(modelled here)* | append-only change events | Auto Loader over files | No |
| **Merge-in-place** | a Delta table updated in place | Change Data Feed → `table_changes()` | **Yes** |

This demo lands an **append-only change feed** (the generator plays "fake Artie"), which keeps
the pipeline pure-SQL and makes the MV-vs-APPLY CHANGES comparison fair. If FinClear's Artie
**merges in place** instead, enable CDF on its target table and read `table_changes()` — the
`_change_type` / `_commit_version` feed is identical and everything downstream is unchanged.
*(Confirm which mode FinClear's Artie is configured for.)*

---

## Repo layout

```
finclear-sdp-demo/
├── databricks.yml                     # Asset Bundle: dev/prod targets, catalog/schema vars
├── resources/
│   ├── finclear_sdp.pipeline.yml      # the SDP pipeline (serverless, tagged)
│   └── finclear_datagen.job.yml       # job that runs the generator
├── pipeline/transformations/          # the medallion (SQL)
│   ├── bronze.sql                     # streaming tables + expectations (Auto Loader)
│   ├── silver_apply_changes.sql       # APPLY CHANGES SCD1 + SCD2  ← the recommended arm
│   ├── silver_materialized_view.sql   # MV ROW_NUMBER dedup        ← comparison arm
│   └── gold.sql                       # report-ready materialized views
├── data_gen/
│   └── finclear_datagen.py            # "fake Artie": initial load + repeatable CDC cycles
├── notebooks/
│   └── 20_measure_mv_vs_apply.py      # event-log cost/perf comparison
├── dashboards/                        # AI/BI dashboard (business + DQ + cost tiles)
└── docs/                              # talk-track + DAB-conversion walkthrough
```

---

## Quick start

```bash
# 0. Authenticate (once)
databricks auth login --host https://fevm-finclear-sdp-demo.cloud.databricks.com --profile finclear-sdp

# 1. Deploy the bundle (dev target)
databricks bundle deploy -p finclear-sdp

# 2. Generate initial data (runs the "fake Artie" generator)
databricks bundle run finclear_datagen -p finclear-sdp

# 3. Run the pipeline (bronze → silver → gold)
databricks bundle run finclear_sdp -p finclear-sdp

# 4. Emit a CDC cycle (~18% updates + deletes + dupes), then re-run the pipeline
#    (re-run the CDC-cycle cell in data_gen/finclear_datagen.py, then step 3 again)

# 5. Measure MV vs APPLY CHANGES  → notebooks/20_measure_mv_vs_apply.py
```

Retarget catalog/schema without editing SQL:

```bash
databricks bundle deploy -p finclear-sdp --var="catalog=my_cat,schema=my_schema"
```

---

## The medallion

- **Bronze** — one streaming table per Summit entity (accounts, securities, trades, holdings,
  contract notes), ingesting the Artie change feed with Auto Loader. Expectations guard change
  metadata; deletes are tolerated.
- **Silver (A — recommended)** — `APPLY CHANGES` collapses the feed to current-state dimensions
  incrementally, handling dedup, ordering, and deletes automatically. SCD1 for current state;
  SCD2 (`silver_accounts_history`) for full history.
- **Silver (B — comparison)** — `silver_*_mv` reproduce the same current state via ROW_NUMBER
  dedup (recompute). Exist only to measure cost against arm A.
- **Gold** — materialized views: `gold_portfolio_valuation`, `gold_daily_trade_activity`,
  `gold_contract_note_summary`.

---

## Notes

- **Serverless** throughout; requires Unity Catalog.
- Cost tags (`project`, `cost_center`, `layer`) are set on the pipeline; for tags that flow into
  `system.billing.usage`, attach a serverless budget policy (see `docs/`).
- Demo workspace is us-east-2 (FEVM has no serverless in ap-southeast-2); in production this runs
  in FinClear's region (ap-southeast-2). Region is incidental to the demo.
