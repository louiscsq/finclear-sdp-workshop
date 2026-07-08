# FinClear — SDP Medallion Workshop

A **hands-on workshop asset**, built with FinClear during the STS Launch Accelerator, on a
simulation of your **Summit** capital-markets data. It mirrors your target architecture —
**Artie CDC → bronze → silver → gold**, served to Power BI (external) and AI/BI (internal) — as
runnable, version-controlled code.

> **This is yours to keep and build on.** It's not a throwaway demo — it's a working reference
> you can clone, run, extend with your own entities and sources, and adapt into the real pipeline.
> The "Make it yours" section below shows where to take it.

> **▶️ New here? Open [`notebooks/00_start_here.py`](notebooks/00_start_here.py)** — the guided,
> runnable front door: the business use case, what each table means, and a step-by-step run-through
> (generate data → run the pipeline → explore each layer → read the dashboard).

It's a "teach to fish" starting point that demonstrates the SDP capabilities most relevant to
FinClear:

- **`APPLY CHANGES` (AUTO CDC)** — SCD Type 1 (current state) + SCD Type 2 (history) from a CDC feed
- **Incremental refresh** — streaming tables vs materialized views, with a **measured** cost comparison
- **Expectations** — data-quality constraints (warn / drop)
- **Serverless usage tagging**, **Unity Catalog** governance + lineage, and the SDP **event log**

---

## How the ingest maps to Artie — both lanes, side by side

Ingestion is set **per entity** (bundle variable `files_entities`), so both of Artie's delivery
patterns run in the **same pipeline** — mirroring FinClear's real two-lane architecture (Summit DB
via Artie CDC, plus file/API feeds). Both lanes normalize into the same `bronze_<entity>_changes`
change stream, so everything downstream is identical:

| Lane | Entities (default) | How it arrives | Bronze reads |
|---|---|---|---|
| **Merge-in-place (CDF)** | accounts, trades, holdings, contract_notes | Artie MERGEs changes into current-state `src_<entity>` Delta tables (CDF on) | **Change Data Feed** (`readChangeFeed`) |
| **Append-only files** | securities (instrument reference / master) | delivered as Parquet change files in a Volume | **Auto Loader** |

The split is realistic: the transactional Summit tables come via Artie **merge-in-place** (matching
*"raw source copy, one object per source table"* + *"CDC merge into the Delta Lake layer"*), while
reference data like **securities** arrives as an append-only file feed. Change the split any time
with `--var="files_entities=securities,contract_notes"` (or `files_entities=` for all-CDF).

**To point this at real Artie:** for CDF-lane tables, enable Change Data Feed on Artie's target
Delta tables and set `src_schema`; for file-lane sources, point `source_volume` at the landing
path. Nothing downstream changes.

---

## Make it yours (where to take this)

- **Point at real Artie** — enable CDF on Artie's tables, set `src_schema`, deploy. Retire the generator.
- **Add your own entities** — drop a builder into `data_gen/`, add a bronze/silver definition; the
  pattern repeats.
- **Add gold marts** — extend `gold.sql` with the reports your analysts and clients actually need.
- **Retarget** — `--var="catalog=...,schema=..."` moves the whole thing to any catalog/schema.
- **Governance** — layer UC row filters / column masks / ABAC on the silver dimensions (e.g. mask
  `accounts.email`); add Delta Sharing with `CURRENT_RECIPIENT()` for per-client external access.
- **CI/CD** — wire `databricks bundle deploy --target prod` into a PR pipeline (see the DevOps walkthrough).

---

## Repo layout

```
finclear-sdp-workshop/
├── databricks.yml                     # Asset Bundle: dev/prod targets, variables
├── resources/                         # pipeline, generator job, dashboard
├── pipeline/transformations/          # the medallion
│   ├── bronze.py                      # per-entity ingest: CDF (merge-in-place) or files + expectations
│   ├── silver_apply_changes.sql       # APPLY CHANGES SCD1 + SCD2  ← recommended arm
│   ├── silver_materialized_view.sql   # MV ROW_NUMBER dedup        ← comparison arm
│   └── gold.sql                       # report-ready materialized views
├── data_gen/finclear_datagen.py       # "fake Artie": init + repeatable CDC cycles (cdf|files)
├── notebooks/20_measure_mv_vs_apply.py# event-log cost/perf comparison
├── dashboards/                        # AI/BI dashboard (business + DQ + cost)
└── docs/                              # workshop guide, measured results, DevOps walkthrough
```

---

## Quick start

```bash
# 0. Authenticate (once)
databricks auth login --host https://fevm-finclear-sdp-demo.cloud.databricks.com --profile finclear-sdp

# 1. Deploy the bundle (dev target)
databricks bundle deploy -p finclear-sdp

# 2. Generate initial data — "fake Artie": CDF-lane tables merge into src_<entity>,
#    file-lane tables (default: securities) land as Parquet in the Volume
databricks bundle run finclear_datagen -p finclear-sdp

# 3. Run the pipeline (bronze reads CDF + files → silver → gold)
databricks bundle run finclear_sdp -p finclear-sdp

# 4. Emit more CDC cycles, then re-run the pipeline to see incremental processing
#    (re-run data_gen/finclear_datagen.py with mode=cycle, then step 3)

# 5. Measure MV vs APPLY CHANGES  → notebooks/20_measure_mv_vs_apply.py
```

Retarget or change which entities use the file lane, without touching SQL:

```bash
databricks bundle deploy -p finclear-sdp --var="catalog=my_cat,schema=my_schema,files_entities=securities,contract_notes"
```

---

## The medallion

- **Bronze** — one streaming table per Summit entity. Per-entity ingest: CDF (merge-in-place lane)
  or Auto Loader (file lane), normalized to one change-stream shape. Expectations guard change
  metadata; deletes tolerated.
- **Silver (recommended)** — `APPLY CHANGES` → current-state dimensions (SCD1) + history (SCD2),
  incrementally, handling dedup / ordering / deletes automatically.
- **Silver (comparison)** — `silver_*_mv` reproduce the same state via ROW_NUMBER dedup (recompute),
  purely to measure cost against the APPLY CHANGES arm.
- **Gold** — materialized views: portfolio valuation, daily trade activity, contract-note summary.

See `docs/workshop_guide.md` for the guided walkthrough, `docs/measurement_results.md` for the
measured MV-vs-APPLY CHANGES numbers, `docs/dab_conversion_walkthrough.md` for the DevOps story,
and `docs/failure_and_recovery.md` for failure isolation, selective refresh, and one-pipeline-vs-many.

---

## Notes
- **Serverless** throughout; requires Unity Catalog.
- Cost tags (`project`, `cost_center`, `layer`) on the pipeline; attach a serverless budget policy for
  tags to flow into `system.billing.usage`.
- Workshop workspace is us-east-2 (FEVM has no serverless in ap-southeast-2). In production this runs
  in FinClear's region (ap-southeast-2); the region is incidental.
