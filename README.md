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

## How the ingest maps to Artie — and how to point it at the real thing

Bronze supports two ingest modes (bundle variable `ingest_mode`), both normalized into the same
`bronze_<entity>_cdc` change stream so everything downstream is identical:

| `ingest_mode` | Models | Bronze reads | Matches |
|---|---|---|---|
| **`cdf`** *(default)* | Artie **merging in place** into current-state Delta tables | **Change Data Feed** (`readChangeFeed`) | FinClear's likely Artie setup |
| `files` | Artie emitting an **append-only change feed** | Auto Loader over files | Artie change-feed / history mode |

The default is **merge-in-place + CDF** because that's what your architecture docs point to
(*"raw source copy, one object per source table"* + *"CDC merge operations into the Delta Lake
layer"*). The "fake Artie" generator MERGEs changes into `src_<entity>` Delta tables with CDF
enabled, and bronze reads their change feed — exactly the pattern you'd run in production.

**To point this at real Artie:** enable Change Data Feed on Artie's target Delta tables, set
`src_schema` to where they live, and keep `ingest_mode=cdf`. Nothing else changes.

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
│   ├── bronze.py                      # CDF (merge-in-place, default) or files ingest + expectations
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

# 1. Deploy the bundle (dev target, merge-in-place default)
databricks bundle deploy -p finclear-sdp

# 2. Generate initial data — "fake Artie" merges into src_<entity> Delta tables (CDF on)
databricks bundle run finclear_datagen -p finclear-sdp

# 3. Run the pipeline (bronze reads the CDF → silver → gold)
databricks bundle run finclear_sdp -p finclear-sdp

# 4. Emit more CDC cycles, then re-run the pipeline to see incremental processing
#    (re-run data_gen/finclear_datagen.py with mode=cycle, then step 3)

# 5. Measure MV vs APPLY CHANGES  → notebooks/20_measure_mv_vs_apply.py
```

Retarget or switch ingest mode without touching SQL:

```bash
databricks bundle deploy -p finclear-sdp --var="catalog=my_cat,schema=my_schema,ingest_mode=files"
```

---

## The medallion

- **Bronze** — one streaming table per Summit entity, reading Artie's CDF (default) or files.
  Expectations guard change metadata; deletes tolerated.
- **Silver (recommended)** — `APPLY CHANGES` → current-state dimensions (SCD1) + history (SCD2),
  incrementally, handling dedup / ordering / deletes automatically.
- **Silver (comparison)** — `silver_*_mv` reproduce the same state via ROW_NUMBER dedup (recompute),
  purely to measure cost against the APPLY CHANGES arm.
- **Gold** — materialized views: portfolio valuation, daily trade activity, contract-note summary.

See `docs/workshop_guide.md` for the guided walkthrough, `docs/measurement_results.md` for the
measured MV-vs-APPLY CHANGES numbers, and `docs/dab_conversion_walkthrough.md` for the DevOps story.

---

## Notes
- **Serverless** throughout; requires Unity Catalog.
- Cost tags (`project`, `cost_center`, `layer`) on the pipeline; attach a serverless budget policy for
  tags to flow into `system.billing.usage`.
- Workshop workspace is us-east-2 (FEVM has no serverless in ap-southeast-2). In production this runs
  in FinClear's region (ap-southeast-2); the region is incidental.
