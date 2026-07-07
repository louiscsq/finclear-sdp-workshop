# FinClear SDP demo — talk track

A ~30-min walkthrough that maps the demo to FinClear's stated concerns. Workspace:
`https://fevm-finclear-sdp-demo.cloud.databricks.com` (us-east-2). Catalog
`finclear_sdp_demo_catalog`, schema `sdp_demo`.

## 1. Framing (2 min)
"This is your Summit data shape — accounts, securities, trades, holdings, contract notes — flowing
Artie CDC → bronze → silver → gold, then out to Power BI (external) and AI/BI (internal). Everything
here is code you can run and change."

## 2. Ingestion — how Artie's changes land (5 min)
- Open `data_gen/finclear_datagen.py`: the "fake Artie" — an **append-only change feed** (insert /
  update / delete, each with `_commit_version`), ~18% churn per cycle, with deliberate duplicates
  and out-of-order events.
- `pipeline/transformations/bronze.sql`: one **streaming table** per entity via **Auto Loader**,
  with **expectations** guarding the change metadata.
- Say: *"Artie has two modes — a change feed (shown here) or merge-in-place. If yours merges in
  place, enable CDF and read `table_changes()`; the downstream is identical. Which mode is your
  Artie in?"* → real question for them.

## 3. Silver — the CDC error, solved (5 min)
- `silver_apply_changes.sql`: **`APPLY CHANGES`** collapses the feed to current-state, handling
  **dedup, ordering, and deletes** automatically (`SEQUENCE BY`, `APPLY AS DELETE`). SCD1 for
  current state; **SCD2** (`silver_accounts_history`) for full history = Artie history-mode
  equivalent.
- Tie back: *"This is the pattern that resolves the streaming-table error you're seeing — a plain
  streaming read breaks on updates; `APPLY CHANGES` off the change feed is built for it."*

## 4. The cost question — MV vs APPLY CHANGES (7 min) ← centerpiece
- Show `docs/measurement_results.md` (measured live):
  - `APPLY CHANGES` writes only the churn — **~1,880 rows/cycle**, and **0 when nothing changed**.
  - The ROW_NUMBER **MV recomputes the whole table every refresh** — ~9,900 rows every time,
    even on no-op runs.
- Say: *"Both produce identical current-state silver. At your millions-of-rows scale and 18%
  churn, incremental is materially cheaper and the gap compounds with history. Use `APPLY CHANGES`
  for CDC silver; use MVs for the gold aggregations where they incrementally maintain."*
- Re-run live with `notebooks/20_measure_mv_vs_apply.py`.

## 5. Data quality + gold + dashboard (5 min)
- Expectations: warn vs drop. The **7,624 "large trades flagged for review"** shows the monitoring
  signal in the event log and on the dashboard's DQ panel.
- `gold.sql`: report-ready MVs (portfolio valuation, daily trade activity, contract-note summary).
- Open the **AI/BI dashboard** — business metrics + DQ panel + serverless-cost tile (no per-user
  license; compute-only — the internal-reporting cost story).

## 6. DevOps — pipeline to bundle (5 min)
- `docs/dab_conversion_walkthrough.md`: `databricks bundle generate pipeline` converts a
  UI-built pipeline into a bundle; `dev`/`prod` targets = PR-based promotion. Maps to their
  dev/prod isolation + code-promotion goals.

## Threads to leave open (their homework)
- Which Artie mode (change feed vs merge-in-place + CDF)?
- Confirm the actual SDP error string against the event log.
- TradeCentre IAM → Unity Catalog identity mapping for per-user PII (DirectQuery + SSO).
- Serverless budget policy so cost tags flow into `system.billing`.
