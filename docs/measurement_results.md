# Measured: MV dedup vs APPLY CHANGES

Captured live in the FEVM workshop workspace (us-east-2), light scale:
10k accounts / 1k securities / 50k trades / 20k holdings / 50k contract notes, ~18% churn/cycle.
Both approaches produce **identical** current-state silver (verified: `silver_accounts` =
`silver_accounts_mv` = 9,918 rows).

## Accounts table — rows written per pipeline update

| Pipeline update | APPLY CHANGES (`silver_accounts`) | MV dedup (`silver_accounts_mv`) |
|---|---|---|
| Initial load (10k inserts) | **10,000** (insert) | 9,918 (recompute) |
| Re-run, no new data | **0** | 9,918 (recompute) |
| Re-run, no new data | **0** | 9,918 (recompute) |
| CDC cycle (~18% churn) | **1,880** (upsert) | 9,841 (recompute) |
| CDC cycle (~18% churn) | **1,883** (upsert) | 9,782 (recompute) |

Source: `APPLY CHANGES` from Delta `DESCRIBE HISTORY` (MERGE `numTargetRows*`); MV from the
pipeline event log (`flow_progress.metrics.num_output_rows`).

## What it shows

- **APPLY CHANGES writes only what changed** — ~1,880 rows per cycle (the churn). Cost scales
  with the *change volume*.
- **The MV rewrites the whole table every refresh** — ~9,900 rows regardless of how little
  changed. Cost scales with *total table size*.
- **The MV pays full cost even when nothing changed** — 9,918 rows rewritten on both no-op
  re-runs, where APPLY CHANGES wrote 0. (ROW_NUMBER dedup can't be incrementally maintained.)
- Across the whole medallion, each MV refresh rewrote **~129,821 rows every update**; the
  APPLY CHANGES arm wrote only the per-cycle churn.

## Why it matters for FinClear

At FinClear's real scale (millions of rows) and stated ~18% update rate, the gap compounds:
the MV's recompute grows with table size *and* accumulated history, while APPLY CHANGES stays
proportional to the changes in each cycle. **Use `APPLY CHANGES` for the silver dimensions
from the CDC feed; reserve materialized views for the gold aggregations**, where they
incrementally maintain and shine.

> Reproduce with `notebooks/20_measure_mv_vs_apply.py` (parametrized by pipeline id).
