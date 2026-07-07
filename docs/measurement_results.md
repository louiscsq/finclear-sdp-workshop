# Measured: MV dedup vs APPLY CHANGES

Captured live in the FEVM **workshop** workspace (us-east-2), schema `sdp_workshop`, **CDF /
merge-in-place mode**, light scale (10k accounts / 1k securities / 50k trades / 20k holdings /
50k contract notes, ~18% churn per cycle). Both approaches produce **identical** current-state
silver (verified: `silver_accounts` = `silver_accounts_mv`).

## Accounts table — rows written per pipeline update

| Pipeline update | APPLY CHANGES (`silver_accounts`) | MV dedup (`silver_accounts_mv`) |
|---|---|---|
| Full refresh (initial snapshot) | **10,000** (insert) | 9,912 (recompute) |
| CDC cycle (~18% churn) | **1,874** (upsert) | 9,848 (recompute) |
| CDC cycle (~18% churn) | **1,886** (upsert) | 9,786 (recompute) |

Source: `APPLY CHANGES` from Delta `DESCRIBE HISTORY` (MERGE `numTargetRows*`); MV from the
pipeline event log (`flow_progress.metrics.num_output_rows`).

## What it shows

- **APPLY CHANGES writes only what changed** — ~1,880 rows per cycle (the churn). Cost scales
  with the *change volume*.
- **The MV rewrites the whole table every refresh** — ~9,800 rows regardless of how little
  changed. Cost scales with *total table size*. (ROW_NUMBER dedup can't be incrementally
  maintained, so it recomputes; a re-run with **no** new data still rewrites the full table,
  where APPLY CHANGES writes 0.)
- On this small 10k table that's already ~5× more rows written per cycle; the gap widens with
  table size and accumulated history.

## Why it matters for FinClear

At FinClear's real scale (millions of rows) and stated ~18% update rate, the gap compounds: the
MV's recompute grows with table size *and* history, while APPLY CHANGES stays proportional to the
changes in each cycle. **Use `APPLY CHANGES` for the silver dimensions from the CDC feed; reserve
materialized views for the gold aggregations**, where they incrementally maintain and shine.

> Reproduce with `notebooks/20_measure_mv_vs_apply.py` (parametrized by pipeline id).
