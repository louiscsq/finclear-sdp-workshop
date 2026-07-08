# Failure isolation & recovery

How this pipeline behaves when something breaks, and why the whole medallion is **one SDP
pipeline** rather than one pipeline per source table.

## TL;DR
- A failure in one table does **not** roll back the others, and does **not** force a full
  reprocess — streaming tables resume from their checkpoints.
- You can re-run **just the failed table** with selective refresh; you don't need separate
  pipelines for targeted retry.
- Artie ingesting per source table is **upstream** of the pipeline, so a per-table Artie failure
  never cascades into the medallion.

---

## What happens when one table fails (inside one pipeline)

SDP compiles every transformation into one dependency graph and runs the independent flows in
parallel. If, say, `bronze_trades_changes` fails:

1. **Independent flows that already succeeded commit and are kept.** `bronze_accounts_changes`,
   `bronze_securities_changes`, etc. are **not** rolled back.
2. **Only the failed table's downstream is skipped** — here `silver_trades` and any gold that
   joins trades. Accounts/securities/holdings silver and their gold still complete.
3. **SDP retries the failing flow** several times within the update before marking it failed.
4. **Re-triggering the update is incremental** — streaming tables resume from their checkpoints,
   so the healthy tables do **not** re-ingest. Only the failed table processes its outstanding data.

> So "one table failed → rerun everything" is a misconception. Healthy tables don't redo work.

---

## Recover just the failed table — selective refresh

You do **not** need to rerun the whole pipeline. Refresh only the affected tables:

```bash
# Refresh only trades bronze + silver (leaves the rest untouched)
databricks pipelines start-update <PIPELINE_ID> \
  --json '{"refresh_selection":["bronze_trades_changes","silver_trades"]}'

# Force a full rebuild of specific tables (e.g. after a schema fix)
databricks pipelines start-update <PIPELINE_ID> \
  --json '{"full_refresh_selection":["bronze_trades_changes"]}'
```

Or in the UI: **Pipeline → Refresh selection**. This gives per-table targeted retry within a
single pipeline — the isolation you'd otherwise reach for separate pipelines to get.

---

## Artie failure is a separate layer

Artie merges each source into its own `src_<entity>` Delta table (CDF enabled) **before** the
pipeline runs. Consequences:

- If **Artie** fails for one source, that is **not** an SDP failure. Bronze simply sees no new
  Change Data Feed for that table and processes everything else normally.
- Each `src_` table is written independently, so a per-source Artie problem is already isolated at
  the ingestion layer — it never reaches the medallion.

---

## One pipeline vs many — the decision

**Default: one pipeline** for the whole medallion. It gives a single *consistent* update, unified
end-to-end lineage, automatic dependency ordering, and — as above — per-flow failure isolation
plus selective refresh.

Introduce a pipeline boundary (and orchestrate the pipelines with a **Workflow**) only for real
boundaries:

| Split when… | Cost you accept |
|---|---|
| Sources need different **SLAs / refresh cadence** | Gold that **joins across entities** reads across pipelines → lose the single consistent update (eventual consistency) |
| Different **team ownership** / blast-radius isolation | **Split lineage** — no single end-to-end graph |
| A poison-pill source must not affect others | More orchestration; **no automatic cross-pipeline dependency** |

**FinClear today:** the gold marts join accounts × securities × holdings × trades, so **one
pipeline** is correct — splitting would trade away consistency, lineage, and auto-ordering to
solve a problem SDP already handles. The valid future splits are *ingestion vs serving* (if
cadences diverge) or *independent domains that don't join*.

## Where Workflows fit
Not for sequencing tables inside the medallion (SDP does that). Use a Databricks **Workflow** to
orchestrate *around* the pipeline: `ingest (Artie / scheduled extracts) → pipeline_task (this
medallion) → serve (Delta Sharing / Power BI refresh / exports)`, on the ~30-minute cadence.
