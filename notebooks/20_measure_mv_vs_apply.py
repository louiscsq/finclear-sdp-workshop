# Databricks notebook source
# MAGIC %md
# MAGIC # Measure: MV dedup vs APPLY CHANGES — cost / performance
# MAGIC
# MAGIC Both approaches build the **same** current-state silver. This notebook measures how much *work*
# MAGIC each one does per pipeline run, so the cost difference is concrete rather than theoretical.
# MAGIC
# MAGIC ## How we measure cost (the logic)
# MAGIC The dominant cost of a transformation is reading, computing and **writing rows** — and rows
# MAGIC written per run tracks compute (DBUs) closely. So we use **rows written per run** as an
# MAGIC immediate, intuitive proxy. (Actual DBUs land in `system.billing` a few hours later — Section C.)
# MAGIC
# MAGIC The two approaches record that number in **different places**, so we read each from where it lives:
# MAGIC
# MAGIC | Approach | Table | What one run does | Where the count lives |
# MAGIC |---|---|---|---|
# MAGIC | `APPLY CHANGES` | `silver_*` (streaming table) | a Delta **MERGE** of only the changed rows | Delta history `operationMetrics` |
# MAGIC | MV dedup | `silver_*_mv` (materialized view) | a full **recompute** of the whole result | pipeline event log `num_output_rows` |
# MAGIC
# MAGIC Why two sources? An APPLY CHANGES flow does a MERGE (so its counts are in Delta history, not the
# MAGIC event log's `num_output_rows`), while an MV has no MERGE (so its write count is only in the event
# MAGIC log). We pull each from its natural home and compare.

# COMMAND ----------

dbutils.widgets.text("pipeline_id", "", "Pipeline ID — leave BLANK to auto-detect (not the run id)")
dbutils.widgets.text("pipeline_name_contains", "finclear-sdp-workshop", "Pipeline name to match (auto-detect)")
dbutils.widgets.text("catalog", "finclear_sdp_demo_catalog", "Catalog")
dbutils.widgets.text("schema", "sdp_workshop", "Schema")

# Use the pasted id if given, otherwise look the PIPELINE up by name. This avoids the common
# mistake of pasting the *run* id (Run details → Pipeline run ID) instead of the *pipeline* id
# (Pipeline details → Pipeline ID) — event_log() needs the pipeline id.
PIPELINE_ID = dbutils.widgets.get("pipeline_id").strip()
if not PIPELINE_ID:
    from databricks.sdk import WorkspaceClient
    needle = dbutils.widgets.get("pipeline_name_contains")
    matches = [p for p in WorkspaceClient().pipelines.list_pipelines() if needle in (p.name or "")]
    if not matches:
        raise ValueError(f"No pipeline found with name containing '{needle}'. Paste the Pipeline ID manually.")
    # prefer the dev pipeline if both dev and prod exist
    matches.sort(key=lambda p: ("-dev" not in (p.name or ""), p.name or ""))
    PIPELINE_ID = matches[0].pipeline_id
    print(f"Auto-detected pipeline_id = {PIPELINE_ID}   (name: {matches[0].name})")

C = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"

# Every silver_<entity> is built by APPLY CHANGES (including securities, off the file-lane feed).
ENTITIES = ["accounts", "securities", "trades", "holdings", "contract_notes"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## A. APPLY CHANGES — rows written per run (Delta MERGE metrics)
# MAGIC
# MAGIC Each `silver_*` table is a real Delta table, and every pipeline run applies a **MERGE** that
# MAGIC upserts/deletes only the rows that changed. Delta records, per commit, exactly how many rows the
# MAGIC MERGE touched in `operationMetrics`:
# MAGIC - `numTargetRowsInserted` — new rows added
# MAGIC - `numTargetRowsUpdated`  — existing rows changed
# MAGIC - `numTargetRowsDeleted`  — rows removed
# MAGIC
# MAGIC **Their sum = the work that run = the churn.** The query below reads `DESCRIBE HISTORY` for each
# MAGIC silver table, keeps only `MERGE` commits, and reports rows-written per table over time. Watch it
# MAGIC stay ≈ the per-cycle churn — and drop to **0** on a run where nothing changed.
# MAGIC
# MAGIC *(Nuance: a MERGE can also rewrite whole files, "copying" unchanged rows — `numTargetRowsCopied`.
# MAGIC We use inserted+updated+deleted as the logical churn; that's the fair like-for-like vs the MV.)*

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F

# 1) One DESCRIBE HISTORY per silver table, unioned into a single DataFrame.
#    (DESCRIBE HISTORY is per-table, so we run it for each entity and stack the results.)
hist = reduce(lambda a, b: a.unionByName(b), [
    spark.sql(f"""
      SELECT '{e}' AS entity, version, timestamp,
             CAST(operationMetrics['numTargetRowsInserted'] AS BIGINT) AS inserted,
             CAST(operationMetrics['numTargetRowsUpdated']  AS BIGINT) AS updated,
             CAST(operationMetrics['numTargetRowsDeleted']  AS BIGINT) AS deleted
      FROM (DESCRIBE HISTORY {C}.silver_{e})
      WHERE operation = 'MERGE'          -- APPLY CHANGES commits show up as MERGE
    """) for e in ENTITIES
])

# 2) rows_written = rows this MERGE actually touched = the churn for that run.
#    coalesce(...,0) because a metric is null when that operation type didn't occur.
result = (hist
    .withColumn("rows_written",
                F.coalesce("inserted", F.lit(0))
              + F.coalesce("updated",  F.lit(0))
              + F.coalesce("deleted",  F.lit(0)))
    .select("entity", "timestamp", "inserted", "updated", "deleted", "rows_written")
    .orderBy("entity", "timestamp"))
display(result)

# COMMAND ----------
# MAGIC %md
# MAGIC ## B. MV dedup — rows written per refresh (event log)
# MAGIC
# MAGIC A materialized view has no MERGE — each refresh **writes its whole result set**. The pipeline
# MAGIC event log records that as `flow_progress.metrics.num_output_rows` (one row per completed flow
# MAGIC per update). Because the dedup uses a window function (`ROW_NUMBER`), the MV **can't be
# MAGIC incrementally maintained**, so it recomputes the entire table every refresh → `num_output_rows`
# MAGIC ≈ the full table size, no matter how little changed.
# MAGIC
# MAGIC The query reads the event log for the pipeline, keeps only **completed** `*_mv` flows, and pulls
# MAGIC that written-row count per refresh.

# COMMAND ----------

display(spark.sql(f"""
  SELECT date_format(timestamp,'HH:mm:ss')                              AS refresh_time,
         replace(origin.flow_name, '{C}.', '')                         AS mv,
         CAST(details:flow_progress.metrics.num_output_rows AS BIGINT)  AS rows_written_recompute
  FROM event_log('{PIPELINE_ID}')
  WHERE event_type = 'flow_progress'
    AND details:flow_progress.status = 'COMPLETED'   -- only finished refreshes (ignore RUNNING/PLANNING)
    AND origin.flow_name LIKE '%_mv'                 -- the MV comparison arm only
    AND details:flow_progress.metrics.num_output_rows IS NOT NULL
  ORDER BY timestamp
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## C. Actual serverless cost (system.billing) — the real DBUs
# MAGIC
# MAGIC Rows-written above is the live proxy; this is the real spend. Billing lags a few hours, so it's
# MAGIC for the follow-up rather than the live run. We attribute by the **pipeline id**
# MAGIC (`usage_metadata.dlt_pipeline_id`) — robust because it doesn't depend on custom tags propagating.

# COMMAND ----------

display(spark.sql(f"""
  SELECT usage_date, sku_name, ROUND(SUM(usage_quantity), 3) AS dbus
  FROM system.billing.usage
  WHERE usage_metadata.dlt_pipeline_id = '{PIPELINE_ID}'
    AND usage_date >= current_date() - INTERVAL 14 DAYS
  GROUP BY usage_date, sku_name
  ORDER BY usage_date DESC
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Takeaway
# MAGIC - **APPLY CHANGES** rows-written ≈ the per-cycle churn (and **0** when nothing changed) — cost scales with *change volume*.
# MAGIC - **MV dedup** rewrites the whole table every refresh — cost scales with *table size + accumulated history*.
# MAGIC - Both produce identical silver. Use APPLY CHANGES for the CDC silver dimensions; use MVs for the gold aggregations, where they incrementally maintain.
# MAGIC - Captured numbers: `docs/measurement_results.md`.
