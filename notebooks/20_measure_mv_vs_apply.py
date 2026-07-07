# Databricks notebook source
# MAGIC %md
# MAGIC # Measure: MV dedup vs APPLY CHANGES — cost / performance
# MAGIC
# MAGIC Run **after** the initial load + a few CDC cycles + pipeline runs. Compares, per pipeline
# MAGIC update, how many rows each silver approach wrote:
# MAGIC
# MAGIC | Approach | Table(s) | Metric source | Expected |
# MAGIC |---|---|---|---|
# MAGIC | `APPLY CHANGES` | `silver_*` (streaming tables) | Delta `DESCRIBE HISTORY` MERGE metrics | ≈ churn per cycle (flat) |
# MAGIC | MV dedup | `silver_*_mv` (materialized views) | pipeline event log `num_output_rows` | ≈ full table every refresh |
# MAGIC
# MAGIC APPLY CHANGES writes only what changed; the ROW_NUMBER MV recomputes the whole table each
# MAGIC refresh (it can't be incrementally maintained) — the concrete form of FinClear's
# MAGIC incremental-vs-full-refresh cost question at ~18% churn.

# COMMAND ----------

dbutils.widgets.text("pipeline_id", "", "SDP pipeline ID")
dbutils.widgets.text("catalog", "finclear_sdp_demo_catalog", "Catalog")
dbutils.widgets.text("schema", "sdp_demo", "Schema")

PIPELINE_ID = dbutils.widgets.get("pipeline_id")
C = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"

ENTITIES = ["accounts", "securities", "trades", "holdings", "contract_notes"]

# COMMAND ----------
# MAGIC %md ## A. APPLY CHANGES — rows written per MERGE (Delta history)
# MAGIC Streaming tables are real Delta tables; each pipeline update applies a MERGE whose
# MAGIC `operationMetrics` record exactly how many rows were inserted / updated / deleted.

# COMMAND ----------

from functools import reduce
hist = reduce(lambda a, b: a.unionByName(b), [
    spark.sql(f"""
      SELECT '{e}' AS entity, version, timestamp, operation,
             CAST(operationMetrics['numTargetRowsInserted'] AS BIGINT) AS inserted,
             CAST(operationMetrics['numTargetRowsUpdated']  AS BIGINT) AS updated,
             CAST(operationMetrics['numTargetRowsDeleted']  AS BIGINT) AS deleted
      FROM (DESCRIBE HISTORY {C}.silver_{e})
      WHERE operation = 'MERGE'
    """) for e in ENTITIES
])
from pyspark.sql import functions as F
(hist.withColumn("rows_written", F.coalesce("inserted", F.lit(0)) + F.coalesce("updated", F.lit(0)) + F.coalesce("deleted", F.lit(0)))
     .groupBy("timestamp").agg(F.sum("rows_written").alias("apply_changes_rows_written"))
     .orderBy("timestamp").show(50, truncate=False))

# COMMAND ----------
# MAGIC %md ## B. MV dedup — rows written per refresh (event log)

# COMMAND ----------

spark.sql(f"""
  SELECT date_format(timestamp,'HH:mm:ss') AS refresh_time,
         origin.flow_name AS mv,
         CAST(details:flow_progress.metrics.num_output_rows AS BIGINT) AS mv_rows_written_recompute
  FROM event_log('{PIPELINE_ID}')
  WHERE event_type='flow_progress'
    AND details:flow_progress.status='COMPLETED'
    AND origin.flow_name LIKE '%_mv'
    AND details:flow_progress.metrics.num_output_rows IS NOT NULL
  ORDER BY timestamp
""").show(100, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## C. Actual serverless cost (system.billing) — tagged
# MAGIC Billing data lags (hours), so this is for the follow-up rather than the live run.

# COMMAND ----------

spark.sql("""
  SELECT usage_date, custom_tags['project'] AS project, sku_name, SUM(usage_quantity) AS dbus
  FROM system.billing.usage
  WHERE custom_tags['project'] = 'finclear-sdp-accelerator'
    AND usage_date >= current_date() - INTERVAL 7 DAYS
  GROUP BY usage_date, custom_tags['project'], sku_name
  ORDER BY usage_date DESC
""").show(50, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Takeaway
# MAGIC APPLY CHANGES rows-written ≈ the per-cycle churn (and **0** when nothing changed); the MV
# MAGIC rewrites the whole table every refresh regardless. Both yield identical current-state
# MAGIC silver. Use APPLY CHANGES for CDC silver dimensions; use MVs for the gold aggregations,
# MAGIC where they incrementally maintain. See `docs/measurement_results.md` for captured numbers.
