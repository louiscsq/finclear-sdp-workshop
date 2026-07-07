# Databricks notebook source
# MAGIC %md
# MAGIC # Measure: MV dedup vs APPLY CHANGES — cost / performance
# MAGIC
# MAGIC Run this **after** at least one initial load + a few CDC cycles + pipeline runs.
# MAGIC It reads the pipeline **event log** and compares, per update, how much work each silver
# MAGIC approach did:
# MAGIC
# MAGIC | Approach | Table(s) | Expected behavior |
# MAGIC |---|---|---|
# MAGIC | `APPLY CHANGES` | `silver_*` | **Incremental** — rows processed ≈ new change events per cycle (flat) |
# MAGIC | MV dedup (ROW_NUMBER) | `silver_*_mv` | **Recompute** — rows scanned ≈ full change log so far (grows each cycle) |
# MAGIC
# MAGIC The gap widens with history and table size — the concrete version of FinClear's
# MAGIC incremental-vs-full-refresh cost question at ~18% churn.

# COMMAND ----------

dbutils.widgets.text("pipeline_id", "", "SDP pipeline ID")
dbutils.widgets.text("catalog", "finclear_demo", "Catalog")
dbutils.widgets.text("schema", "sdp_demo", "Schema")

PIPELINE_ID = dbutils.widgets.get("pipeline_id")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md ## 1. Load the pipeline event log

# COMMAND ----------

events = spark.sql(f"SELECT * FROM event_log('{PIPELINE_ID}')")
events.createOrReplaceTempView("events")
print("event rows:", events.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Rows written per flow, per update
# MAGIC `flow_progress` events carry `details:flow_progress.metrics.num_output_rows`. We bucket
# MAGIC each flow as APPLY CHANGES (`silver_*`, not `_mv`) or MV (`silver_*_mv`).

# COMMAND ----------

flow_metrics = spark.sql("""
  SELECT
    origin.update_id,
    origin.flow_name                                                       AS flow_name,
    CAST(details:flow_progress.metrics.num_output_rows AS BIGINT)          AS num_output_rows,
    CAST(details:flow_progress.metrics.num_upserted_rows AS BIGINT)        AS num_upserted_rows,
    CAST(details:flow_progress.metrics.num_deleted_rows AS BIGINT)         AS num_deleted_rows,
    timestamp
  FROM events
  WHERE event_type = 'flow_progress'
    AND details:flow_progress.status = 'COMPLETED'
    AND details:flow_progress.metrics.num_output_rows IS NOT NULL
""")

summary = (flow_metrics
  .withColumn("approach", F.when(F.col("flow_name").endswith("_mv"), F.lit("MV dedup (recompute)"))
                           .when(F.col("flow_name").startswith("silver_"), F.lit("APPLY CHANGES (incremental)"))
                           .otherwise(F.lit("other")))
  .filter(F.col("approach") != "other"))

print("Per-update rows written by approach (watch MV grow, APPLY CHANGES stay ~flat):")
(summary.groupBy("update_id", "approach")
        .agg(F.sum("num_output_rows").alias("rows_written"))
        .orderBy("update_id", "approach")
        .show(100, truncate=False))

print("Total rows written across all updates, by approach:")
summary.groupBy("approach").agg(
    F.sum("num_output_rows").alias("total_rows_written"),
    F.countDistinct("flow_name").alias("num_flows"),
).show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Did the MV recompute or refresh incrementally?
# MAGIC MV `planning_information` reports whether a full recompute or a row-based incremental
# MAGIC refresh was chosen. ROW_NUMBER dedup generally forces **complete recomputation**.

# COMMAND ----------

spark.sql("""
  SELECT
    origin.flow_name,
    details:planning_information:technique                     AS refresh_technique,
    timestamp
  FROM events
  WHERE event_type = 'planning_information'
    AND origin.flow_name LIKE 'silver_%_mv'
  ORDER BY timestamp DESC
""").show(50, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Actual serverless cost (system.billing) — tagged
# MAGIC Billing data lags (hours), so this is for the follow-up rather than the live run.
# MAGIC Filters to the pipeline's cost tags set in the bundle.

# COMMAND ----------

spark.sql(f"""
  SELECT
    usage_date,
    custom_tags['project']       AS project,
    sku_name,
    SUM(usage_quantity)          AS dbus
  FROM system.billing.usage
  WHERE custom_tags['project'] = 'finclear-sdp-accelerator'
    AND usage_date >= current_date() - INTERVAL 7 DAYS
  GROUP BY usage_date, custom_tags['project'], sku_name
  ORDER BY usage_date DESC
""").show(50, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Talking point
# MAGIC APPLY CHANGES processes only the rows that changed each cycle; the ROW_NUMBER MV
# MAGIC re-scans the whole change log every refresh. Both produce identical current-state silver —
# MAGIC but at FinClear's volumes and 18% churn, the incremental approach is materially cheaper,
# MAGIC and the gap compounds as history accumulates. Use MVs where they shine instead: the
# MAGIC **gold** aggregations, which incrementally maintain.
