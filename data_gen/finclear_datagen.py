# Databricks notebook source
# MAGIC %md
# MAGIC # FinClear — "Fake Artie" data generator + CDC simulator
# MAGIC
# MAGIC Simulates the FinClear **Summit** capital-markets source, the way **Artie** delivers it.
# MAGIC Two sinks (widget `sink`):
# MAGIC
# MAGIC - **`cdf`** *(default)* — Artie **merges changes in place** into current-state `src_<entity>`
# MAGIC   Delta tables with Change Data Feed enabled. Bronze reads their CDF. This matches FinClear's
# MAGIC   likely Artie setup ("raw source copy, one object per source table" + "CDC merge into Delta").
# MAGIC - **`files`** *(fallback)* — Artie emits an **append-only change feed** as Parquet files in a
# MAGIC   Volume; bronze reads them with Auto Loader.
# MAGIC
# MAGIC Both yield the identical `_change_type` / `_commit_version` change stream downstream.
# MAGIC
# MAGIC **`mode`**: `init` (initial load only) · `cycle` (one CDC cycle) · `both` (default).
# MAGIC Run `init` once, then `cycle` repeatedly (re-run the SDP pipeline between cycles to see
# MAGIC incremental processing). Each cycle emits ~18% updates + a few deletes + duplicate /
# MAGIC out-of-order events — the churn FinClear quoted.

# COMMAND ----------

dbutils.widgets.dropdown("mode", "both", ["init", "cycle", "both"], "Run mode")
dbutils.widgets.dropdown("sink", "cdf", ["cdf", "files"], "Artie sink")
dbutils.widgets.text("catalog", "finclear_sdp_demo_catalog", "Target catalog")
dbutils.widgets.text("schema", "sdp_workshop", "Target schema")
dbutils.widgets.text("src_schema", "", "Schema for Artie src tables (cdf mode; blank=schema)")
dbutils.widgets.text("volume", "artie_cdc", "Landing volume (files mode)")
# Light defaults — dial up for a bigger cost gap, down for a faster live run.
dbutils.widgets.text("n_accounts", "10000", "Accounts")
dbutils.widgets.text("n_securities", "1000", "Securities")
dbutils.widgets.text("n_trades", "50000", "Trades")
dbutils.widgets.text("n_holdings", "20000", "Holdings")
dbutils.widgets.text("n_contract_notes", "50000", "Contract notes")
dbutils.widgets.text("update_pct", "0.18", "Update rate per cycle")
dbutils.widgets.text("delete_pct", "0.01", "Delete rate per cycle")

MODE = dbutils.widgets.get("mode")
SINK = dbutils.widgets.get("sink")            # 'cdf' (merge-in-place, default) | 'files'
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SRC_SCHEMA = dbutils.widgets.get("src_schema") or SCHEMA
VOLUME = dbutils.widgets.get("volume")
VOL_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
UPDATE_PCT = float(dbutils.widgets.get("update_pct"))
DELETE_PCT = float(dbutils.widgets.get("delete_pct"))

from pyspark.sql import functions as F, Window

# Catalog is assumed to already exist (managed catalogs often can't be created ad hoc).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SRC_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# Control table: tracks the global commit-version high-water mark so sequencing is
# monotonic across runs.
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}._sim_state
  (entity STRING, max_id BIGINT, commit_version BIGINT)
""")

ENTITIES = ["accounts", "securities", "trades", "holdings", "contract_notes"]
KEY = {"accounts": "account_id", "securities": "security_id", "trades": "trade_id",
       "holdings": "holding_id", "contract_notes": "contract_note_id"}
COUNT = {e: int(dbutils.widgets.get(f"n_{e}")) for e in ENTITIES}


def _next_commit_version(n_rows: int) -> int:
    row = spark.sql(f"SELECT COALESCE(MAX(commit_version),0) AS v FROM {CATALOG}.{SCHEMA}._sim_state").first()
    start = int(row["v"]) + 1
    spark.sql(f"INSERT INTO {CATALOG}.{SCHEMA}._sim_state VALUES ('_cv', 0, {start + n_rows})")
    return start


def _write_changes(entity: str, df):
    """files sink: append the change batch as Parquet under the entity's Volume folder."""
    df.write.mode("append").parquet(f"{VOL_PATH}/{entity}/")


def _apply_cdf(entity: str, changes, is_initial: bool):
    """cdf sink: MERGE the change batch into the current-state `src_<entity>` Delta table
    (CDF enabled) — i.e. Artie merging in place. Bronze then reads the Change Data Feed."""
    key = KEY[entity]
    tgt = f"{CATALOG}.{SRC_SCHEMA}.src_{entity}"
    biz_cols = [c for c in changes.columns if c not in ("_change_type", "_commit_version")]
    if is_initial:
        biz = changes.select(*biz_cols)
        # Create empty CDF-enabled table, then append inserts (captured by CDF from v1).
        (biz.limit(0).write.mode("overwrite")
            .option("delta.enableChangeDataFeed", "true").saveAsTable(tgt))
        spark.sql(f"ALTER TABLE {tgt} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        biz.write.mode("append").saveAsTable(tgt)
    else:
        # MERGE requires a deduped source: keep the latest change per key.
        w = Window.partitionBy(key).orderBy(F.col("_commit_version").desc())
        staged = changes.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
        staged.createOrReplaceTempView("_stg")
        set_clause = ", ".join(f"t.`{c}` = s.`{c}`" for c in biz_cols if c != key)
        ins_cols = ", ".join(f"`{c}`" for c in biz_cols)
        ins_vals = ", ".join(f"s.`{c}`" for c in biz_cols)
        spark.sql(f"""
            MERGE INTO {tgt} t USING _stg s ON t.`{key}` = s.`{key}`
            WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED AND s._change_type <> 'delete' THEN INSERT ({ins_cols}) VALUES ({ins_vals})
        """)


def persist(entity: str, changes, is_initial: bool):
    if SINK == "files":
        _write_changes(entity, changes)
    else:
        _apply_cdf(entity, changes, is_initial)


# COMMAND ----------
# MAGIC %md ## Row builders (pure-Spark, no external deps — fast on serverless)

# COMMAND ----------

def build_accounts(id_df):
    types = F.array(*[F.lit(x) for x in ["Individual", "Joint", "Trust", "Company", "SMSF"]])
    statuses = F.array(*[F.lit(x) for x in ["Active", "Active", "Active", "Suspended", "Closed"]])
    risk = F.array(*[F.lit(x) for x in ["Conservative", "Balanced", "Growth", "Aggressive"]])
    return (id_df
        .withColumn("account_id", F.col("id"))
        .withColumn("account_name", F.concat(F.lit("Account "), F.col("id")))
        .withColumn("account_type", types[(F.rand() * 5).cast("int")])
        .withColumn("adviser_id", (F.rand() * 200).cast("int") + 1)
        .withColumn("status", statuses[(F.rand() * 5).cast("int")])
        .withColumn("risk_profile", risk[(F.rand() * 4).cast("int")])
        .withColumn("opened_date", F.date_sub(F.current_date(), (F.rand() * 3650).cast("int")))
        .withColumn("country", F.lit("AU"))
        .withColumn("email", F.concat(F.lit("client"), F.col("id"), F.lit("@example.com")))  # PII (mask target)
        .select("account_id", "account_name", "account_type", "adviser_id", "status",
                "risk_profile", "opened_date", "country", "email"))


def build_securities(id_df):
    ac = F.array(*[F.lit(x) for x in ["Equity", "ETF", "Bond", "Cash", "Managed Fund"]])
    return (id_df
        .withColumn("security_id", F.col("id"))
        .withColumn("ticker", F.concat(F.lit("ASX"), F.lpad(F.col("id").cast("string"), 4, "0")))
        .withColumn("name", F.concat(F.lit("Security "), F.col("id")))
        .withColumn("asset_class", ac[(F.rand() * 5).cast("int")])
        .withColumn("exchange", F.lit("ASX"))
        .withColumn("currency", F.lit("AUD"))
        .withColumn("sector", F.concat(F.lit("Sector "), (F.rand() * 11).cast("int")))
        .select("security_id", "ticker", "name", "asset_class", "exchange", "currency", "sector"))


def build_trades(id_df, n_acct, n_sec):
    side = F.array(F.lit("BUY"), F.lit("SELL"))
    status = F.array(*[F.lit(x) for x in ["Confirmed", "Settled", "Settled", "Cancelled"]])
    return (id_df
        .withColumn("trade_id", F.col("id"))
        .withColumn("account_id", (F.rand() * n_acct).cast("bigint") + 1)
        .withColumn("security_id", (F.rand() * n_sec).cast("bigint") + 1)
        .withColumn("trade_date", F.date_sub(F.current_date(), (F.rand() * 365).cast("int")))
        .withColumn("side", side[(F.rand() * 2).cast("int")])
        .withColumn("quantity", (F.rand() * 5000 + 1).cast("int"))
        .withColumn("price", F.round(F.rand() * 200 + 1, 2))
        .withColumn("gross_amount", F.round(F.col("quantity") * F.col("price"), 2))
        .withColumn("brokerage", F.round(F.col("gross_amount") * 0.001 + 5, 2))
        .withColumn("net_amount", F.round(F.col("gross_amount") + F.col("brokerage"), 2))
        .withColumn("currency", F.lit("AUD"))
        .withColumn("settlement_date", F.date_add(F.col("trade_date"), 2))
        .withColumn("status", status[(F.rand() * 4).cast("int")])
        .select("trade_id", "account_id", "security_id", "trade_date", "side", "quantity",
                "price", "gross_amount", "brokerage", "net_amount", "currency",
                "settlement_date", "status"))


def build_holdings(id_df, n_acct, n_sec):
    return (id_df
        .withColumn("holding_id", F.col("id"))
        .withColumn("account_id", (F.rand() * n_acct).cast("bigint") + 1)
        .withColumn("security_id", (F.rand() * n_sec).cast("bigint") + 1)
        .withColumn("units", (F.rand() * 10000 + 1).cast("int"))
        .withColumn("avg_cost", F.round(F.rand() * 150 + 1, 2))
        .withColumn("market_price", F.round(F.rand() * 200 + 1, 2))
        .withColumn("market_value", F.round(F.col("units") * F.col("market_price"), 2))
        .withColumn("cost_base", F.round(F.col("units") * F.col("avg_cost"), 2))
        .withColumn("unrealised_pnl", F.round(F.col("market_value") - F.col("cost_base"), 2))
        .withColumn("as_at_date", F.current_date())
        .select("holding_id", "account_id", "security_id", "units", "avg_cost", "market_price",
                "market_value", "cost_base", "unrealised_pnl", "as_at_date"))


def build_contract_notes(id_df, n_trades, n_acct):
    status = F.array(*[F.lit(x) for x in ["Issued", "Issued", "Amended", "Cancelled"]])
    return (id_df
        .withColumn("contract_note_id", F.col("id"))
        .withColumn("trade_id", (F.rand() * n_trades).cast("bigint") + 1)
        .withColumn("account_id", (F.rand() * n_acct).cast("bigint") + 1)
        .withColumn("issue_date", F.date_sub(F.current_date(), (F.rand() * 365).cast("int")))
        .withColumn("gross_amount", F.round(F.rand() * 100000 + 100, 2))
        .withColumn("brokerage", F.round(F.col("gross_amount") * 0.001 + 5, 2))
        .withColumn("gst", F.round(F.col("brokerage") * 0.1, 2))
        .withColumn("net_amount", F.round(F.col("gross_amount") + F.col("brokerage") + F.col("gst"), 2))
        .withColumn("status", status[(F.rand() * 4).cast("int")])
        .select("contract_note_id", "trade_id", "account_id", "issue_date", "gross_amount",
                "brokerage", "gst", "net_amount", "status"))


def build(entity, id_df):
    na, ns, nt = COUNT["accounts"], COUNT["securities"], COUNT["trades"]
    if entity == "accounts":       return build_accounts(id_df)
    if entity == "securities":     return build_securities(id_df)
    if entity == "trades":         return build_trades(id_df, na, ns)
    if entity == "holdings":       return build_holdings(id_df, na, ns)
    if entity == "contract_notes": return build_contract_notes(id_df, nt, na)


# COMMAND ----------
# MAGIC %md ## Initial load — one `insert` change per key, per entity

# COMMAND ----------

def initial_load():
    for entity in ENTITIES:
        n = COUNT[entity]
        rows = build(entity, spark.range(1, n + 1))
        cv = _next_commit_version(n)
        w = Window.orderBy(F.col(KEY[entity]))
        changes = (rows
            .withColumn("_change_type", F.lit("insert"))
            .withColumn("_commit_version", F.lit(cv) + F.row_number().over(w) - 1))
        persist(entity, changes, is_initial=True)
        print(f"[initial] {entity}: {n} inserts (sink={SINK})")
    print(f"Initial load complete (sink={SINK}).")


# COMMAND ----------
# MAGIC %md
# MAGIC ## CDC cycle
# MAGIC Per entity: ~`update_pct` updates (mutated attributes), ~`delete_pct` deletes, plus a few
# MAGIC **duplicate** / **out-of-order** events.
# MAGIC - In `files` mode these flow through raw, so `APPLY CHANGES … SEQUENCE BY` dedups/orders them.
# MAGIC - In `cdf` mode the MERGE below (i.e. Artie) already collapses them per key before the feed —
# MAGIC   which is exactly why merge-in-place output is clean; APPLY CHANGES still handles ordering,
# MAGIC   deletes, and SCD2 history.

# COMMAND ----------

def run_cycle():
    for entity in ENTITIES:
        n = COUNT[entity]
        n_upd = int(n * UPDATE_PCT)
        n_del = int(n * DELETE_PCT)
        n_dup = max(1, n_upd // 20)
        cv = _next_commit_version(n_upd + n_del + n_dup + 10)

        upd = build(entity, spark.range(1, n + 1).orderBy(F.rand()).limit(n_upd)).withColumn("_change_type", F.lit("update"))
        del_rows = build(entity, spark.range(1, n + 1).orderBy(F.rand()).limit(n_del)).withColumn("_change_type", F.lit("delete"))
        dups = upd.limit(n_dup)  # duplicates — APPLY CHANGES should collapse these

        batch = upd.unionByName(del_rows).unionByName(dups)
        # Assign commit versions on a shuffled order to simulate out-of-order arrival.
        w = Window.orderBy(F.rand())
        batch = batch.withColumn("_commit_version", F.lit(cv) + F.row_number().over(w) - 1)
        persist(entity, batch, is_initial=False)
        print(f"[cycle] {entity}: {n_upd} upd, {n_del} del, ~{n_dup} dup (sink={SINK})")
    print("CDC cycle complete. Re-run the SDP pipeline to process incrementally.")


# COMMAND ----------

if MODE in ("init", "both"):
    initial_load()
if MODE in ("cycle", "both"):
    run_cycle()
