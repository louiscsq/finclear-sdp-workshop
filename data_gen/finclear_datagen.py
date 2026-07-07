# Databricks notebook source
# MAGIC %md
# MAGIC # FinClear — "Fake Artie" data generator + CDC simulator
# MAGIC
# MAGIC Simulates the FinClear **Summit** capital-markets source, the way **Artie** delivers it to
# MAGIC the lakehouse: an **append-only change feed** (insert / update / delete events, each with a
# MAGIC monotonic `_commit_version`) landed as Parquet files in a Unity Catalog **Volume**.
# MAGIC
# MAGIC The SDP pipeline reads these files with Auto Loader (bronze), then `APPLY CHANGES` /
# MAGIC materialized views collapse them to current-state silver.
# MAGIC
# MAGIC > **Production note.** In change-feed / history mode, Artie emits exactly this kind of
# MAGIC > append-only stream. If Artie instead **merges in place** into a Delta table, enable
# MAGIC > Change Data Feed on that table and read `table_changes()` — it yields the identical
# MAGIC > `_change_type` / `_commit_version` feed, and everything downstream is unchanged.
# MAGIC
# MAGIC Run the **initial load** once, then run the **CDC cycle** cell as many times as you like
# MAGIC (each run emits ~18% updates + a few deletes + some duplicate / out-of-order events, the
# MAGIC realistic churn FinClear quoted). Re-run the SDP pipeline between cycles to see incremental
# MAGIC processing.

# COMMAND ----------

dbutils.widgets.text("catalog", "finclear_demo", "Target catalog")
dbutils.widgets.text("schema", "sdp_demo", "Target schema")
dbutils.widgets.text("volume", "artie_cdc", "Landing volume (change files)")
# Light defaults — dial up for a bigger cost gap, down for a faster live run.
dbutils.widgets.text("n_accounts", "10000", "Accounts")
dbutils.widgets.text("n_securities", "1000", "Securities")
dbutils.widgets.text("n_trades", "50000", "Trades")
dbutils.widgets.text("n_holdings", "20000", "Holdings")
dbutils.widgets.text("n_contract_notes", "50000", "Contract notes")
dbutils.widgets.text("update_pct", "0.18", "Update rate per cycle")
dbutils.widgets.text("delete_pct", "0.01", "Delete rate per cycle")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
VOL_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

from pyspark.sql import functions as F, Window

# Catalog is assumed to already exist (managed catalogs often can't be created ad hoc).
# Point the `catalog` param at an existing catalog you can write to.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# A tiny control table tracks the current entities (so cycles can pick real keys to mutate)
# and the global commit-version high-water mark, so sequencing is monotonic across cycles.
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}._sim_state
  (entity STRING, max_id BIGINT, commit_version BIGINT)
""")

ENTITIES = ["accounts", "securities", "trades", "holdings", "contract_notes"]


def _next_commit_version(n_rows: int) -> int:
    """Reserve a contiguous block of commit versions; returns the block start."""
    row = spark.sql(f"SELECT COALESCE(MAX(commit_version),0) AS v FROM {CATALOG}.{SCHEMA}._sim_state").first()
    start = int(row["v"]) + 1
    spark.sql(f"INSERT INTO {CATALOG}.{SCHEMA}._sim_state VALUES ('_cv', 0, {start + n_rows})")
    return start


def _write_changes(entity: str, df):
    """Land a change batch as Parquet under the entity's Volume folder."""
    (df.write.mode("append").parquet(f"{VOL_PATH}/{entity}/"))


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


def build_contract_notes(id_df, n_trades):
    status = F.array(*[F.lit(x) for x in ["Issued", "Issued", "Amended", "Cancelled"]])
    return (id_df
        .withColumn("contract_note_id", F.col("id"))
        .withColumn("trade_id", (F.rand() * n_trades).cast("bigint") + 1)
        .withColumn("account_id", (F.rand() * int(dbutils.widgets.get("n_accounts"))).cast("bigint") + 1)
        .withColumn("issue_date", F.date_sub(F.current_date(), (F.rand() * 365).cast("int")))
        .withColumn("gross_amount", F.round(F.rand() * 100000 + 100, 2))
        .withColumn("brokerage", F.round(F.col("gross_amount") * 0.001 + 5, 2))
        .withColumn("gst", F.round(F.col("brokerage") * 0.1, 2))
        .withColumn("net_amount", F.round(F.col("gross_amount") + F.col("brokerage") + F.col("gst"), 2))
        .withColumn("status", status[(F.rand() * 4).cast("int")])
        .select("contract_note_id", "trade_id", "account_id", "issue_date", "gross_amount",
                "brokerage", "gst", "net_amount", "status"))


BUILDERS = {
    "accounts": lambda idf: build_accounts(idf),
    "securities": lambda idf: build_securities(idf),
    "trades": lambda idf: build_trades(idf, int(dbutils.widgets.get("n_accounts")), int(dbutils.widgets.get("n_securities"))),
    "holdings": lambda idf: build_holdings(idf, int(dbutils.widgets.get("n_accounts")), int(dbutils.widgets.get("n_securities"))),
    "contract_notes": lambda idf: build_contract_notes(idf, int(dbutils.widgets.get("n_trades"))),
}
KEY = {
    "accounts": "account_id", "securities": "security_id", "trades": "trade_id",
    "holdings": "holding_id", "contract_notes": "contract_note_id",
}
COUNT = {
    "accounts": int(dbutils.widgets.get("n_accounts")),
    "securities": int(dbutils.widgets.get("n_securities")),
    "trades": int(dbutils.widgets.get("n_trades")),
    "holdings": int(dbutils.widgets.get("n_holdings")),
    "contract_notes": int(dbutils.widgets.get("n_contract_notes")),
}


# COMMAND ----------
# MAGIC %md ## Initial load — one `insert` change per key, per entity

# COMMAND ----------

for entity in ENTITIES:
    n = COUNT[entity]
    ids = spark.range(1, n + 1).withColumnRenamed("id", "id")
    rows = BUILDERS[entity](ids)
    cv = _next_commit_version(n)
    w = Window.orderBy(F.col(KEY[entity]))
    changes = (rows
        .withColumn("_change_type", F.lit("insert"))
        .withColumn("_commit_version", F.lit(cv) + F.row_number().over(w) - 1))
    _write_changes(entity, changes)
    print(f"[initial] {entity}: {n} inserts (commit_version {cv}..{cv + n - 1})")

print("Initial load complete →", VOL_PATH)

# COMMAND ----------
# MAGIC %md
# MAGIC ## CDC cycle — run repeatedly
# MAGIC Emits, per entity: ~`update_pct` updates (mutated attributes, new commit_version),
# MAGIC ~`delete_pct` deletes, plus a handful of **duplicate** and **out-of-order** events so you
# MAGIC can see `APPLY CHANGES … SEQUENCE BY` dedup/order them automatically.

# COMMAND ----------

UPDATE_PCT = float(dbutils.widgets.get("update_pct"))
DELETE_PCT = float(dbutils.widgets.get("delete_pct"))

for entity in ENTITIES:
    n = COUNT[entity]
    key = KEY[entity]
    n_upd = int(n * UPDATE_PCT)
    n_del = int(n * DELETE_PCT)
    total = n_upd + n_del + max(1, n_upd // 20)  # + ~5% duplicates
    cv = _next_commit_version(total + 10)

    # Updates: sample existing keys, rebuild attributes, mark as update.
    upd_ids = (spark.range(1, n + 1).orderBy(F.rand()).limit(n_upd).withColumnRenamed("id", "id"))
    upd = BUILDERS[entity](upd_ids).withColumn("_change_type", F.lit("update"))

    # Deletes: sample existing keys (payload columns null except key).
    del_ids = spark.range(1, n + 1).orderBy(F.rand()).limit(n_del)
    del_rows = BUILDERS[entity](del_ids).withColumn("_change_type", F.lit("delete"))

    # Duplicates: re-emit a few update rows (APPLY CHANGES should collapse these).
    dups = upd.limit(max(1, n_upd // 20))

    batch = upd.unionByName(del_rows).unionByName(dups)
    # Assign commit versions, then shuffle to simulate out-of-order arrival within the batch.
    w = Window.orderBy(F.rand())
    batch = batch.withColumn("_commit_version", F.lit(cv) + F.row_number().over(w) - 1)
    _write_changes(entity, batch)
    print(f"[cycle] {entity}: {n_upd} upd, {n_del} del, ~{max(1, n_upd//20)} dup")

print("CDC cycle complete. Re-run the SDP pipeline to process incrementally.")
