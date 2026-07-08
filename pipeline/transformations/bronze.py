# ============================================================================
# BRONZE — raw Artie CDC change feed, one streaming table per Summit entity.
#
# Ingestion is PER-ENTITY (config `files_entities`, a comma-separated list). Both
# lanes normalize into the SAME `bronze_<entity>_cdc` change stream, so silver/gold
# are identical regardless of how a table arrived:
#
#   cdf   (default)  — Artie MERGES changes in place into current-state Delta tables
#                      (`src_<entity>`); we read their Change Data Feed. Fits the
#                      transactional Summit tables (accounts, trades, holdings, …).
#   files            — an append-only change feed as Parquet files in a Volume, read
#                      with Auto Loader. Fits file-delivered / reference sources
#                      (e.g. the instrument master) and Artie's history mode.
#
# The CDF read must be Python (SQL streaming can't read a change feed).
# ============================================================================
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG    = spark.conf.get("catalog")
SRC_SCHEMA = spark.conf.get("src_schema")
SRC_VOLUME = spark.conf.get("source_volume")
# Entities that arrive as an append-only file feed; everything else uses CDF.
FILES_ENTITIES = {e.strip() for e in spark.conf.get("files_entities", "").split(",") if e.strip()}

ENTITIES = {
    "accounts": "account_id", "securities": "security_id", "trades": "trade_id",
    "holdings": "holding_id", "contract_notes": "contract_note_id",
}


def _make_bronze(entity: str, key: str):
    mode = "files" if entity in FILES_ENTITIES else "cdf"

    @dp.table(
        name=f"bronze_{entity}_cdc",
        cluster_by=[key],
        comment=f"Raw Artie CDC change feed for Summit {entity} (ingest={mode})",
    )
    @dp.expect_or_drop("valid_key", f"{key} IS NOT NULL")
    @dp.expect_or_drop("valid_op", "_change_type IN ('insert','update','delete')")
    @dp.expect_or_drop("valid_seq", "_commit_version IS NOT NULL")
    def _bronze():
        if mode == "cdf":
            # Merge-in-place: stream the Change Data Feed of Artie's current-state table.
            # startingVersion=0 so the first pipeline run captures the initial snapshot too.
            df = (
                spark.readStream
                .option("readChangeFeed", "true")
                .option("startingVersion", "0")
                .table(f"{CATALOG}.{SRC_SCHEMA}.src_{entity}")
                .filter(F.col("_change_type") != "update_preimage")
                .withColumn(
                    "_change_type",
                    F.when(F.col("_change_type") == "update_postimage", F.lit("update"))
                     .otherwise(F.col("_change_type")),
                )
                .drop("_commit_timestamp")
                .withColumn("_source_file", F.lit(f"src_{entity}"))
            )
        else:
            # Append-only file feed: Auto Loader over Parquet change files.
            df = (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .option("cloudFiles.schemaLocation", f"{SRC_VOLUME}/_schema/{entity}")
                .load(f"{SRC_VOLUME}/{entity}/")
                .drop("_rescued_data")  # keep silver's COLUMNS * EXCEPT list consistent with cdf mode
                .withColumn("_source_file", F.col("_metadata.file_path"))
            )
        return df.withColumn("_ingested_at", F.current_timestamp())

    return _bronze


for _entity, _key in ENTITIES.items():
    _make_bronze(_entity, _key)
