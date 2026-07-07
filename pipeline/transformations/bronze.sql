-- ============================================================================
-- BRONZE — raw Artie CDC change feed (append-only), ingested with Auto Loader.
-- One streaming table per Summit source entity.
--
-- Expectations guard the CHANGE METADATA only. Business-rule data-quality checks
-- live in silver, because bronze must tolerate `delete` events (which carry the
-- key and mostly nulls). `${source_volume}` is a pipeline configuration parameter.
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_accounts_cdc
(
  CONSTRAINT valid_key EXPECT (account_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_op  EXPECT (_change_type IN ('insert','update','delete')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_seq EXPECT (_commit_version IS NOT NULL)
)
COMMENT 'Raw Artie CDC feed for Summit accounts (append-only change events)'
CLUSTER BY (account_id)
AS SELECT *, current_timestamp() AS _ingested_at, _metadata.file_path AS _source_file
FROM STREAM read_files('${source_volume}/accounts/', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE bronze_securities_cdc
(
  CONSTRAINT valid_key EXPECT (security_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_op  EXPECT (_change_type IN ('insert','update','delete')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_seq EXPECT (_commit_version IS NOT NULL)
)
COMMENT 'Raw Artie CDC feed for Summit securities (instrument reference)'
CLUSTER BY (security_id)
AS SELECT *, current_timestamp() AS _ingested_at, _metadata.file_path AS _source_file
FROM STREAM read_files('${source_volume}/securities/', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE bronze_trades_cdc
(
  CONSTRAINT valid_key EXPECT (trade_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_op  EXPECT (_change_type IN ('insert','update','delete')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_seq EXPECT (_commit_version IS NOT NULL)
)
COMMENT 'Raw Artie CDC feed for Summit trades'
CLUSTER BY (trade_id)
AS SELECT *, current_timestamp() AS _ingested_at, _metadata.file_path AS _source_file
FROM STREAM read_files('${source_volume}/trades/', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE bronze_holdings_cdc
(
  CONSTRAINT valid_key EXPECT (holding_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_op  EXPECT (_change_type IN ('insert','update','delete')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_seq EXPECT (_commit_version IS NOT NULL)
)
COMMENT 'Raw Artie CDC feed for Summit holdings (positions)'
CLUSTER BY (holding_id)
AS SELECT *, current_timestamp() AS _ingested_at, _metadata.file_path AS _source_file
FROM STREAM read_files('${source_volume}/holdings/', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE bronze_contract_notes_cdc
(
  CONSTRAINT valid_key EXPECT (contract_note_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_op  EXPECT (_change_type IN ('insert','update','delete')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_seq EXPECT (_commit_version IS NOT NULL)
)
COMMENT 'Raw Artie CDC feed for Summit contract notes'
CLUSTER BY (contract_note_id)
AS SELECT *, current_timestamp() AS _ingested_at, _metadata.file_path AS _source_file
FROM STREAM read_files('${source_volume}/contract_notes/', format => 'parquet');
