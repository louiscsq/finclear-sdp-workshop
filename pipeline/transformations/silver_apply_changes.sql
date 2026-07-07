-- ============================================================================
-- SILVER (approach A) — APPLY CHANGES / AUTO CDC.
--
-- Collapses the append-only bronze change feed into current-state dimensions,
-- INCREMENTALLY (streaming). APPLY CHANGES handles for free the three things you
-- otherwise hand-code in bronze:
--   * dedup       — duplicate change events are dropped
--   * ordering    — SEQUENCE BY resolves out-of-order arrivals (keeps latest)
--   * deletes     — APPLY AS DELETE removes the row
--
-- SCD TYPE 1 = current state only. SCD TYPE 2 = full history (__START_AT/__END_AT),
-- the native equivalent of Artie "history mode".
-- ============================================================================

-- ---- Accounts: current state (SCD1) + full history (SCD2) --------------------
CREATE OR REFRESH STREAMING TABLE silver_accounts
(
  CONSTRAINT valid_status EXPECT (status IN ('Active','Suspended','Closed')),
  CONSTRAINT has_email    EXPECT (email IS NOT NULL)
)
COMMENT 'Current-state accounts (deduped, deletes applied) — via APPLY CHANGES SCD1';

APPLY CHANGES INTO silver_accounts
FROM STREAM(bronze_accounts_cdc)
KEYS (account_id)
APPLY AS DELETE WHEN _change_type = 'delete'
SEQUENCE BY _commit_version
COLUMNS * EXCEPT (_change_type, _commit_version, _ingested_at, _source_file)
STORED AS SCD TYPE 1;

CREATE OR REFRESH STREAMING TABLE silver_accounts_history
COMMENT 'Full change history of accounts (SCD Type 2) — Artie history-mode equivalent';

APPLY CHANGES INTO silver_accounts_history
FROM STREAM(bronze_accounts_cdc)
KEYS (account_id)
APPLY AS DELETE WHEN _change_type = 'delete'
SEQUENCE BY _commit_version
COLUMNS * EXCEPT (_change_type, _commit_version, _ingested_at, _source_file)
STORED AS SCD TYPE 2;

-- ---- Securities (SCD1) -------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_securities
COMMENT 'Current-state instrument reference — via APPLY CHANGES SCD1';

APPLY CHANGES INTO silver_securities
FROM STREAM(bronze_securities_cdc)
KEYS (security_id)
APPLY AS DELETE WHEN _change_type = 'delete'
SEQUENCE BY _commit_version
COLUMNS * EXCEPT (_change_type, _commit_version, _ingested_at, _source_file)
STORED AS SCD TYPE 1;

-- ---- Trades (SCD1) -----------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_trades
(
  -- DROP: hard data-quality guardrails (invalid rows are dropped)
  CONSTRAINT valid_qty EXPECT (quantity > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amt EXPECT (gross_amount >= 0) ON VIOLATION DROP ROW,
  -- WARN: monitoring signal — flag unusually large trades for review (row is kept)
  CONSTRAINT large_trade_review EXPECT (gross_amount < 500000)
)
COMMENT 'Current-state trades — via APPLY CHANGES SCD1';

APPLY CHANGES INTO silver_trades
FROM STREAM(bronze_trades_cdc)
KEYS (trade_id)
APPLY AS DELETE WHEN _change_type = 'delete'
SEQUENCE BY _commit_version
COLUMNS * EXCEPT (_change_type, _commit_version, _ingested_at, _source_file)
STORED AS SCD TYPE 1;

-- ---- Holdings (SCD1) ---------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_holdings
COMMENT 'Current-state holdings (positions) — via APPLY CHANGES SCD1';

APPLY CHANGES INTO silver_holdings
FROM STREAM(bronze_holdings_cdc)
KEYS (holding_id)
APPLY AS DELETE WHEN _change_type = 'delete'
SEQUENCE BY _commit_version
COLUMNS * EXCEPT (_change_type, _commit_version, _ingested_at, _source_file)
STORED AS SCD TYPE 1;

-- ---- Contract notes (SCD1) ---------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_contract_notes
COMMENT 'Current-state contract notes — via APPLY CHANGES SCD1';

APPLY CHANGES INTO silver_contract_notes
FROM STREAM(bronze_contract_notes_cdc)
KEYS (contract_note_id)
APPLY AS DELETE WHEN _change_type = 'delete'
SEQUENCE BY _commit_version
COLUMNS * EXCEPT (_change_type, _commit_version, _ingested_at, _source_file)
STORED AS SCD TYPE 1;
