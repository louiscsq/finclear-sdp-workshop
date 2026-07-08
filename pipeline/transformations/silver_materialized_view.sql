-- ============================================================================
-- SILVER (approach B) — MATERIALIZED VIEW dedup  [comparison arm]
--
-- Produces the SAME current-state dimension as approach A, but with a window
-- (ROW_NUMBER) dedup over the full change log. This is the natural "SQL way" to
-- dedup — and it works — but ROW_NUMBER cannot be incrementally maintained, so
-- each refresh RE-COMPUTES over all change events accumulated so far.
--
-- These *_mv tables exist only to MEASURE cost/perf against the APPLY CHANGES
-- arm (see notebooks/20_measure_mv_vs_apply.py). Gold is built on the APPLY
-- CHANGES tables. At FinClear's ~18% churn on large tables, watch the recompute
-- cost of these grow with history while the APPLY CHANGES tables stay flat.
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW silver_accounts_mv
COMMENT 'Current-state accounts via MV dedup (recompute) — comparison vs silver_accounts'
AS
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY _commit_version DESC) AS _rn
  FROM bronze_accounts_changes
)
SELECT * EXCEPT (_change_type, _commit_version, _ingested_at, _source_file, _rn)
FROM ranked
WHERE _rn = 1 AND _change_type <> 'delete';

CREATE OR REFRESH MATERIALIZED VIEW silver_securities_mv AS
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY security_id ORDER BY _commit_version DESC) AS _rn
  FROM bronze_securities_changes
)
SELECT * EXCEPT (_change_type, _commit_version, _ingested_at, _source_file, _rn)
FROM ranked
WHERE _rn = 1 AND _change_type <> 'delete';

CREATE OR REFRESH MATERIALIZED VIEW silver_trades_mv AS
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY trade_id ORDER BY _commit_version DESC) AS _rn
  FROM bronze_trades_changes
)
SELECT * EXCEPT (_change_type, _commit_version, _ingested_at, _source_file, _rn)
FROM ranked
WHERE _rn = 1 AND _change_type <> 'delete';

CREATE OR REFRESH MATERIALIZED VIEW silver_holdings_mv AS
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY holding_id ORDER BY _commit_version DESC) AS _rn
  FROM bronze_holdings_changes
)
SELECT * EXCEPT (_change_type, _commit_version, _ingested_at, _source_file, _rn)
FROM ranked
WHERE _rn = 1 AND _change_type <> 'delete';

CREATE OR REFRESH MATERIALIZED VIEW silver_contract_notes_mv AS
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY contract_note_id ORDER BY _commit_version DESC) AS _rn
  FROM bronze_contract_notes_changes
)
SELECT * EXCEPT (_change_type, _commit_version, _ingested_at, _source_file, _rn)
FROM ranked
WHERE _rn = 1 AND _change_type <> 'delete';
