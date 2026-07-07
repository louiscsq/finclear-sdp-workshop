-- ============================================================================
-- GOLD — report-ready materialized views (business-ready marts).
--
-- MVs are the right tool here: joins + aggregations incrementally maintain well,
-- and gold is read by Power BI (external, via TradeCentre) and AI/BI (internal).
-- Built on the APPLY CHANGES silver dimensions.
-- ============================================================================

-- Portfolio value & unrealised P&L per account × asset class.
CREATE OR REFRESH MATERIALIZED VIEW gold_portfolio_valuation
COMMENT 'Current portfolio market value, cost base and unrealised P&L per account and asset class'
AS
SELECT
  a.account_id,
  a.account_name,
  a.account_type,
  a.risk_profile,
  s.asset_class,
  COUNT(*)               AS num_holdings,
  SUM(h.market_value)    AS total_market_value,
  SUM(h.cost_base)       AS total_cost_base,
  SUM(h.unrealised_pnl)  AS total_unrealised_pnl
FROM silver_holdings h
JOIN silver_accounts   a ON h.account_id  = a.account_id
JOIN silver_securities s ON h.security_id = s.security_id
GROUP BY a.account_id, a.account_name, a.account_type, a.risk_profile, s.asset_class;

-- Daily trade activity by asset class (excludes cancelled).
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_trade_activity
COMMENT 'Daily trade counts and value by asset class'
AS
SELECT
  t.trade_date,
  s.asset_class,
  COUNT(*)                                        AS num_trades,
  SUM(CASE WHEN t.side = 'BUY'  THEN 1 ELSE 0 END) AS buy_count,
  SUM(CASE WHEN t.side = 'SELL' THEN 1 ELSE 0 END) AS sell_count,
  SUM(t.gross_amount)                             AS total_gross,
  SUM(t.brokerage)                                AS total_brokerage
FROM silver_trades t
JOIN silver_securities s ON t.security_id = s.security_id
WHERE t.status <> 'Cancelled'
GROUP BY t.trade_date, s.asset_class;

-- Contract-note summary by issue date and status.
CREATE OR REFRESH MATERIALIZED VIEW gold_contract_note_summary
COMMENT 'Contract-note volumes and value by issue date and status'
AS
SELECT
  issue_date,
  status,
  COUNT(*)           AS num_notes,
  SUM(gross_amount)  AS total_gross,
  SUM(brokerage)     AS total_brokerage,
  SUM(gst)           AS total_gst,
  SUM(net_amount)    AS total_net
FROM silver_contract_notes
GROUP BY issue_date, status;
