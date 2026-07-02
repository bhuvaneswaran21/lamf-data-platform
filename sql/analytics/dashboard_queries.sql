
SELECT loan_book, aum_pledged, total_dp, active_loans, wavg_ltv
FROM iceberg.gold.mart_portfolio_daily
ORDER BY as_of_date DESC LIMIT 1;

SELECT npa_bucket, loans, exposure, margin_calls, total_shortfall
FROM iceberg.gold.mart_risk_daily
ORDER BY exposure DESC;


SELECT loan_id, customer_id, outstanding, drawing_power, shortfall_amount, ltv_pct
FROM iceberg.gold.fact_loan_daily
WHERE margin_call_flag = true
ORDER BY shortfall_amount DESC
LIMIT 50;


SELECT CAST(floor(ltv_pct / 10) * 10 AS integer) AS ltv_bucket, count(*) AS loans
FROM iceberg.gold.fact_loan_daily
WHERE ltv_pct IS NOT NULL
GROUP BY 1 ORDER BY 1;


SELECT s.amc, sum(h.collateral_value) AS exposure
FROM iceberg.gold.fact_holding_daily h
JOIN iceberg.gold.dim_scheme s ON h.scheme_code = s.scheme_code
GROUP BY s.amc ORDER BY exposure DESC LIMIT 10;


SELECT segment, count(*) AS customers, sum(total_outstanding) AS outstanding
FROM iceberg.gold.customer_360
GROUP BY segment ORDER BY customers DESC;


-- SELECT * FROM iceberg.gold."fact_loan_daily$snapshots" ORDER BY committed_at DESC;
