-- Change `workspace` below if your current Databricks catalog is different.
SELECT * FROM workspace.financial_lakehouse.silver_file_payments ORDER BY payment_id;
SELECT * FROM workspace.financial_lakehouse.quarantine_file_events ORDER BY event_id;
SELECT business_date, merchant_id, settled_total_inr, settled_count, failed_count
FROM workspace.financial_lakehouse.gold_file_daily_payments
ORDER BY business_date, merchant_id;
SELECT SUM(settled_total_inr) AS settled_total_inr
FROM workspace.financial_lakehouse.gold_file_daily_payments;
