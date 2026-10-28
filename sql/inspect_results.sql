-- Replace workspace with the catalog printed by notebook cell 2 if different.
SELECT * FROM workspace.financial_lakehouse.silver_payments ORDER BY payment_id;
SELECT * FROM workspace.financial_lakehouse.gold_daily_payments ORDER BY business_date, merchant_id;
SELECT rejection_reason, COUNT(*) AS records
FROM workspace.financial_lakehouse.quarantine_events GROUP BY rejection_reason;
