-- Serving layer for sqldb-banking-serving (Azure SQL Database, free offer).
-- Gold tables are loaded by the ADF pipeline pl_load_gold_to_sql from Parquet files.
-- Run each CREATE statement as its own batch (CREATE SCHEMA / CREATE VIEW must be first).
-- Reconstructed from the statements actually run; tables first, then indexes, then views.

CREATE SCHEMA gold;
GO

CREATE TABLE gold.dim_date (
  date_key int NOT NULL PRIMARY KEY, [date] date NOT NULL, [year] int, [month] int,
  month_name nvarchar(10), [day] int, day_name nvarchar(20), is_weekend bit);

CREATE TABLE gold.dim_merchant (
  merchant_id nvarchar(30) NOT NULL PRIMARY KEY, name nvarchar(200), mcc nvarchar(20),
  category nvarchar(100), risk_tier nvarchar(30), emirate nvarchar(100), city nvarchar(100));

CREATE TABLE gold.dim_account (
  account_id nvarchar(30) NOT NULL PRIMARY KEY, customer_id nvarchar(30), account_type nvarchar(50),
  currency nvarchar(10), open_date date, status nvarchar(30), branch_code nvarchar(20));

CREATE TABLE gold.dim_customer (
  customer_sk int NOT NULL PRIMARY KEY, customer_id nvarchar(30), first_name nvarchar(100),
  last_name nvarchar(100), nationality nvarchar(50), emirate nvarchar(100), segment nvarchar(30),
  kyc_status nvarchar(30), onboarding_date date, valid_from date, valid_to date, is_current bit);

CREATE TABLE gold.fact_transactions (
  transaction_id nvarchar(30) NOT NULL PRIMARY KEY, date_key int, txn_ts datetime2,
  customer_sk int NOT NULL, account_id nvarchar(30), merchant_id nvarchar(30),
  txn_type nvarchar(50), channel nvarchar(50), status nvarchar(50), decline_reason nvarchar(100),
  fraud_flag int, country nvarchar(50), currency nvarchar(10), amount decimal(18,2),
  fx_rate_to_aed decimal(18,6), dq_missing_amount bit, dq_negative_purchase bit,
  dq_orphan_account bit, dq_orphan_merchant bit, dq_bad_timestamp bit,
  amount_aed decimal(18,2), is_valid bit);
GO

CREATE NONCLUSTERED INDEX ix_fact_date
  ON gold.fact_transactions (date_key)
  INCLUDE (amount_aed, status, txn_type, fraud_flag, is_valid);
CREATE NONCLUSTERED INDEX ix_fact_merchant ON gold.fact_transactions (merchant_id);
CREATE NONCLUSTERED INDEX ix_fact_customer ON gold.fact_transactions (customer_sk);
CREATE NONCLUSTERED INDEX ix_fact_account  ON gold.fact_transactions (account_id);
GO

-- Reporting view 1: flat, valid rows only (is_valid = 1)
CREATE OR ALTER VIEW gold.vw_txn_report AS
SELECT f.transaction_id,
       d.[date]  AS txn_date,
       d.[year], d.[month], d.month_name,
       f.txn_type, f.channel, f.status, f.decline_reason, f.fraud_flag,
       f.country, f.currency, f.amount, f.amount_aed,
       c.segment, c.emirate AS customer_emirate, c.nationality,
       m.name AS merchant_name, m.category AS merchant_category,
       m.emirate AS merchant_emirate
FROM gold.fact_transactions f
JOIN gold.dim_date d      ON d.date_key = f.date_key
JOIN gold.dim_customer c  ON c.customer_sk = f.customer_sk
LEFT JOIN gold.dim_merchant m ON m.merchant_id = f.merchant_id
WHERE f.is_valid = 1;
GO

-- Reporting view 2: monthly KPIs
-- purchase spend = successful PURCHASE rows; decline rate = DECLINED / all valid rows;
-- fraud rate = fraud_flag = 1 / all valid rows.
CREATE OR ALTER VIEW gold.vw_monthly_kpi AS
SELECT d.[year], d.[month], d.month_name,
       COUNT(*) AS txn_count,
       SUM(CASE WHEN f.txn_type = 'PURCHASE' AND f.status = 'SUCCESS'
                THEN f.amount_aed ELSE 0 END) AS purchase_spend_aed,
       SUM(CASE WHEN f.txn_type = 'REFUND' AND f.status = 'SUCCESS'
                THEN f.amount_aed ELSE 0 END) AS refund_aed,
       SUM(CASE WHEN f.status = 'DECLINED' THEN 1 ELSE 0 END) AS declined_txns,
       CAST(100.0 * SUM(CASE WHEN f.status = 'DECLINED' THEN 1 ELSE 0 END)
            / COUNT(*) AS decimal(5,2)) AS decline_rate_pct,
       SUM(CASE WHEN f.fraud_flag = 1 THEN 1 ELSE 0 END) AS fraud_txns,
       CAST(100.0 * SUM(CASE WHEN f.fraud_flag = 1 THEN 1 ELSE 0 END)
            / COUNT(*) AS decimal(5,2)) AS fraud_rate_pct
FROM gold.fact_transactions f
JOIN gold.dim_date d ON d.date_key = f.date_key
WHERE f.is_valid = 1
GROUP BY d.[year], d.[month], d.month_name;
GO