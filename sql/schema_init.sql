-- DDL for the 5-schema architecture:
-- raw - raw data in "as-is" format, from source CRM, no constraints enforced
-- clean - cleaned, deduplicated, surrogate keys introduced
-- dimensional - Kimball star schema (SCD1/SCD2 dimensions, fact tables)
-- mart - aggregated materialized view for analytics/reporting
-- etl - pipeline control (run tracking + DQ logging)


-- SCHEMAS
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS clean;
CREATE SCHEMA IF NOT EXISTS dimensional;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS etl;

-- RAW SCHEMA
-- stores data exactly as received from the source/CRM
-- no business rules or FK constraints enforced at this layer
-- allows NULLs, duplicates, and any other DQ issues

-- 1)
-- DDL for table raw.raw_customers
-- natural/composite key for later deduplication: company_name + country + signup_date
-- source_updated_at captures the business-effective source change timestamp for SCD2 version boundaries; not used as a processing watermark
-- ingested_at records when the row entered the DWH; used as the incremental watermark
CREATE TABLE IF NOT EXISTS raw.raw_customers (customer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                              company_name VARCHAR(100),
                                              country VARCHAR(100),
                                              industry VARCHAR(100),
                                              signup_date DATE,
                                              source_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                              ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)
;


-- 2)
-- DDL for table raw.raw_plans
-- natural/composite key for later deduplication: plan_name + plan_type
-- plan_id references in raw_subscriptions are logical only - no FK enforced at this layer
-- source_updated_at captures the business-effective source change timestamp for traceability; not used as a processing watermark
-- ingested_at records when the row entered the DWH; used as the incremental watermark
CREATE TABLE IF NOT EXISTS raw.raw_plans (plan_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                          plan_name VARCHAR(100),
                                          plan_type VARCHAR(50),
                                          billing_cycle_months INT,
                                          category VARCHAR(50),
                                          base_price NUMERIC(12,2),
                                          source_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                          ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)
;


-- 3)
-- DDL for table raw.raw_subscriptions
-- natural/composite key for later deduplication: customer_id + plan_id + start_date
-- customer_id is a logical reference to raw_customers, no FK enforced
-- source_updated_at captures the business-effective source change timestamp for subscription changes; not used as a processing watermark
-- ingested_at records when the row entered the DWH; used as the incremental watermark
CREATE TABLE IF NOT EXISTS raw.raw_subscriptions (sub_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                                  customer_id BIGINT,
                                                  plan_id BIGINT,
                                                  start_date DATE,
                                                  end_date DATE,
                                                  amount NUMERIC(12,2),
                                                  source_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                                  ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)
;


-- 4)
-- DDL for table raw.raw_transactions
-- natural/composite key for later deduplication: sub_id + tx_date + status
-- sub_id is a logical reference to raw_subscriptions, no FK enforced
-- amount is the per-transaction payment amount; raw allows NULL so missing amounts can be DQ-logged
-- ingested_at records when the row entered the DWH; used as the incremental watermark
CREATE TABLE IF NOT EXISTS raw.raw_transactions (tx_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                                 sub_id BIGINT,
                                                 tx_date DATE,
                                                 status VARCHAR(100),
                                                 amount NUMERIC(10,2),
                                                 ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)
;


-- ETL SCHEMA
-- pipeline control tables - run tracking and DQ logging

-- 1)
-- DDL for table etl.etl_runs
-- stores 1 row per pipeline execution
-- run_id is referenced throughout dq_log to correlate all DQ issues per pipeline run
-- ended_at remains NULL if the pipeline fails before reaching the final completion step
-- status tracks pipeline state: RUNNING while in progress, COMPLETED on success
-- batch_effective_date is just audit metadata only
-- watermark_lower_bound and watermark_upper_bound define the processing window for ingested_at-based incremental loads
-- FAILED status is defined but never set by the pipeline; stale RUNNING rows indicate a failed run
CREATE TABLE IF NOT EXISTS etl.etl_runs (run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                         started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                         ended_at TIMESTAMP,
                                         status VARCHAR(20) NOT NULL DEFAULT 'RUNNING' CHECK (status IN ('RUNNING', 
                                                                                                         'COMPLETED', 
                                                                                                         'FAILED')),
                                         batch_effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
                                         watermark_lower_bound TIMESTAMP,
                                         watermark_upper_bound TIMESTAMP)
;


-- 2)
-- DDL for table etl.dq_log
-- persistent log of DQ issues found during pipeline runs
-- run_id records the pipeline run that first detected the issue
-- last_seen_run_id tracks which run most recently re-detected the issue
-- ON CONFLICT DO UPDATE in the pipeline refreshes last_seen_run_id each run, preserving run_id (first detection) for root-cause analysis
-- detail provides column-level context (e.g. which column was NULL)
-- detail_key discriminates within an issue type when a single record can have multiple failures of the same type (e.g. MISSING_FK on both customer_id and plan_id for the same subscription)
-- empty string '' is the default for issue types where at most one failure per record is possible
-- covers 9 (predefined) issue types
CREATE TABLE IF NOT EXISTS etl.dq_log (log_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                       run_id BIGINT NOT NULL REFERENCES etl.etl_runs(run_id),
                                       last_seen_run_id BIGINT NOT NULL REFERENCES etl.etl_runs(run_id),
                                       table_name VARCHAR(100) NOT NULL,
                                       record_id BIGINT NOT NULL,
                                       issue_type VARCHAR(50) NOT NULL CHECK (issue_type IN ('NULL_VALUE',
                                                                                             'INVALID_CATEGORY',
                                                                                             'DATE_LOGIC_ERROR_DATES',
                                                                                             'DATE_LOGIC_ERROR_SIGNUP',
                                                                                             'DATE_OUT_OF_RANGE',
                                                                                             'NEGATIVE_NUMBER',
                                                                                             'DUPLICATE',
                                                                                             'DERIVED_VALUE_MISMATCH',
                                                                                             'MISSING_FK')),
                                       detail_key VARCHAR(100) NOT NULL DEFAULT '',
                                       detail TEXT,
                                       detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                       CONSTRAINT unique_dq_log_record_issue UNIQUE (table_name, record_id, issue_type, detail_key))
;


-- CLEAN SCHEMA
-- cleaned, deduplicated, normalized data
-- surrogate keys are introduced at this layer as the primary keys
-- raw PKs are stored as source_*_id (UNIQUE) 
-- FK constraints are enforced using surrogate keys
-- surrogate FK resolution during upsert: raw FK values (e.g. raw_subscriptions.customer_id) are resolved to clean surrogate PKs by JOINs on source_*_id.

-- 1)
-- DDL for table clean.clean_customers
-- upsert key: ON CONFLICT (source_customer_id)
-- source_updated_at propagated from raw.raw_customers; used by dim_customer SCD2 to derive version boundaries
CREATE TABLE IF NOT EXISTS clean.clean_customers (customer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                                  source_customer_id BIGINT NOT NULL UNIQUE,
                                                  company_name VARCHAR(100) NOT NULL,
                                                  country VARCHAR(100) NOT NULL,
                                                  industry VARCHAR(100) NOT NULL,
                                                  signup_date DATE NOT NULL,
                                                  source_updated_at TIMESTAMP,
                                                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)
;


-- 2)
-- DDL for table clean.clean_plans
-- plan_type restricted to allowed categories at this layer
-- billing_cycle_months and base_price must be positive
-- UPPER(plan_name) is used only for deduplication; stored plan_name preserves source casing
-- source_updated_at propagated from raw.raw_plans for traceability
-- upsert key: ON CONFLICT (source_plan_id)
CREATE TABLE IF NOT EXISTS clean.clean_plans (plan_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                              source_plan_id BIGINT NOT NULL UNIQUE,
                                              plan_name VARCHAR(100) NOT NULL,
                                              plan_type VARCHAR(50) NOT NULL CHECK (plan_type IN ('Monthly', 
                                                                                                  'Annual')),
                                              billing_cycle_months INT NOT NULL CHECK (billing_cycle_months > 0),
                                              category VARCHAR(50) NOT NULL CHECK (category IN ('Starter', 
                                                                                                'Professional', 
                                                                                                'Enterprise')),
                                              base_price NUMERIC(12,2) NOT NULL CHECK (base_price > 0),
                                              source_updated_at TIMESTAMP,
                                              created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                              updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)
;


-- 3)
-- DDL for table clean.clean_subscriptions
-- customer_id and plan_id are surrogate FKs resolved from raw values during upsert
-- end_date is nullable: NULL means the subscription is still active
-- source_updated_at propagated from raw.raw_subscriptions for traceability
-- upsert key: ON CONFLICT (source_sub_id)
CREATE TABLE IF NOT EXISTS clean.clean_subscriptions (sub_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                                      source_sub_id BIGINT NOT NULL UNIQUE,
                                                      customer_id BIGINT NOT NULL REFERENCES clean.clean_customers(customer_id),
                                                      plan_id BIGINT NOT NULL REFERENCES clean.clean_plans(plan_id),
                                                      start_date DATE NOT NULL,
                                                      end_date DATE,
                                                      amount NUMERIC(12,2) NOT NULL,
                                                      source_updated_at TIMESTAMP,
                                                      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                                      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                                      CONSTRAINT check_clean_end_date_after_start_date CHECK (end_date IS NULL OR end_date >= start_date))
;


-- 4)
-- DDL for table clean.clean_transactions
-- status restricted to allowed categories at this layer
-- sub_id is surrogate FK resolved from raw values during upsert
-- amount propagated from raw.raw_transactions; used to compute sum_successful_payments in the fact
-- upsert key: ON CONFLICT (source_tx_id)
CREATE TABLE IF NOT EXISTS clean.clean_transactions (tx_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                                     source_tx_id BIGINT NOT NULL UNIQUE,
                                                     sub_id BIGINT NOT NULL REFERENCES clean.clean_subscriptions(sub_id),
                                                     tx_date DATE NOT NULL,
                                                     status VARCHAR(100) NOT NULL CHECK (status IN ('Success', 
                                                                                                    'Failed', 
                                                                                                    'Refunded')),
                                                     amount NUMERIC(10,2) NOT NULL,
                                                     created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                                     updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)
;


-- DIMENSIONAL SCHEMA
-- Kimball star schema - (conformed) dimensions and fact tables
-- Dimensions keep source_*_id for traceability

-- 1)
-- DDL for table dimensional.dim_date
-- generated calendar dimension covering 2020-2030
-- populated via generate_series in the ETL pipeline
-- date_key is an integer in YYYYMMDD format (e.g. 20250115) for efficient JOINs
-- week_of_year uses week numbering (PostgreSQL EXTRACT(WEEK FROM ...))
CREATE TABLE IF NOT EXISTS dimensional.dim_date (date_key INT PRIMARY KEY,
                                                 full_date DATE NOT NULL UNIQUE,
                                                 year INT NOT NULL,
                                                 quarter INT NOT NULL,
                                                 month INT NOT NULL,
                                                 month_name VARCHAR(20) NOT NULL,
                                                 day_of_month INT NOT NULL,
                                                 day_of_week INT NOT NULL,
                                                 day_name VARCHAR(20) NOT NULL,
                                                 is_weekend BOOLEAN NOT NULL,
                                                 week_of_year INT NOT NULL)
;


-- 2)
-- DDL for table dimensional.dim_customer (SCD type 2)
-- tracks historical changes to company_name, country, and industry
-- source_customer_id is NOT unique, since SCD2 produces multiple rows per business entity
-- valid_from/valid_to define the period during which the version was current
-- valid_from for first versions - signup_date (the customer existed from that business date)
-- valid_from for changed versions - source_updated_at::DATE (the business-effective change date)
-- valid_to = '9999-12-31' indicates the current active version; default date
-- is_current = TRUE flags the active version
-- source_updated_at stores the business timestamp that opened this specific version
-- created_at/updated_at provide system audit timestamps independent of SCD2 business dates
-- fact loads resolve source_customer_id + event date range, then store the resolved customer_id
CREATE TABLE IF NOT EXISTS dimensional.dim_customer (customer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                                     source_customer_id BIGINT NOT NULL,
                                                     company_name VARCHAR(100) NOT NULL,
                                                     country VARCHAR(100) NOT NULL,
                                                     industry VARCHAR(100) NOT NULL,
                                                     signup_date DATE NOT NULL,
                                                     source_updated_at TIMESTAMP,
                                                     valid_from DATE NOT NULL,
                                                     valid_to DATE NOT NULL DEFAULT '9999-12-31',
                                                     is_current BOOLEAN NOT NULL DEFAULT TRUE,
                                                     created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                                     updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                                     CONSTRAINT check_dim_customer_valid_range CHECK (valid_from <= valid_to))
;


-- 3)
-- DDL for table dimensional.dim_plan - SCD type 1
-- source_plan_id is UNIQUE - SCD1 maintains exactly one row per business entity
-- plan_name casing is preserved from clean.clean_plans; only deduplication normalizes with UPPER
-- created_at records the first time the row was inserted by the pipeline
-- upsert key: ON CONFLICT (source_plan_id) DO UPDATE
CREATE TABLE IF NOT EXISTS dimensional.dim_plan (plan_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                                 source_plan_id BIGINT NOT NULL UNIQUE,
                                                 plan_name VARCHAR(100) NOT NULL,
                                                 plan_type VARCHAR(50) NOT NULL CHECK (plan_type IN ('Monthly', 
                                                                                                     'Annual')),
                                                 billing_cycle_months INT NOT NULL CHECK (billing_cycle_months > 0),
                                                 category VARCHAR(50) NOT NULL CHECK (category IN ('Starter', 
                                                                                                   'Professional', 
                                                                                                   'Enterprise')),
                                                 base_price NUMERIC(12,2) NOT NULL CHECK (base_price > 0),
                                                 created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                                 updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)
;


-- 4)
-- DDL for table dimensional.fact_subscription
-- one row per subscription instance
-- sub_id is the raw business key used as PK (degenerate dimension)
-- customer_id FK points to the dim_customer version current at subscription start_date (SCD2)
-- plan_id FK points to dim_plan (SCD1; always current - updated)
-- start_date_key and end_date_key reference dim_date; end_date_key is NULL for active subscriptions
-- subscription_duration_days is NULL for active subscriptions
-- is_active and subscription_duration_days are refreshed on every conflict update; DQ verification checks is_active against end_date_key
-- transaction aggregate measures computed from clean.clean_transactions during ETL load; refreshed each run
-- sum_successful_payments sums actual successful transaction amounts
CREATE TABLE IF NOT EXISTS dimensional.fact_subscription (sub_id BIGINT PRIMARY KEY,
                                                          customer_id BIGINT NOT NULL REFERENCES dimensional.dim_customer(customer_id),
                                                          plan_id BIGINT NOT NULL REFERENCES dimensional.dim_plan(plan_id),
                                                          start_date_key INT NOT NULL REFERENCES dimensional.dim_date(date_key),
                                                          end_date_key INT REFERENCES dimensional.dim_date(date_key),
                                                          amount NUMERIC(12,2) NOT NULL,
                                                          subscription_duration_days INT,
                                                          is_active BOOLEAN NOT NULL,
                                                          total_successful_payments INT NOT NULL DEFAULT 0,
                                                          total_failed_payments INT NOT NULL DEFAULT 0,
                                                          total_refunded_payments INT NOT NULL DEFAULT 0,
                                                          sum_successful_payments NUMERIC(12,2) NOT NULL DEFAULT 0)
;


-- 5)
-- DDL for table dimensional.fact_transaction
-- grain: one row per accepted transaction
-- tx_id is the raw business key used as PK (degenerate dimension)
-- sub_id is the raw subscription key linking back to fact_subscription
-- customer_id and plan_id inherited from fact_subscription at load time (reuses SCD2 pin)
-- date_key references the transaction date in dim_date
-- boolean status flags replace the status string for analytical convenience
CREATE TABLE IF NOT EXISTS dimensional.fact_transaction (tx_id BIGINT PRIMARY KEY,
                                                         sub_id BIGINT NOT NULL,
                                                         customer_id BIGINT NOT NULL REFERENCES dimensional.dim_customer(customer_id),
                                                         plan_id BIGINT NOT NULL REFERENCES dimensional.dim_plan(plan_id),
                                                         date_key INT NOT NULL REFERENCES dimensional.dim_date(date_key),
                                                         amount NUMERIC(10,2) NOT NULL,
                                                         is_success BOOLEAN NOT NULL,
                                                         is_failed BOOLEAN NOT NULL,
                                                         is_refunded BOOLEAN NOT NULL,
                                                         created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                                         updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)
;


-- MART SCHEMA

-- mart.dm_sales_performance
-- materialized view for analytics and reporting
-- reads from the dimensional layer
-- refreshed on each pipeline run
-- expands each subscription across its active months using generate_series
-- active subscriptions (NULL end_date) are capped at the latest date in the dataset
-- dim_customer joined on customer_id (SCD2-pinned): country reflects the customer's location when the subscription started
-- active_subscriptions: count of subscriptions active during the calendar month
-- new_subscriptions: count of subscriptions whose start_date falls in the calendar month
-- churned_subscriptions: count of subscriptions whose end_date falls in the calendar month
-- total_contract_value: sum of subscription amounts for active subscriptions in the month
-- total_collected: sum of successful payment amounts where transaction date falls in the month
DROP MATERIALIZED VIEW IF EXISTS mart.dm_sales_performance;

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.dm_sales_performance AS
WITH dataset_cap AS (
    SELECT MAX(COALESCE(dd_end.full_date, dd_start.full_date)) AS cap_date
    FROM dimensional.fact_subscription AS fs
    INNER JOIN dimensional.dim_date AS dd_start ON dd_start.date_key = fs.start_date_key
    LEFT JOIN dimensional.dim_date AS dd_end ON dd_end.date_key = fs.end_date_key),
subscription_months AS (
    SELECT fs.sub_id,
           dp.category AS plan_category,
           dc.country,
           fs.amount,
           dd_start.full_date AS start_date,
           dd_end.full_date AS end_date,
           generate_series(
               DATE_TRUNC('month', dd_start.full_date),
               DATE_TRUNC('month', COALESCE(dd_end.full_date, cap.cap_date)),
               INTERVAL '1 month') AS calendar_month
    FROM dimensional.fact_subscription AS fs
    INNER JOIN dimensional.dim_plan AS dp
        ON dp.plan_id = fs.plan_id
    INNER JOIN dimensional.dim_customer AS dc
        ON dc.customer_id = fs.customer_id
    INNER JOIN dimensional.dim_date AS dd_start
        ON dd_start.date_key = fs.start_date_key
    LEFT JOIN dimensional.dim_date AS dd_end
        ON dd_end.date_key = fs.end_date_key
    CROSS JOIN dataset_cap AS cap),
payment_months AS (
    SELECT ft.sub_id,
           DATE_TRUNC('month', dd_tx.full_date) AS payment_month,
           SUM(ft.amount) AS monthly_payments
    FROM dimensional.fact_transaction AS ft
    INNER JOIN dimensional.dim_date AS dd_tx
        ON dd_tx.date_key = ft.date_key
    WHERE ft.is_success = TRUE
    GROUP BY ft.sub_id,
             DATE_TRUNC('month', dd_tx.full_date))
SELECT TO_CHAR(sm.calendar_month, 'YYYY-MM') AS calendar_month,
       sm.plan_category,
       sm.country,
       COUNT(*) AS active_subscriptions,
       COUNT(*) FILTER (WHERE DATE_TRUNC('month', sm.start_date) = sm.calendar_month) AS new_subscriptions,
       COUNT(*) FILTER (WHERE sm.end_date IS NOT NULL
                        AND DATE_TRUNC('month', sm.end_date) = sm.calendar_month) AS churned_subscriptions,
       COALESCE(SUM(sm.amount), 0) AS total_contract_value,
       COALESCE(SUM(pm.monthly_payments), 0) AS total_collected
FROM subscription_months AS sm
LEFT JOIN payment_months AS pm
    ON pm.sub_id = sm.sub_id AND pm.payment_month = sm.calendar_month
GROUP BY sm.calendar_month, sm.plan_category, sm.country
WITH NO DATA;
