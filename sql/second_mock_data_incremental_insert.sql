-- 2nd/incremental mock data sample


-- SCD2-triggering updates on raw.raw_customers
-- modifies tracked attributes (company_name, country, industry) on 3 existing customers
BEGIN;
UPDATE raw.raw_customers
SET industry = 'Healthcare',
    source_updated_at = '2025-07-01 00:00:00',
    ingested_at = CURRENT_TIMESTAMP
WHERE customer_id = 3
; -- customer record edited: industry changed
COMMIT;

BEGIN;
UPDATE raw.raw_customers
SET country = 'Country A',
    source_updated_at = '2025-07-01 00:00:00',
    ingested_at = CURRENT_TIMESTAMP
WHERE customer_id = 7
; -- customer record edited: country changed
COMMIT;

BEGIN;
UPDATE raw.raw_customers
SET company_name = 'Company Eleven Rebranded',
    source_updated_at = '2025-07-01 00:00:00',
    ingested_at = CURRENT_TIMESTAMP
WHERE customer_id = 11
; -- customer record edited: company name changed
COMMIT;


-- SCD1-triggering update on raw.raw_plans
-- modifies base_price 
BEGIN;
UPDATE raw.raw_plans
SET base_price = 92000,
    source_updated_at = '2025-07-01 00:00:00',
    ingested_at = CURRENT_TIMESTAMP
WHERE plan_id = 2
; -- record edited: price changed
COMMIT;


-- new plan records
-- 2 records: 1 clean, 1 with predefined DQ issues
BEGIN;
INSERT INTO raw.raw_plans (plan_name, plan_type, billing_cycle_months, category, base_price)
VALUES ('Premium Plus Annual', 'Annual', 12, 'Enterprise', 180000),
       ('Promo Monthly', 'Monthly', 1, 'Gold', 30000) -- predefined DQ issue - INVALID_CATEGORY 
;
COMMIT;


-- update: 3 active subscriptions from the initial batch are now closed
-- on the next pipeline run, upsert propagates these changes through the pipeline
BEGIN;
UPDATE raw.raw_subscriptions
SET end_date = '2025-07-15',
    source_updated_at = '2025-07-15 00:00:00',
    ingested_at = CURRENT_TIMESTAMP
WHERE sub_id = 12
;
COMMIT;

BEGIN;
UPDATE raw.raw_subscriptions
SET end_date = '2025-07-20',
    source_updated_at = '2025-07-20 00:00:00',
    ingested_at = CURRENT_TIMESTAMP
WHERE sub_id = 13
;
COMMIT;

BEGIN;
UPDATE raw.raw_subscriptions
SET end_date = '2025-08-01',
    source_updated_at = '2025-08-01 00:00:00',
    ingested_at = CURRENT_TIMESTAMP
WHERE sub_id = 14
;
COMMIT;


-- new records/customers added
-- 3 clean records, 2 with predefined DQ issues
BEGIN;
INSERT INTO raw.raw_customers (company_name, country, industry, signup_date)
VALUES ('Company Sixteen', 'Country A', 'Technology', '2025-07-03'),
       ('Company Seventeen', 'Country A', 'Healthcare', '2025-07-08'),
       ('Company Eighteen', 'Country B', 'Finance', '2025-07-15'),
       -- predefined subset of data with DQ issues
       (NULL, 'Country C', 'Retail', '2025-07-10'), -- predefined DQ issue - NULL company_name
       ('Company Twenty', 'Country A', 'Technology', '1990-06-01') -- predefined DQ issue - DATE_OUT_OF_RANGE, signup_date is unreasonable
;
COMMIT;


-- new records/subscriptions added
BEGIN;
INSERT INTO raw.raw_subscriptions (customer_id, plan_id, start_date, end_date, amount)
VALUES (3, 1, '2025-07-05', NULL, 48000), -- new sub for existing customer 
       (16, 2, '2025-07-08', '2025-08-15', 90000),
       (17, 1, '2025-07-10', NULL, 53000),
       (18, 9, '2025-07-15', NULL, 148000),
       -- predefined subset of data containing records with DQ issues
       (3, 9999, '2025-07-20', NULL, 72000), -- predefined DQ issue - MISSING_FK plan_id, non-existent plan
       (16, 2, '2025-08-10', '2025-07-25', 91000), -- predefined DQ issue - DATE_LOGIC_ERROR, end_date before start_date
       (17, 1, '2626-03-01', NULL, 53000), -- predefined DQ issue - DATE_OUT_OF_RANGE for start_date
       (18, 5, '2025-07-20', NULL, -8000) -- predefined DQ issue - NEGATIVE_NUMBER for amount
;
COMMIT;


-- new records/transactions added
BEGIN;
INSERT INTO raw.raw_transactions (sub_id, tx_date, status, amount)
VALUES (12, '2025-07-05', 'Success', 94000.00),
       (13, '2025-07-10', 'Success', 57000.00),
       (14, '2025-07-15', 'Success', 160000.00),
       (14, '2025-07-25', 'Success', 160000.00),
       (22, '2025-07-25', 'Success', 48000.00),
       -- predefined subset of data containing records with DQ issues
       (12, '2025-07-05', 'Success', 94000.00), -- predefined DQ issue - DUPLICATE
       (23, '2025-07-05', 'Success', 90000.00), -- predefined DQ issue - DATE_LOGIC_ERROR, tx_date before start_date
       (24, '2025-07-20', 'Processing', 53000.00), -- predefined DQ issue - INVALID_CATEGORY for status
       (8888, '2025-07-30', 'Success', 50000.00), -- predefined DQ issue - MISSING_FK, non-existent ID
       (22, '2035-07-15', 'Success', 48000.00)  -- predefined DQ issue - DATE_OUT_OF_RANGE for tx_date
;
COMMIT;
