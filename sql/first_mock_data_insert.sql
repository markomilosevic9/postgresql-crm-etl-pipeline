-- 1st/initial mock data sample


-- mock data insertion into raw.raw_plans
-- 8 records
-- 4 records with predefined DQ issues
BEGIN;
INSERT INTO raw.raw_plans (plan_name, plan_type, billing_cycle_months, category, base_price)
VALUES ('Basic Monthly', 'Monthly', 1, 'Starter', 52000),
       ('Standard Monthly', 'Monthly', 1, 'Professional', 88000),
       ('Premium Monthly', 'Monthly', 1, 'Enterprise', 95000),
       ('Basic Annual', 'Annual', 12, 'Starter', 140000),
       ('Standard Annual', 'Annual', 12, 'Professional', 150000),
       ('Premium Annual', 'Annual', 12, 'Enterprise', 160000),
       -- predefined subset of data with DQ issues
       (NULL, 'Monthly', 1, 'Starter', 45000), -- predefined DQ issue - NULL plan_name
       ('Trial Plan', NULL, NULL, 'Gold', -5000) -- predefined DQ issues - NULL plan_type, NULL billing_cycle_months, INVALID_CATEGORY category, NEGATIVE_NUMBER base_price
;
COMMIT;


-- mock data insertion into raw.raw_customers
-- 15 records
-- 2 records with predefined DQ issues
BEGIN;
INSERT INTO raw.raw_customers (company_name, country, industry, signup_date)
VALUES ('Company One', 'Country A', 'Technology', '2025-01-15'),
       ('Company Two', 'Country B', 'Healthcare', '2025-01-20'),
       ('Company Three', 'Country C', 'Finance', '2025-01-25'),
       ('Company Four', 'Country A', 'Technology', '2025-02-01'),
       ('Company Five', 'Country A', 'Retail', '2025-02-05'),
       ('Company Six', 'Country C', 'Retail', '2025-02-10'),
       ('Company Seven', 'Country B', 'Healthcare', '2025-03-01'),
       ('Company Eight', 'Country A', 'Technology', '2025-03-15'),
       ('Company Nine', 'Country C', 'Finance', '2025-04-01'),
       ('Company Ten', 'Country A', 'Retail', '2025-04-15'),
       ('Company Eleven', 'Country A', 'Technology', '2025-05-01'),
       ('Company Twelve', 'Country B', 'Healthcare', '2025-05-15'),
       -- predefined subset of data with DQ issues
       ('Company Thirteen', NULL, 'Manufacturing', '2025-06-01'), -- predefined DQ issue - NULL country
       (NULL, 'Country C', 'Technology', '2025-06-10'), -- predefined DQ issue - NULL company_name
       ('Company Fifteen', 'Country A', 'Retail', '2025-06-15')
;
COMMIT;


-- mock data insertion into raw.raw_subscriptions
-- 20 records
-- 5 records with predefined DQ issues
BEGIN;
INSERT INTO raw.raw_subscriptions (customer_id, plan_id, start_date, end_date, amount)
VALUES (1, 3, '2025-01-15', '2025-02-28', 95000),
       (2, 1, '2025-01-20', '2025-03-05', 55000),
       (3, 5, '2025-02-01', '2025-04-10', 150000),
       (5, 1, '2025-02-15', '2025-03-20', 52000),
       (7, 2, '2025-03-05', '2025-04-15', 88000),
       (9, 4, '2025-04-10', '2025-06-05', 145000),
       (11, 3, '2025-05-05', '2025-06-01', 93000),
       (4, 2, '2025-02-10', '2025-03-10', 87000),
       (6, 1, '2025-03-15', '2025-04-20', 53000),
       (8, 4, '2025-04-05', '2025-05-15', 140000),
       (10, 1, '2025-05-10', '2025-06-15', 51000),
       (12, 3, '2025-06-01', NULL, 94000),
       (15, 1, '2025-06-15', NULL, 57000),
       (1, 6, '2025-06-20', NULL, 160000),
       (2, 2, '2025-06-15', NULL, 87000),
       -- predefined subset of data containing problematic records with DQ issues
       (4, 9999, '2025-02-10', '2025-03-15', 78000), -- predefined DQ issue - MISSING_FK plan_id, non-existent plan
       (6, 1, '2025-04-01', '2025-03-15', 82000), -- predefined DQ issue - DATE_LOGIC_ERROR end_date before start_date
       (9999, 2, '2025-03-20', '2025-04-25', 55000), -- predefined DQ issue - MISSING_FK customer_id, non-existent ID
       (8, NULL, '2025-05-01', '2025-06-10', 60000), -- predefined DQ issue - NULL_VALUE plan_id
       (10, 1, NULL, '2025-06-20', 52000) -- predefined DQ issue - NULL_VALUE start_date
;
COMMIT;


-- mock data insertion into raw.raw_transactions
-- 19 records
-- 5 records with predefined DQ issues
-- amount reflects the per-transaction payment amount; 0.00 for Failed transactions
BEGIN;
INSERT INTO raw.raw_transactions (sub_id, tx_date, status, amount)
VALUES (1, '2025-01-25', 'Success', 95000.00),
       (1, '2025-02-15', 'Success', 95000.00),
       (2, '2025-02-05', 'Success', 55000.00),
       (3, '2025-02-15', 'Success', 150000.00),
       (3, '2025-03-20', 'Success', 150000.00),
       (4, '2025-02-25', 'Success', 52000.00),
       (5, '2025-03-15', 'Success', 88000.00),
       (6, '2025-04-15', 'Success', 53000.00),
       (6, '2025-05-10', 'Success', 53000.00),
       (7, '2025-05-15', 'Success', 93000.00),
       -- predefined subset of data containing failed/refunded transactions
       (8, '2025-02-20', 'Failed', 0.00),
       (9, '2025-03-25', 'Refunded',53000.00),
       (10, '2025-04-20', 'Failed', 0.00),
       (11, '2025-05-20', 'Failed', 0.00),
       -- predefined subset of data containing records with DQ issues
       (1, '2025-01-25', 'Success', 95000.00), -- predefined DQ issue - DUPLICATE
       (2, '2025-01-15', 'Success', 55000.00), -- predefined DQ issue - DATE_LOGIC_ERROR tx_date before start_date
       (5, '2025-03-10', 'Processing', 88000.00), -- predefined DQ issue - INVALID_CATEGORY status
       (NULL, '2025-04-01', 'Success', 50000.00), -- predefined DQ issue - NULL_VALUE, no sub_id
       (9999, '2025-05-15', 'Success', 55000.00)  -- predefined DQ issue - MISSING_FK, non-existent ID
;
COMMIT;
