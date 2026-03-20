USE OlistDWH;

-- ============================================================
-- EDA: raw.customers
-- ============================================================

-- 0. Data Preview
-- ============================================================
SELECT TOP 10 *
FROM raw.customers;


-- 1. Row Count
-- ============================================================
SELECT COUNT(*) AS total_rows
FROM raw.customers;


-- 2. Null Analysis
-- ============================================================
SELECT
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)             AS null_customer_id,
    SUM(CASE WHEN customer_unique_id IS NULL THEN 1 ELSE 0 END)      AS null_customer_unique_id,
    SUM(CASE WHEN customer_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_zip_code_prefix,
    SUM(CASE WHEN customer_city IS NULL THEN 1 ELSE 0 END)           AS null_customer_city,
    SUM(CASE WHEN customer_state IS NULL THEN 1 ELSE 0 END)          AS null_customer_state
FROM raw.customers;


-- 3. Duplicate customer_id
-- ============================================================
SELECT customer_id, COUNT(*) AS cnt
FROM raw.customers
GROUP BY customer_id
HAVING COUNT(*) > 1;


-- 4. Duplicate customer_unique_id
-- ============================================================
SELECT customer_unique_id, COUNT(*) AS cnt
FROM raw.customers
GROUP BY customer_unique_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;


-- 5. Difference: customer_id vs customer_unique_id
-- ============================================================
SELECT
    COUNT(*)                            AS total_rows,
    COUNT(DISTINCT customer_id)         AS distinct_customer_id,
    COUNT(DISTINCT customer_unique_id)  AS distinct_customer_unique_id,
    COUNT(*) - COUNT(DISTINCT customer_unique_id) AS returning_customers
FROM raw.customers;


-- 6. Distribution by State
-- ============================================================
SELECT
    customer_state,
    COUNT(*)                                                    AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)          AS pct
FROM raw.customers
GROUP BY customer_state
ORDER BY cnt DESC;


-- 7. Top 10 Cities by Customer Count
-- ============================================================
SELECT TOP 10
    customer_city,
    customer_state,
    COUNT(*) AS cnt
FROM raw.customers
GROUP BY customer_city, customer_state
ORDER BY cnt DESC;


SELECT TOP 10
    customer_city,
    COUNT(*) AS cnt
FROM raw.customers
GROUP BY customer_city
ORDER BY cnt DESC;


-- 8. Outliers: Cities with Inconsistent State Assignment
-- ============================================================
SELECT
    customer_city,
    COUNT(DISTINCT customer_state)  AS distinct_states,
    STRING_AGG(customer_state, ', ') WITHIN GROUP (ORDER BY customer_state) AS states
FROM (
    SELECT DISTINCT customer_city, customer_state
    FROM raw.customers
) AS deduped
GROUP BY customer_city
HAVING COUNT(DISTINCT customer_state) > 1
ORDER BY distinct_states DESC;
