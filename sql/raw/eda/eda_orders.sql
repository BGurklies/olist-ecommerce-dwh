USE OlistDWH;

-- ============================================================
-- EDA: raw.orders
-- ============================================================

-- 0. Data Preview
-- ============================================================
SELECT TOP 10 *
FROM raw.orders;


-- 1. Row Count
-- ============================================================
SELECT COUNT(*) AS total_rows
FROM raw.orders;


-- 2. Min/Max Character Length per Column
-- ============================================================
SELECT column_name, MIN(length) AS min_length, MAX(length) AS max_length
FROM (
    SELECT 'order_id'                      AS column_name, LEN(order_id)                      AS length FROM raw.orders
    UNION ALL
    SELECT 'customer_id'                   AS column_name, LEN(customer_id)                   AS length FROM raw.orders
    UNION ALL
    SELECT 'order_status'                  AS column_name, LEN(order_status)                  AS length FROM raw.orders
    UNION ALL
    SELECT 'order_purchase_timestamp'      AS column_name, LEN(order_purchase_timestamp)      AS length FROM raw.orders
    UNION ALL
    SELECT 'order_approved_at'             AS column_name, LEN(order_approved_at)             AS length FROM raw.orders
    UNION ALL
    SELECT 'order_delivered_carrier_date'  AS column_name, LEN(order_delivered_carrier_date)  AS length FROM raw.orders
    UNION ALL
    SELECT 'order_delivered_customer_date' AS column_name, LEN(order_delivered_customer_date) AS length FROM raw.orders
    UNION ALL
    SELECT 'order_estimated_delivery_date' AS column_name, LEN(order_estimated_delivery_date) AS length FROM raw.orders
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 3. Min/Max Character Length per Column (quotes cleansed)
-- ============================================================
SELECT column_name, MIN(length) AS min_length, MAX(length) AS max_length
FROM (
    SELECT 'order_id'                      AS column_name, LEN(TRIM(REPLACE(order_id,                      '"', ''))) AS length FROM raw.orders
    UNION ALL
    SELECT 'customer_id'                   AS column_name, LEN(TRIM(REPLACE(customer_id,                   '"', ''))) AS length FROM raw.orders
    UNION ALL
    SELECT 'order_status'                  AS column_name, LEN(TRIM(order_status))                                    AS length FROM raw.orders
    UNION ALL
    SELECT 'order_purchase_timestamp'      AS column_name, LEN(TRIM(order_purchase_timestamp))                        AS length FROM raw.orders
    UNION ALL
    SELECT 'order_approved_at'             AS column_name, LEN(TRIM(order_approved_at))                               AS length FROM raw.orders
    UNION ALL
    SELECT 'order_delivered_carrier_date'  AS column_name, LEN(TRIM(order_delivered_carrier_date))                    AS length FROM raw.orders
    UNION ALL
    SELECT 'order_delivered_customer_date' AS column_name, LEN(TRIM(order_delivered_customer_date))                   AS length FROM raw.orders
    UNION ALL
    SELECT 'order_estimated_delivery_date' AS column_name, LEN(TRIM(order_estimated_delivery_date))                   AS length FROM raw.orders
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 4. Null Analysis
-- ============================================================
SELECT
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)                      AS null_order_id,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)                   AS null_customer_id,
    SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END)                  AS null_order_status,
    SUM(CASE WHEN order_purchase_timestamp IS NULL THEN 1 ELSE 0 END)      AS null_purchase_timestamp,
    SUM(CASE WHEN order_approved_at IS NULL THEN 1 ELSE 0 END)             AS null_approved_at,
    SUM(CASE WHEN order_delivered_carrier_date IS NULL THEN 1 ELSE 0 END)  AS null_delivered_carrier_date,
    SUM(CASE WHEN order_delivered_customer_date IS NULL THEN 1 ELSE 0 END) AS null_delivered_customer_date,
    SUM(CASE WHEN order_estimated_delivery_date IS NULL THEN 1 ELSE 0 END) AS null_estimated_delivery_date
FROM raw.orders;


-- 5. Duplicate order_id
-- ============================================================
SELECT order_id, COUNT(*) AS cnt
FROM raw.orders
GROUP BY order_id
HAVING COUNT(*) > 1;


-- 6. Order Status Distribution
-- ============================================================
SELECT
    order_status,
    COUNT(*)                                    AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM raw.orders
GROUP BY order_status
ORDER BY cnt DESC;


-- 7. Order Date Range
-- ============================================================
SELECT
    MIN(order_purchase_timestamp)       AS earliest_order,
    MAX(order_purchase_timestamp)       AS latest_order,
    DATEDIFF(
        DAY,
        MIN(order_purchase_timestamp),
        MAX(order_purchase_timestamp)
    )                                   AS date_range_days
FROM raw.orders;


-- 8. Null Dates by Order Status
-- ============================================================
SELECT
    order_status,
    COUNT(*) AS total,
    SUM(CASE WHEN order_approved_at IS NULL THEN 1 ELSE 0 END) AS null_approved_at,
    SUM(CASE WHEN order_delivered_carrier_date IS NULL THEN 1 ELSE 0 END) AS null_delivered_carrier_date,
    SUM(CASE WHEN order_delivered_customer_date IS NULL THEN 1 ELSE 0 END) AS null_delivered_customer_date
FROM raw.orders
GROUP BY order_status
ORDER BY total DESC;


-- 9. Delivery Time Analysis (delivered orders only)
-- ============================================================
SELECT
    MIN(DATEDIFF(DAY, order_purchase_timestamp, order_delivered_customer_date)) AS min_delivery_days,
    MAX(DATEDIFF(DAY, order_purchase_timestamp, order_delivered_customer_date)) AS max_delivery_days,
    AVG(DATEDIFF(DAY, order_purchase_timestamp, order_delivered_customer_date)) AS avg_delivery_days
FROM raw.orders
WHERE order_status = 'delivered'
AND order_delivered_customer_date IS NOT NULL;


-- 10. Late Deliveries (delivered after estimated date)
-- ============================================================
SELECT
    COUNT(*) AS late_deliveries,
    ROUND(COUNT(*) * 100.0 / (
        SELECT COUNT(*) FROM raw.orders WHERE order_status = 'delivered'
    ), 2) AS pct_late
FROM raw.orders
WHERE order_status = 'delivered'
AND order_delivered_customer_date > order_estimated_delivery_date;


SELECT * FROM raw.orders WHERE order_status = 'delivered' AND order_delivered_customer_date IS NULL;

SELECT * FROM raw.orders WHERE order_status != 'delivered' AND order_delivered_customer_date IS NOT NULL;


-- 11. Outliers: Suspicious Delivery Times
-- ============================================================
SELECT
    order_id,
    order_purchase_timestamp,
    order_delivered_customer_date,
    DATEDIFF(DAY, order_purchase_timestamp, order_delivered_customer_date) AS delivery_days
FROM raw.orders
WHERE DATEDIFF(DAY, order_purchase_timestamp, order_delivered_customer_date) > 60
ORDER BY delivery_days DESC;


-- 12. Orders per Month
-- ============================================================
SELECT
    SUBSTRING(order_purchase_timestamp, 1, 7) AS year_month,
    COUNT(*)                                   AS total_orders
FROM raw.orders
GROUP BY SUBSTRING(order_purchase_timestamp, 1, 7)
ORDER BY year_month;
