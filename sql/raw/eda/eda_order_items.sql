USE OlistDWH;

-- ============================================================
-- EDA: raw.order_items
-- ============================================================

-- 0. Data Preview
-- ============================================================
SELECT TOP 10 *
FROM raw.order_items


-- 1. Row Count
-- ============================================================
SELECT COUNT(*) AS total_rows
FROM raw.order_items;


-- 2. Min/Max Character Length per Column
-- ============================================================
SELECT column_name, MIN(length) AS min_length, MAX(length) AS max_length
FROM (
    SELECT 'order_id'              AS column_name, LEN(order_id) AS length FROM raw.order_items
    UNION ALL
    SELECT 'order_item_id'       AS column_name, LEN(order_item_id) AS length FROM raw.order_items
    UNION ALL
    SELECT 'product_id' AS column_name, LEN(product_id) AS length FROM raw.order_items
    UNION ALL
    SELECT 'seller_id'            AS column_name, LEN(seller_id) AS length FROM raw.order_items
    UNION ALL
    SELECT 'shipping_limit_date'           AS column_name, LEN(shipping_limit_date) AS length FROM raw.order_items
    UNION ALL
    SELECT 'price'           AS column_name, LEN(price) AS length FROM raw.order_items
    UNION ALL
    SELECT 'freight_value'           AS column_name, LEN(freight_value) AS length FROM raw.order_items
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 3. Min/Max Character Length per Column (quotes cleansed)
-- ============================================================
SELECT column_name, MIN(length) AS min_length, MAX(length) AS max_length
FROM (
    SELECT 'order_id'              AS column_name, LEN(TRIM(REPLACE(order_id,  '"', ''))) AS length FROM raw.order_items
    UNION ALL
    SELECT 'order_item_id'       AS column_name, LEN(order_item_id) AS length FROM raw.order_items
    UNION ALL
    SELECT 'product_id' AS column_name, LEN(TRIM(REPLACE(product_id,  '"', ''))) AS length FROM raw.order_items
    UNION ALL
    SELECT 'seller_id'            AS column_name, LEN(TRIM(REPLACE(seller_id,  '"', ''))) AS length FROM raw.order_items
    UNION ALL
    SELECT 'shipping_limit_date'           AS column_name, LEN(shipping_limit_date) AS length FROM raw.order_items
    UNION ALL
    SELECT 'price'           AS column_name, LEN(price) AS length FROM raw.order_items
    UNION ALL
    SELECT 'freight_value'           AS column_name, LEN(freight_value) AS length FROM raw.order_items
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 2. Null Analysis
-- ============================================================
SELECT
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)            AS null_order_id,
    SUM(CASE WHEN order_item_id IS NULL THEN 1 ELSE 0 END)       AS null_order_item_id,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END)          AS null_product_id,
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END)           AS null_seller_id,
    SUM(CASE WHEN shipping_limit_date IS NULL THEN 1 ELSE 0 END) AS null_shipping_limit_date,
    SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END)               AS null_price,
    SUM(CASE WHEN freight_value IS NULL THEN 1 ELSE 0 END)       AS null_freight_value
FROM raw.order_items;


-- 3. Duplicate Check (order_id + order_item_id = composite key)
-- ============================================================
SELECT order_id, order_item_id, COUNT(*) AS duplicate_count
FROM raw.order_items
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

SELECT order_id, COUNT(*) AS duplicate_count
FROM raw.order_items
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

SELECT * FROM raw.order_items WHERE order_id = '"8272b63d03f5f79c56e9e4120aec44ef"'

-- 4. Items per Order Distribution
-- ============================================================
SELECT
    order_item_id AS item_id,
    COUNT(*)      AS orders_with_this_item_id
FROM raw.order_items
GROUP BY order_item_id
ORDER BY CAST(order_item_id AS INT) ASC;


-- 5. Orders with Multiple Items
-- ============================================================
SELECT
    COUNT(*)                                                        AS total_orders,
    SUM(CASE WHEN item_count > 1 THEN 1 ELSE 0 END)                 AS multi_item_orders,
    ROUND(SUM(CASE WHEN item_count > 1 THEN 1 ELSE 0 END) * 100.0
        / COUNT(DISTINCT order_id), 2)                              AS pct_multi_item
FROM (
    SELECT order_id, COUNT(*) AS item_count
    FROM raw.order_items
    GROUP BY order_id
) AS order_counts;


-- 6. Price Analysis
-- ============================================================
SELECT
    MIN(CAST(price AS DECIMAL(10,2)))   AS min_price,
    MAX(CAST(price AS DECIMAL(10,2)))   AS max_price,
    AVG(CAST(price AS DECIMAL(10,2)))   AS avg_price,
    ROUND(
        (SELECT TOP 1 CAST(price AS DECIMAL(10,2))
         FROM raw.order_items
         ORDER BY ABS(CAST(price AS DECIMAL(10,2)
        ) - (SELECT AVG(CAST(price AS DECIMAL(10,2))) FROM raw.order_items))
    ), 2)                           AS approx_median_price
FROM raw.order_items;


-- 7. Freight Value Analysis
-- ============================================================
SELECT
    MIN(CAST(freight_value AS DECIMAL(10,2)))  AS min_freight,
    MAX(CAST(freight_value AS DECIMAL(10,2)))  AS max_freight,
    AVG(CAST(freight_value AS DECIMAL(10,2)))  AS avg_freight
FROM raw.order_items;


-- 8. Outliers: Zero or Negative Prices
-- ============================================================
SELECT COUNT(*) AS zero_or_negative_price
FROM raw.order_items
WHERE CAST(price AS DECIMAL(10,2)) <= 0;


-- 9. Outliers: Extremely High Prices (top 10)
-- ============================================================
SELECT TOP 10
    order_id,
    product_id,
    order_item_id,
    CAST(price AS DECIMAL(10,2))         AS price,
    CAST(freight_value AS DECIMAL(10,2)) AS freight_value
FROM raw.order_items
ORDER BY CAST(price AS DECIMAL(10,2)) DESC;


-- 10. Top 10 Sellers by Volume
-- ============================================================
SELECT TOP 10
    seller_id,
    COUNT(*)                             AS total_items_sold,
    ROUND(SUM(CAST(price AS DECIMAL(10,2))), 2) AS total_revenue
FROM raw.order_items
GROUP BY seller_id
ORDER BY total_items_sold DESC;
