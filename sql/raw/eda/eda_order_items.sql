USE OlistDWH;

-- ============================================================
-- EDA: raw.order_items
-- ============================================================

-- 0. Data Preview
-- ============================================================
SELECT TOP 10 *
FROM raw.order_items;


-- 1. Row Count
-- ============================================================
SELECT COUNT(*) AS total_rows
FROM raw.order_items;


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




-- CONTINUE HERE -----------------------------------------------


-- XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX-----





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
