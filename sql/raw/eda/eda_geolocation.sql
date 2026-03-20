USE OlistDWH;

-- ============================================================
-- EDA: raw.geolocation
-- ============================================================

-- 0. Data Preview
-- ============================================================
SELECT TOP 10 *
FROM raw.geolocation;


-- 1. Row Count
-- ============================================================
SELECT COUNT(*) AS total_rows
FROM raw.geolocation;


-- 2. Null Analysis
-- ============================================================
SELECT
    SUM(CASE WHEN geolocation_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_zip_code_prefix,
    SUM(CASE WHEN geolocation_lat IS NULL THEN 1 ELSE 0 END)             AS null_lat,
    SUM(CASE WHEN geolocation_lng IS NULL THEN 1 ELSE 0 END)             AS null_lng,
    SUM(CASE WHEN geolocation_city IS NULL THEN 1 ELSE 0 END)            AS null_city,
    SUM(CASE WHEN geolocation_state IS NULL THEN 1 ELSE 0 END)           AS null_state
FROM raw.geolocation;


-- 3. Duplicate Rows (vollständige Duplikate)
-- ============================================================
SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state,
    COUNT(*) AS cnt
FROM raw.geolocation
GROUP BY
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
HAVING COUNT(*) > 1
ORDER BY cnt DESC;


-- 4. Distinct vs Total (Duplikatausmaß)
-- ============================================================
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT geolocation_zip_code_prefix) AS distinct_zip_prefixes,
    COUNT(*) - COUNT(DISTINCT geolocation_zip_code_prefix) AS excess_rows
FROM raw.geolocation;


-- 5. Multiple Coordinates per Zip Code (Koordinatenstreuung)
-- ============================================================
SELECT
    geolocation_zip_code_prefix,
    COUNT(*) AS cnt,
    COUNT(DISTINCT geolocation_lat) AS distinct_lats,
    COUNT(DISTINCT geolocation_lng) AS distinct_lngs,
    MIN(CAST(geolocation_lat AS FLOAT)) AS min_lat,
    MAX(CAST(geolocation_lat AS FLOAT)) AS max_lat,
    MIN(CAST(geolocation_lng AS FLOAT)) AS min_lng,
    MAX(CAST(geolocation_lng AS FLOAT)) AS max_lng
FROM raw.geolocation
GROUP BY geolocation_zip_code_prefix
HAVING COUNT(*) > 1
ORDER BY cnt DESC;


-- 6. Distribution by State
-- ============================================================
SELECT
    geolocation_state,
    COUNT(*) AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM raw.geolocation
GROUP BY geolocation_state
ORDER BY cnt DESC;


-- 7. Cities with Inconsistent State Assignment
-- ============================================================
SELECT
    geolocation_city,
    COUNT(DISTINCT geolocation_state) AS distinct_states,
    STRING_AGG(geolocation_state, ', ') WITHIN GROUP (ORDER BY geolocation_state) AS states
FROM (
    SELECT DISTINCT geolocation_city, geolocation_state
    FROM raw.geolocation
) AS dedupde
GROUP BY geolocation_city
HAVING COUNT(DISTINCT geolocation_state) >1
ORDER BY distinct_states DESC;


-- 8. Outliers: Coordinates outside Brazil bounds
-- lat: -33.7683 to 5.2717 | lng: -73.9828 to -34.7930
-- ============================================================
SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
FROM raw.geolocation
WHERE
    CAST(geolocation_lat AS FLOAT) < -33.7683
    OR CAST(geolocation_lat AS FLOAT) > 5.2717
    OR CAST(geolocation_lng AS FLOAT) < -73.9828
    OR CAST(geolocation_lng AS FLOAT) > -34.7930
ORDER BY geolocation_zip_code_prefix;


-- 9. Zip Code Prefix Range
-- ============================================================
SELECT
    COUNT(DISTINCT geolocation_zip_code_prefix) AS distinct_zips,
    MIN(geolocation_zip_code_prefix)            AS min_zip,
    MAX(geolocation_zip_code_prefix)            AS max_zip
FROM raw.geolocation;
