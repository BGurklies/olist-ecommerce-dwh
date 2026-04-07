USE OlistDWH;

-- EDA: raw.geolocation

-- Analysis is based on the most recent batch_id to reflect the latest data state. Adjust as needed for historical analysis.
DECLARE @batch_id UNIQUEIDENTIFIER;
SELECT TOP 1 @batch_id = batch_id
FROM raw.geolocation
ORDER BY load_ts DESC;


-- 0. Data Preview
SELECT TOP 10 *
FROM raw.geolocation
WHERE batch_id = @batch_id;


-- 1. Row Count
SELECT COUNT(*) AS total_rows
FROM raw.geolocation
WHERE batch_id = @batch_id;


-- 2. Null Analysis
SELECT
    SUM(CASE WHEN geolocation_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_zip_code_prefix,
    SUM(CASE WHEN geolocation_lat IS NULL THEN 1 ELSE 0 END)             AS null_lat,
    SUM(CASE WHEN geolocation_lng IS NULL THEN 1 ELSE 0 END)             AS null_lng,
    SUM(CASE WHEN geolocation_city IS NULL THEN 1 ELSE 0 END)            AS null_city,
    SUM(CASE WHEN geolocation_state IS NULL THEN 1 ELSE 0 END)           AS null_state
FROM raw.geolocation
WHERE batch_id = @batch_id;


-- 3. Empty String Analysis (after cleansing)
SELECT
    SUM(CASE WHEN TRIM(geolocation_zip_code_prefix) = '' THEN 1 ELSE 0 END) AS empty_zip_code_prefix,
    SUM(CASE WHEN TRIM(geolocation_city)  = '' THEN 1 ELSE 0 END)           AS empty_city,
    SUM(CASE WHEN TRIM(geolocation_state) = '' THEN 1 ELSE 0 END)           AS empty_state
FROM raw.geolocation
WHERE batch_id = @batch_id;


-- 4. Coordinate Parse Failures (TRY_CONVERT returns NULL on malformed input)
SELECT
    SUM(CASE WHEN geolocation_lat IS NOT NULL AND TRY_CONVERT(FLOAT, geolocation_lat) IS NULL THEN 1 ELSE 0 END) AS unparseable_lat,
    SUM(CASE WHEN geolocation_lng IS NOT NULL AND TRY_CONVERT(FLOAT, geolocation_lng) IS NULL THEN 1 ELSE 0 END) AS unparseable_lng
FROM raw.geolocation
WHERE batch_id = @batch_id;


-- 5. Zip Code Format Check (Brazilian CEP prefix: 5 numeric digits)
SELECT
    SUM(CASE WHEN LEN(TRIM(geolocation_zip_code_prefix)) != 5                           THEN 1 ELSE 0 END) AS invalid_length,
    SUM(CASE WHEN LEN(TRIM(geolocation_zip_code_prefix))  = 5
             AND TRIM(geolocation_zip_code_prefix) LIKE '%[^0-9]%'                       THEN 1 ELSE 0 END) AS invalid_format
FROM raw.geolocation
WHERE batch_id = @batch_id
  AND geolocation_zip_code_prefix IS NOT NULL;


-- 6. Duplicate Rows (vollständige Duplikate)
SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state,
    COUNT(*) AS cnt
FROM raw.geolocation
WHERE batch_id = @batch_id
GROUP BY
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
HAVING COUNT(*) > 1
ORDER BY cnt DESC;


-- 7. Distinct vs Total (Duplikatausmaß)
SELECT
    COUNT(*)                                                                                                                AS total_rows,
    COUNT(DISTINCT geolocation_zip_code_prefix)                                                                            AS distinct_zip_prefixes,
    COUNT(DISTINCT CONCAT(geolocation_zip_code_prefix, '|', geolocation_lat, '|', geolocation_lng))                        AS distinct_coordinates,
    COUNT(*) - COUNT(DISTINCT CONCAT(geolocation_zip_code_prefix, '|', geolocation_lat, '|', geolocation_lng))             AS duplicate_coordinates
FROM raw.geolocation
WHERE batch_id = @batch_id;


-- 8. Multiple Coordinates per Zip Code (Koordinatenstreuung)
SELECT
    geolocation_zip_code_prefix,
    COUNT(*)                                    AS cnt,
    COUNT(DISTINCT geolocation_lat)             AS distinct_lats,
    COUNT(DISTINCT geolocation_lng)             AS distinct_lngs,
    MIN(TRY_CONVERT(FLOAT, geolocation_lat))    AS min_lat,
    MAX(TRY_CONVERT(FLOAT, geolocation_lat))    AS max_lat,
    MIN(TRY_CONVERT(FLOAT, geolocation_lng))    AS min_lng,
    MAX(TRY_CONVERT(FLOAT, geolocation_lng))    AS max_lng
FROM raw.geolocation
WHERE batch_id = @batch_id
GROUP BY geolocation_zip_code_prefix
HAVING COUNT(*) > 1
ORDER BY cnt DESC;


-- 9. Distribution by State
SELECT
    geolocation_state,
    COUNT(*)                                           AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM raw.geolocation
WHERE batch_id = @batch_id
GROUP BY geolocation_state
ORDER BY cnt DESC;


-- 10. Cities with Inconsistent State Assignment
SELECT
    geolocation_city,
    COUNT(DISTINCT geolocation_state)                                            AS distinct_states,
    STRING_AGG(geolocation_state, ', ') WITHIN GROUP (ORDER BY geolocation_state) AS states
FROM (
    SELECT DISTINCT geolocation_city, geolocation_state
    FROM raw.geolocation
    WHERE batch_id = @batch_id
) AS deduped
GROUP BY geolocation_city
HAVING COUNT(DISTINCT geolocation_state) > 1
ORDER BY distinct_states DESC;


-- 11. Outliers: Coordinates outside Brazil bounds
-- lat: -33.7683 to 5.2717 | lng: -73.9828 to -34.7930
SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
FROM raw.geolocation
WHERE batch_id = @batch_id
  AND TRY_CONVERT(FLOAT, geolocation_lat) IS NOT NULL
  AND TRY_CONVERT(FLOAT, geolocation_lng) IS NOT NULL
  AND (
      TRY_CONVERT(FLOAT, geolocation_lat) < -33.7683
      OR TRY_CONVERT(FLOAT, geolocation_lat) >   5.2717
      OR TRY_CONVERT(FLOAT, geolocation_lng) < -73.9828
      OR TRY_CONVERT(FLOAT, geolocation_lng) > -34.7930
  )
ORDER BY geolocation_zip_code_prefix;


-- 12. Zip Code Prefix Range
SELECT
    COUNT(DISTINCT geolocation_zip_code_prefix) AS distinct_zips,
    MIN(geolocation_zip_code_prefix)            AS min_zip,
    MAX(geolocation_zip_code_prefix)            AS max_zip
FROM raw.geolocation
WHERE batch_id = @batch_id;


-- 13. Referential Integrity: geolocation zip codes not found in customers
SELECT COUNT(DISTINCT geolocation_zip_code_prefix) AS zips_without_customer
FROM raw.geolocation g
WHERE g.batch_id = @batch_id
  AND NOT EXISTS (
    SELECT 1 FROM raw.customers c WHERE c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
);
