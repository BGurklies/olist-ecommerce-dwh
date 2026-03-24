CREATE OR ALTER PROCEDURE cleansed.sp_load_orders
AS
BEGIN
    SET NOCOUNT ON;

    -- --------------------------------------------------------
    -- 1. Truncate
    -- --------------------------------------------------------
    TRUNCATE TABLE cleansed.orders;


    -- --------------------------------------------------------
    -- 2. Error Logging
    -- --------------------------------------------------------

    -- NULL order_id
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'orders', order_id, 'order_id', 'NULL value', NULL
    FROM raw.orders
    WHERE order_id IS NULL;

    -- NULL customer_id
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'orders', order_id, 'customer_id', 'NULL value', NULL
    FROM raw.orders
    WHERE customer_id IS NULL;

    -- NULL order_status
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'orders', order_id, 'order_status', 'NULL value', NULL
    FROM raw.orders
    WHERE order_status IS NULL;

    -- NULL order_purchase_timestamp
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'orders', order_id, 'order_purchase_timestamp', 'NULL value', NULL
    FROM raw.orders
    WHERE order_purchase_timestamp IS NULL;

    -- NULL order_estimated_delivery_date
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'orders', order_id, 'order_estimated_delivery_date', 'NULL value', NULL
    FROM raw.orders
    WHERE order_estimated_delivery_date IS NULL;

    -- Empty string after cleansing: order_id
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'orders', order_id, 'order_id', 'Empty string after cleansing', order_id
    FROM raw.orders
    WHERE TRIM(REPLACE(order_id, '"', '')) = '';

    -- Empty string after cleansing: customer_id
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'orders', order_id, 'customer_id', 'Empty string after cleansing', customer_id
    FROM raw.orders
    WHERE TRIM(REPLACE(customer_id, '"', '')) = '';

    -- Empty string after cleansing: order_status
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'orders', order_id, 'order_status', 'Empty string after cleansing', order_status
    FROM raw.orders
    WHERE TRIM(order_status) = '';

    -- Invalid length: order_id (should be 32)
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'orders', order_id, 'order_id',
        'Invalid length after cleansing: ' + CAST(LEN(TRIM(REPLACE(order_id, '"', ''))) AS NVARCHAR),
        order_id
    FROM raw.orders
    WHERE LEN(TRIM(REPLACE(order_id, '"', ''))) != 32
    AND order_id IS NOT NULL;

    -- Invalid length: customer_id (should be 32)
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'orders', order_id, 'customer_id',
        'Invalid length after cleansing: ' + CAST(LEN(TRIM(REPLACE(customer_id, '"', ''))) AS NVARCHAR),
        customer_id
    FROM raw.orders
    WHERE LEN(TRIM(REPLACE(customer_id, '"', ''))) != 32
    AND customer_id IS NOT NULL;

    -- Invalid datetime format: order_purchase_timestamp
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'orders', order_id, 'order_purchase_timestamp', 'Invalid datetime format', order_purchase_timestamp
    FROM raw.orders
    WHERE TRY_CONVERT(DATETIME, order_purchase_timestamp, 120) IS NULL
    AND order_purchase_timestamp IS NOT NULL;

    -- Invalid datetime format: order_approved_at
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'orders', order_id, 'order_approved_at', 'Invalid datetime format', order_approved_at
    FROM raw.orders
    WHERE TRY_CONVERT(DATETIME, order_approved_at, 120) IS NULL
    AND order_approved_at IS NOT NULL;

    -- Invalid datetime format: order_delivered_carrier_date
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'orders', order_id, 'order_delivered_carrier_date', 'Invalid datetime format', order_delivered_carrier_date
    FROM raw.orders
    WHERE TRY_CONVERT(DATETIME, order_delivered_carrier_date, 120) IS NULL
    AND order_delivered_carrier_date IS NOT NULL;

    -- Invalid datetime format: order_delivered_customer_date
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'orders', order_id, 'order_delivered_customer_date', 'Invalid datetime format', order_delivered_customer_date
    FROM raw.orders
    WHERE TRY_CONVERT(DATETIME, order_delivered_customer_date, 120) IS NULL
    AND order_delivered_customer_date IS NOT NULL;

    -- Invalid datetime format: order_estimated_delivery_date
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'orders', order_id, 'order_estimated_delivery_date', 'Invalid datetime format', order_estimated_delivery_date
    FROM raw.orders
    WHERE TRY_CONVERT(DATETIME, order_estimated_delivery_date, 120) IS NULL
    AND order_estimated_delivery_date IS NOT NULL;

    -- Logical check: delivered_customer_date before purchase_timestamp
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'orders', order_id, 'order_delivered_customer_date',
        'Delivered before purchase',
        order_delivered_customer_date
    FROM raw.orders
    WHERE TRY_CONVERT(DATETIME, order_delivered_customer_date, 120) < TRY_CONVERT(DATETIME, order_purchase_timestamp, 120)
    AND order_delivered_customer_date IS NOT NULL;


    -- --------------------------------------------------------
    -- 3. Load cleansed data
    -- --------------------------------------------------------
    INSERT INTO cleansed.orders (
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    )
    SELECT
        REPLACE(TRIM(order_id),       '"', ''),
        REPLACE(TRIM(customer_id),    '"', ''),
        TRIM(order_status),
        TRY_CONVERT(DATETIME, order_purchase_timestamp, 120),
        TRY_CONVERT(DATETIME, order_approved_at, 120),
        TRY_CONVERT(DATETIME, order_delivered_carrier_date, 120),
        TRY_CONVERT(DATETIME, order_delivered_customer_date, 120),
        TRY_CONVERT(DATETIME, order_estimated_delivery_date, 120)
    FROM raw.orders;

    DECLARE @error_count INT;
    SELECT @error_count = COUNT(*) FROM cleansed.error_log WHERE table_name = 'orders';

    PRINT 'cleansed.orders loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' rows';
    PRINT 'Errors logged: ' + CAST(@error_count AS NVARCHAR);
END;
GO
