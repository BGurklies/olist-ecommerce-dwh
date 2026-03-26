CREATE OR ALTER PROCEDURE cleansed.sp_load_order_items
    @run_id NVARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- --------------------------------------------------------
    -- 1. Error Logging (DQ Checks) - based on raw.order_items
    -- --------------------------------------------------------

    -- NULL order_id
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', order_id, 'order_id', 'NULL value', NULL
    FROM raw.order_items
    WHERE order_id IS NULL;

    -- NULL order_item_id
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', order_id, 'order_item_id', 'NULL value', NULL
    FROM raw.order_items
    WHERE order_item_id IS NULL;

    -- NULL product_id
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', order_id, 'product_id', 'NULL value', NULL
    FROM raw.order_items
    WHERE product_id IS NULL;

    -- NULL seller_id
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', order_id, 'seller_id', 'NULL value', NULL
    FROM raw.order_items
    WHERE seller_id IS NULL;

    -- NULL shipping_limit_date
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', order_id, 'shipping_limit_date', 'NULL value', NULL
    FROM raw.order_items
    WHERE shipping_limit_date IS NULL;

    -- NULL price
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', order_id, 'price', 'NULL value', NULL
    FROM raw.order_items
    WHERE price IS NULL;

    -- NULL freight_value
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', order_id, 'freight_value', 'NULL value', NULL
    FROM raw.order_items
    WHERE freight_value IS NULL;

    -- Empty string after cleansing: order_id
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', order_id, 'order_id', 'Empty string after cleansing', order_id
    FROM raw.order_items
    WHERE order_id IS NOT NULL
      AND LEN(TRIM(REPLACE(order_id, '"', ''))) = 0;

    -- Empty string after cleansing: order_item_id
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', order_id, 'order_item_id', 'Empty string after cleansing', order_item_id
    FROM raw.order_items
    WHERE order_item_id IS NOT NULL
      AND LEN(REPLACE(TRIM(order_item_id), '"', '')) = 0;

    -- Empty string after cleansing: product_id
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', order_id, 'product_id', 'Empty string after cleansing', product_id
    FROM raw.order_items
    WHERE product_id IS NOT NULL
      AND LEN(TRIM(REPLACE(product_id, '"', ''))) = 0;

    -- Empty string after cleansing: seller_id
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', order_id, 'seller_id', 'Empty string after cleansing', seller_id
    FROM raw.order_items
    WHERE seller_id IS NOT NULL
      AND LEN(TRIM(REPLACE(seller_id, '"', ''))) = 0;

    -- Invalid length: order_id (should be 32)
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', order_id, 'order_id',
        'Invalid length after cleansing: ' + CAST(LEN(TRIM(REPLACE(order_id, '"', ''))) AS NVARCHAR),
        order_id
    FROM raw.order_items
    WHERE order_id IS NOT NULL
      AND LEN(TRIM(REPLACE(order_id, '"', ''))) != 32;

   -- Invalid length: product_id (should be 32)
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', product_id, 'product_id',
        'Invalid length after cleansing: ' + CAST(LEN(TRIM(REPLACE(product_id, '"', ''))) AS NVARCHAR),
        product_id
    FROM raw.order_items
    WHERE product_id IS NOT NULL
      AND LEN(TRIM(REPLACE(product_id, '"', ''))) != 32;

    -- Invalid length: seller_id (should be 32)
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', seller_id, 'seller_id',
        'Invalid length after cleansing: ' + CAST(LEN(TRIM(REPLACE(seller_id, '"', ''))) AS NVARCHAR),
        seller_id
    FROM raw.order_items
    WHERE seller_id IS NOT NULL
      AND LEN(TRIM(REPLACE(seller_id, '"', ''))) != 32;

    -- Invalid datetime format: shipping_limit_date
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', order_id, 'shipping_limit_date', 'Invalid datetime format', shipping_limit_date
    FROM raw.order_items
    WHERE shipping_limit_date IS NOT NULL
      AND TRY_CONVERT(DATETIME, shipping_limit_date, 120) IS NULL;

    -- Invalid decimal format: price
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', order_id, 'price', 'Invalid decimal format', price
    FROM raw.order_items
    WHERE price IS NOT NULL
      AND TRY_CONVERT(DECIMAL(10,2), price) IS NULL;

    -- Invalid decimal format: freight_value
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', order_id, 'freight_value', 'Invalid decimal format', freight_value
    FROM raw.order_items
    WHERE freight_value IS NOT NULL
      AND TRY_CONVERT(DECIMAL(10,2), freight_value) IS NULL;

    -- Outlier: non-positive price
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'order_items', order_id, 'price', 'Outlier: price <= 0', price
    FROM raw.order_items
    WHERE TRY_CONVERT(DECIMAL(10,2), price) <= 0;

    -- --------------------------------------------------------
    -- 2. Incremental upsert into cleansed.order_items (MERGE)
    -- Grain: (order_id, order_item_id)
    -- --------------------------------------------------------

    ;WITH normalized AS (
        SELECT
            REPLACE(TRIM(order_id), '"', '')       AS order_id,
            TRIM(order_item_id) AS order_item_id,
            REPLACE(TRIM(product_id), '"', '')    AS product_id,
            REPLACE(TRIM(seller_id), '"', '')     AS seller_id,
            TRY_CONVERT(DATETIME, shipping_limit_date, 120) AS shipping_limit_date,
            TRY_CONVERT(DECIMAL(10,2), price)    AS price,
            TRY_CONVERT(DECIMAL(10,2), freight_value) AS freight_value
        FROM raw.order_items
    ),
    hashed AS (
        SELECT
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value,
            HASHBYTES('SHA2_256', CONCAT(
                order_id, '|',
                order_item_id, '|',
                product_id, '|',
                seller_id, '|',
                COALESCE(CONVERT(NVARCHAR(30), shipping_limit_date, 126), ''), '|',
                COALESCE(CONVERT(NVARCHAR(50), price), ''), '|',
                COALESCE(CONVERT(NVARCHAR(50), freight_value), '')
            )) AS row_hash
        FROM normalized
    )
    MERGE cleansed.order_items AS tgt
    USING (
       SELECT
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value,
            row_hash
        FROM hashed
        WHERE order_id IS NOT NULL
          AND order_item_id IS NOT NULL
          AND product_id IS NOT NULL
          AND seller_id IS NOT NULL
          AND shipping_limit_date IS NOT NULL
          AND price IS NOT NULL
          AND freight_value IS NOT NULL
          AND LEN(order_id) = 32
          AND LEN(product_id) = 32
          AND LEN(seller_id) = 32
    ) AS src
    ON tgt.order_id = src.order_id
        AND tgt.order_item_id = src.order_item_id
    WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN
        UPDATE SET
            product_id = src.product_id,
            seller_id = src.seller_id,
            shipping_limit_date = src.shipping_limit_date,
            price = src.price,
            freight_value = src.freight_value,
            row_hash = src.row_hash,
            updated_at = GETDATE()
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value,
            row_hash,
            updated_at
        )
        VALUES (
            src.order_id,
            src.order_item_id,
            src.product_id,
            src.seller_id,
            src.shipping_limit_date,
            src.price,
            src.freight_value,
            src.row_hash,
            GETDATE()
        );

    DECLARE @merge_rowcount INT;
    SET @merge_rowcount = @@ROWCOUNT;

    DECLARE @error_count INT;
    SELECT @error_count = COUNT(*)
    FROM cleansed.error_log
    WHERE table_name = 'order_items';

    PRINT 'cleansed.order_items merged (insert/update): ' + CAST(@merge_rowcount AS NVARCHAR) + ' rows';
    PRINT 'Errors logged: ' + CAST(@error_count AS NVARCHAR);
END;
GO
