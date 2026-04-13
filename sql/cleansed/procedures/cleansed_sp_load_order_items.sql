USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE cleansed.sp_load_order_items
    @batch_id    UNIQUEIDENTIFIER OUTPUT,
    @pipeline_id INT              = NULL,
    @job_run_id  UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @merge_rowcount INT           = 0;
    DECLARE @start_time     DATETIME2(3)  = SYSUTCDATETIME();
    DECLARE @duration_ms    INT;
    DECLARE @error_msg      NVARCHAR(MAX);

    -- Inherit the RAW batch_id so the same ID flows through all layers
    -- (raw → cleansed → mart), enabling end-to-end tracing.
    SELECT @batch_id = src.last_batch_id
    FROM orchestration.pipeline_config cleansed_cfg
    JOIN orchestration.pipeline_config src
        ON src.pipeline_id      = cleansed_cfg.source_pipeline_id
    WHERE cleansed_cfg.pipeline_id = @pipeline_id
      AND src.last_run_status      = 'SUCCESS';

    BEGIN TRY
        INSERT INTO audit.load_log (
            batch_id,       job_run_id,  pipeline_id,
            layer,          sp_name,     table_name,
            rows_processed, status,      load_ts
        )
        VALUES (
            @batch_id,      @job_run_id, @pipeline_id,
            'CLEANSED',     'cleansed.sp_load_order_items', 'cleansed.order_items',
            0,              'RUNNING',   SYSUTCDATETIME()
        );

        -- 1. Normalize raw data into a temp table so DQ checks and the MERGE
        --    share the same cleaned values without duplicating transformation logic.
        SELECT
            row_id,
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value,
            REPLACE(TRIM(order_id),    '"', '')  AS clean_order_id,
            TRIM(order_item_id)                  AS clean_order_item_id,
            REPLACE(TRIM(product_id),  '"', '')  AS clean_product_id,
            REPLACE(TRIM(seller_id),   '"', '')  AS clean_seller_id,
            TRY_CONVERT(DATETIME2(0),  TRIM(shipping_limit_date), 120) AS parsed_shipping_date,
            TRY_CONVERT(DECIMAL(10,2), TRIM(price))                    AS parsed_price,
            TRY_CONVERT(DECIMAL(10,2), TRIM(freight_value))            AS parsed_freight_value
        INTO #normalized_order_items
        FROM raw.order_items
        WHERE batch_id = @batch_id;

        -- 2. DQ checks: completeness, validity (length + format + range), uniqueness.
        --    One dq_log row per distinct (column_name, issue) category with affected_row_count.
        WITH dq_checks AS (

            -- Completeness: NULL checks
            SELECT 'order_id'           AS column_name, 'NULL value' AS issue FROM #normalized_order_items WHERE clean_order_id IS NULL
            UNION ALL
            SELECT 'order_item_id',      'NULL value'                         FROM #normalized_order_items WHERE clean_order_item_id IS NULL
            UNION ALL
            SELECT 'product_id',         'NULL value'                         FROM #normalized_order_items WHERE clean_product_id IS NULL
            UNION ALL
            SELECT 'seller_id',          'NULL value'                         FROM #normalized_order_items WHERE clean_seller_id IS NULL
            UNION ALL
            SELECT 'shipping_limit_date','NULL value'                         FROM #normalized_order_items WHERE shipping_limit_date IS NULL
            UNION ALL
            SELECT 'price',              'NULL value'                         FROM #normalized_order_items WHERE price IS NULL
            UNION ALL
            SELECT 'freight_value',      'NULL value'                         FROM #normalized_order_items WHERE freight_value IS NULL

            -- Completeness: empty string checks after cleansing
            UNION ALL
            SELECT 'order_id',      'Empty string after cleansing' FROM #normalized_order_items WHERE clean_order_id = ''
            UNION ALL
            SELECT 'order_item_id', 'Empty string after cleansing' FROM #normalized_order_items WHERE clean_order_item_id = ''
            UNION ALL
            SELECT 'product_id',    'Empty string after cleansing' FROM #normalized_order_items WHERE clean_product_id = ''
            UNION ALL
            SELECT 'seller_id',     'Empty string after cleansing' FROM #normalized_order_items WHERE clean_seller_id = ''

            -- Validity: format and length checks (hex IDs)
            UNION ALL
            SELECT 'order_id',   'Invalid length or format: expected 32-char lowercase hex' FROM #normalized_order_items WHERE clean_order_id != ''   AND (LEN(clean_order_id) != 32 OR clean_order_id LIKE '%[^0-9a-f]%')
            UNION ALL
            SELECT 'product_id', 'Invalid length or format: expected 32-char lowercase hex' FROM #normalized_order_items WHERE clean_product_id != '' AND (LEN(clean_product_id) != 32 OR clean_product_id LIKE '%[^0-9a-f]%')
            UNION ALL
            SELECT 'seller_id',  'Invalid length or format: expected 32-char lowercase hex' FROM #normalized_order_items WHERE clean_seller_id != ''  AND (LEN(clean_seller_id) != 32 OR clean_seller_id LIKE '%[^0-9a-f]%')

            -- Validity: order_item_id must be numeric
            UNION ALL
            SELECT 'order_item_id', 'Invalid numeric format'
            FROM #normalized_order_items
            WHERE clean_order_item_id != '' AND clean_order_item_id LIKE '%[^0-9]%'

            -- Validity: datetime format
            UNION ALL
            SELECT 'shipping_limit_date', 'Invalid datetime format'
            FROM #normalized_order_items
            WHERE shipping_limit_date IS NOT NULL AND parsed_shipping_date IS NULL

            -- Validity: decimal format
            UNION ALL
            SELECT 'price',         'Invalid decimal format' FROM #normalized_order_items WHERE price IS NOT NULL         AND parsed_price IS NULL
            UNION ALL
            SELECT 'freight_value', 'Invalid decimal format' FROM #normalized_order_items WHERE freight_value IS NOT NULL AND parsed_freight_value IS NULL

            -- Validity: price must be positive
            UNION ALL
            SELECT 'price', 'Invalid range: must be > 0'
            FROM #normalized_order_items
            WHERE parsed_price IS NOT NULL AND parsed_price <= 0

            -- Uniqueness: one row per duplicate occurrence so outer GROUP BY counts total
            UNION ALL
            SELECT 'order_id, order_item_id', 'Duplicate (order_id, order_item_id) in batch'
            FROM (SELECT COUNT(*) OVER (PARTITION BY order_id, order_item_id) AS cnt FROM #normalized_order_items) d
            WHERE cnt > 1

        )

        INSERT INTO audit.dq_log (batch_id, job_run_id, table_name, column_name, issue, affected_row_count)
        SELECT @batch_id, @job_run_id, 'order_items', column_name, issue, COUNT(*)
        FROM dq_checks
        GROUP BY column_name, issue;

        -- Abort if duplicates were detected.
        IF EXISTS (
            SELECT 1 FROM audit.dq_log
            WHERE batch_id   = @batch_id
              AND table_name = 'order_items'
              AND issue LIKE 'Duplicate%'
        )
            THROW 50004, 'Duplicate (order_id, order_item_id) values detected in batch. Investigate dq_log before reloading.', 1;

        -- 3. Incremental upsert + soft delete via MERGE. row_hash detects changed rows
        --    to avoid unnecessary updates. Rows absent from the current batch are soft-
        --    deleted (is_deleted = 1) rather than removed. Reappearing rows are reactivated.
        BEGIN TRANSACTION;
        ;WITH hashed AS (
            SELECT
                clean_order_id,
                clean_order_item_id,
                clean_product_id,
                clean_seller_id,
                parsed_shipping_date,
                parsed_price,
                parsed_freight_value,
                HASHBYTES('SHA2_256', CONCAT(
                    clean_order_id,      '|', clean_order_item_id, '|',
                    clean_product_id,    '|', clean_seller_id,     '|',
                    ISNULL(CONVERT(NVARCHAR(19), parsed_shipping_date, 120), ''), '|',
                    ISNULL(CONVERT(NVARCHAR(20), parsed_price),        ''),       '|',
                    ISNULL(CONVERT(NVARCHAR(20), parsed_freight_value),'')
                )) AS row_hash
            FROM #normalized_order_items
        )
        MERGE cleansed.order_items AS tgt
        USING (
            SELECT *
            FROM hashed
            WHERE clean_order_id IS NOT NULL AND clean_order_id != ''       AND clean_order_id NOT LIKE '%[^0-9a-f]%'      AND LEN(clean_order_id) = 32
              AND clean_product_id IS NOT NULL AND clean_product_id != ''     AND clean_product_id NOT LIKE '%[^0-9a-f]%'    AND LEN(clean_product_id) = 32
              AND clean_seller_id IS NOT NULL AND clean_seller_id != ''      AND clean_seller_id NOT LIKE '%[^0-9a-f]%'     AND LEN(clean_seller_id) = 32
              AND clean_order_item_id IS NOT NULL AND clean_order_item_id != ''  AND clean_order_item_id NOT LIKE '%[^0-9]%'
              AND parsed_shipping_date IS NOT NULL
              AND parsed_price IS NOT NULL
              AND parsed_freight_value IS NOT NULL
        ) AS src
        ON tgt.order_id = src.clean_order_id AND tgt.order_item_id = src.clean_order_item_id
        -- Data changed or row is reactivating after a soft delete
        WHEN MATCHED AND (tgt.row_hash <> src.row_hash OR tgt.is_deleted = 1) THEN
            UPDATE SET
                product_id          = src.clean_product_id,
                seller_id           = src.clean_seller_id,
                shipping_limit_date = src.parsed_shipping_date,
                price               = src.parsed_price,
                freight_value       = src.parsed_freight_value,
                row_hash            = src.row_hash,
                is_deleted          = 0,
                deleted_at          = NULL,
                updated_at          = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                order_id,            order_item_id,
                product_id,          seller_id,
                shipping_limit_date, price,
                freight_value,       row_hash,        updated_at
            )
            VALUES (
                src.clean_order_id,  src.clean_order_item_id,
                src.clean_product_id, src.clean_seller_id,
                src.parsed_shipping_date, src.parsed_price,
                src.parsed_freight_value, src.row_hash, SYSUTCDATETIME()
            )
        -- Row exists in cleansed but not in current batch — source no longer contains it
        WHEN NOT MATCHED BY SOURCE AND tgt.is_deleted = 0 THEN
            UPDATE SET
                is_deleted = 1,
                deleted_at = SYSUTCDATETIME(),
                updated_at = SYSUTCDATETIME();

        SET @merge_rowcount = @@ROWCOUNT;
        SET @duration_ms    = DATEDIFF(MILLISECOND, @start_time, SYSUTCDATETIME());

        UPDATE audit.load_log
        SET rows_processed        = @merge_rowcount,
            status                = 'SUCCESS',
            processed_duration_ms = @duration_ms
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_order_items';

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        SET @error_msg   = ERROR_MESSAGE();
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, SYSUTCDATETIME());

        UPDATE audit.load_log
        SET status                = 'FAILED',
            processed_duration_ms = @duration_ms
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_order_items';

        INSERT INTO audit.error_log (
            batch_id,      job_run_id,  pipeline_id, sp_name,
            error_message, error_ts,
            error_severity, error_procedure, error_line
        )
        VALUES (
            @batch_id,     @job_run_id, @pipeline_id, 'cleansed.sp_load_order_items',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
