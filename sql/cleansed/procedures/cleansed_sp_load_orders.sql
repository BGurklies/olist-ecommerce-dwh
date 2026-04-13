USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE cleansed.sp_load_orders
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
            'CLEANSED',     'cleansed.sp_load_orders', 'cleansed.orders',
            0,              'RUNNING',   SYSUTCDATETIME()
        );

        -- 1. Normalize raw data into a temp table so DQ checks and the MERGE
        --    share the same cleaned values without duplicating transformation logic.
        SELECT
            row_id,
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date,
            REPLACE(TRIM(order_id),       '"', '')  AS clean_order_id,
            REPLACE(TRIM(customer_id),    '"', '')  AS clean_customer_id,
            TRIM(order_status)                      AS clean_order_status,
            TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp),      120) AS parsed_order_purchase_ts,
            TRY_CONVERT(DATETIME2(0), TRIM(order_approved_at),             120) AS parsed_order_approved_at,
            TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_carrier_date),  120) AS parsed_order_delivered_carrier_date,
            TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_customer_date), 120) AS parsed_order_delivered_customer_date,
            TRY_CONVERT(DATETIME2(0), TRIM(order_estimated_delivery_date), 120) AS parsed_order_estimated_delivery_date
        INTO #normalized_orders
        FROM raw.orders
        WHERE batch_id = @batch_id;

        -- 2. DQ checks: completeness, validity (length + format + range), uniqueness.
        --    One dq_log row per distinct (column_name, issue) category with affected_row_count.
        WITH dq_checks AS (

            -- Completeness: NULL checks
            SELECT 'order_id'                      AS column_name, 'NULL value' AS issue FROM #normalized_orders WHERE clean_order_id IS NULL
            UNION ALL
            SELECT 'customer_id',                   'NULL value'                         FROM #normalized_orders WHERE clean_customer_id IS NULL
            UNION ALL
            SELECT 'order_status',                  'NULL value'                         FROM #normalized_orders WHERE clean_order_status IS NULL
            UNION ALL
            SELECT 'order_purchase_timestamp',      'NULL value'                         FROM #normalized_orders WHERE order_purchase_timestamp IS NULL
            UNION ALL
            SELECT 'order_estimated_delivery_date', 'NULL value'                         FROM #normalized_orders WHERE order_estimated_delivery_date IS NULL

            -- Completeness: empty string checks after cleansing
            UNION ALL
            SELECT 'order_id',     'Empty string after cleansing' FROM #normalized_orders WHERE clean_order_id = ''
            UNION ALL
            SELECT 'customer_id',  'Empty string after cleansing' FROM #normalized_orders WHERE clean_customer_id = ''
            UNION ALL
            SELECT 'order_status', 'Empty string after cleansing' FROM #normalized_orders WHERE clean_order_status = ''

            -- Validity: length and format checks (hexids)
            UNION ALL
            SELECT 'order_id',    'Invalid length or format: expected 32-char lowercase hex' FROM #normalized_orders WHERE clean_order_id != ''   AND (LEN(clean_order_id) != 32 OR clean_order_id LIKE '%[^0-9a-f]%')
            UNION ALL
            SELECT 'customer_id', 'Invalid length or format: expected 32-char lowercase hex' FROM #normalized_orders WHERE clean_customer_id != '' AND (LEN(clean_customer_id) != 32 OR clean_customer_id LIKE '%[^0-9a-f]%')

            -- Validity: order_status allowed values
            UNION ALL
            SELECT 'order_status', 'Invalid value: not a recognized order status'
            FROM #normalized_orders
            WHERE clean_order_status != ''
              AND clean_order_status NOT IN ('delivered','shipped','canceled','unavailable','invoiced','processing','approved','created')

            -- Validity: datetime format
            UNION ALL
            SELECT 'order_purchase_timestamp',      'Invalid datetime format' FROM #normalized_orders WHERE order_purchase_timestamp IS NOT NULL      AND parsed_order_purchase_ts IS NULL
            UNION ALL
            SELECT 'order_approved_at',             'Invalid datetime format' FROM #normalized_orders WHERE order_approved_at IS NOT NULL             AND parsed_order_approved_at IS NULL
            UNION ALL
            SELECT 'order_delivered_carrier_date',  'Invalid datetime format' FROM #normalized_orders WHERE order_delivered_carrier_date IS NOT NULL  AND parsed_order_delivered_carrier_date IS NULL
            UNION ALL
            SELECT 'order_delivered_customer_date', 'Invalid datetime format' FROM #normalized_orders WHERE order_delivered_customer_date IS NOT NULL AND parsed_order_delivered_customer_date IS NULL
            UNION ALL
            SELECT 'order_estimated_delivery_date', 'Invalid datetime format' FROM #normalized_orders WHERE order_estimated_delivery_date IS NOT NULL AND parsed_order_estimated_delivery_date IS NULL

            -- Validity: logical — customer date cannot precede purchase date
            UNION ALL
            SELECT 'order_delivered_customer_date', 'Delivered before purchase'
            FROM #normalized_orders
            WHERE parsed_order_delivered_customer_date IS NOT NULL
              AND parsed_order_purchase_ts   IS NOT NULL
              AND parsed_order_delivered_customer_date < parsed_order_purchase_ts

            -- Uniqueness: one row per duplicate occurrence so outer GROUP BY counts total
            UNION ALL
            SELECT 'order_id', 'Duplicate order_id in batch'
            FROM (SELECT COUNT(*) OVER (PARTITION BY order_id) AS cnt FROM #normalized_orders) d
            WHERE cnt > 1

        )

        INSERT INTO audit.dq_log (batch_id, job_run_id, table_name, column_name, issue, affected_row_count)
        SELECT @batch_id, @job_run_id, 'orders', column_name, issue, COUNT(*)
        FROM dq_checks
        GROUP BY column_name, issue;

        -- Abort if duplicates were detected.
        IF EXISTS (
            SELECT 1 FROM audit.dq_log
            WHERE batch_id   = @batch_id
              AND table_name = 'orders'
              AND issue LIKE 'Duplicate%'
        )
            THROW 50004, 'Duplicate order_id values detected in batch. Investigate dq_log before reloading.', 1;

        -- 3. Incremental upsert + soft delete via MERGE. row_hash detects changed rows
        --    to avoid unnecessary updates. Rows absent from the current batch are soft-
        --    deleted (is_deleted = 1) rather than removed. Reappearing rows are reactivated.
        BEGIN TRANSACTION;
        ;WITH hashed AS (
            SELECT
                clean_order_id,
                clean_customer_id,
                clean_order_status,
                parsed_order_purchase_ts,
                parsed_order_approved_at,
                parsed_order_delivered_carrier_date,
                parsed_order_delivered_customer_date,
                parsed_order_estimated_delivery_date,
                HASHBYTES('SHA2_256', CONCAT(
                    clean_order_id,    '|', clean_customer_id,   '|', clean_order_status, '|',
                    ISNULL(CONVERT(NVARCHAR(19), parsed_order_purchase_ts,   120), ''), '|',
                    ISNULL(CONVERT(NVARCHAR(19), parsed_order_approved_at,   120), ''), '|',
                    ISNULL(CONVERT(NVARCHAR(19), parsed_order_delivered_carrier_date,  120), ''), '|',
                    ISNULL(CONVERT(NVARCHAR(19), parsed_order_delivered_customer_date, 120), ''), '|',
                    ISNULL(CONVERT(NVARCHAR(19), parsed_order_estimated_delivery_date,120), '')
                )) AS row_hash
            FROM #normalized_orders
        )
        MERGE cleansed.orders AS tgt
        USING (
            SELECT *
            FROM hashed
            WHERE clean_order_id IS NOT NULL AND clean_order_id != '' AND clean_order_id NOT LIKE '%[^0-9a-f]%'     AND LEN(clean_order_id) = 32
              AND clean_customer_id IS NOT NULL AND clean_customer_id != '' AND clean_customer_id NOT LIKE '%[^0-9a-f]%'  AND LEN(clean_customer_id) = 32
              AND clean_order_status IS NOT NULL AND clean_order_status != ''
              AND parsed_order_purchase_ts IS NOT NULL
              AND parsed_order_estimated_delivery_date IS NOT NULL
        ) AS src
        ON tgt.order_id = src.clean_order_id
        -- Data changed or row is reactivating after a soft delete
        WHEN MATCHED AND (tgt.row_hash <> src.row_hash OR tgt.is_deleted = 1) THEN
            UPDATE SET
                customer_id                   = src.clean_customer_id,
                order_status                  = src.clean_order_status,
                order_purchase_timestamp      = src.parsed_order_purchase_ts,
                order_approved_at             = src.parsed_order_approved_at,
                order_delivered_carrier_date  = src.parsed_order_delivered_carrier_date,
                order_delivered_customer_date = src.parsed_order_delivered_customer_date,
                order_estimated_delivery_date = src.parsed_order_estimated_delivery_date,
                row_hash                      = src.row_hash,
                is_deleted                    = 0,
                deleted_at                    = NULL,
                updated_at                    = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                order_id,              customer_id,
                order_status,          order_purchase_timestamp,
                order_approved_at,     order_delivered_carrier_date,
                order_delivered_customer_date, order_estimated_delivery_date,
                row_hash,              updated_at
            )
            VALUES (
                src.clean_order_id,    src.clean_customer_id,
                src.clean_order_status, src.parsed_order_purchase_ts,
                src.parsed_order_approved_at, src.parsed_order_delivered_carrier_date,
                src.parsed_order_delivered_customer_date, src.parsed_order_estimated_delivery_date,
                src.row_hash,          SYSUTCDATETIME()
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
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_orders';

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
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_orders';

        INSERT INTO audit.error_log (
            batch_id,      job_run_id,  pipeline_id, sp_name,
            error_message, error_ts,
            error_severity, error_procedure, error_line
        )
        VALUES (
            @batch_id,     @job_run_id, @pipeline_id, 'cleansed.sp_load_orders',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
