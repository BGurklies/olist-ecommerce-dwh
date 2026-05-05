USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE mart.sp_load_dim_product
    @pipeline_id INT              = NULL,
    @job_run_id  UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @rows         INT       = 0;
    DECLARE @rows_inserted INT      = 0;
    DECLARE @rows_updated  INT      = 0;
    DECLARE @merge_output  TABLE (action NVARCHAR(10));
    DECLARE @start_time  DATETIME2 = SYSUTCDATETIME();
    DECLARE @duration_ms INT;
    DECLARE @error_msg   NVARCHAR(MAX);
    DECLARE @log_id      INT;

    INSERT INTO audit.load_log (
        job_run_id,  pipeline_id,
        layer,       sp_name,     table_name,
        rows_processed, status,  load_ts
    )
    VALUES (
        @job_run_id, @pipeline_id,
        'MART',      'mart.sp_load_dim_product', 'mart.dim_product',
        0,           'RUNNING',   SYSUTCDATETIME()
    );
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Unknown member: surrogate -1 handles unresolvable FKs in fact tables.
        -- IDENTITY_INSERT required to force the explicit -1 value.
        IF NOT EXISTS (SELECT 1 FROM mart.dim_product WHERE product_key = -1)
        BEGIN
            SET IDENTITY_INSERT mart.dim_product ON;
            INSERT INTO mart.dim_product (product_key, product_id, product_category_name, product_category_name_english,
                product_name_length, product_description_length, product_photos_qty,
                product_weight_g, product_length_cm, product_height_cm, product_width_cm)
            VALUES (-1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', NULL, NULL, NULL, NULL, NULL, NULL, NULL);
            SET IDENTITY_INSERT mart.dim_product OFF;
        END

        -- SCD Type 1 MERGE from cleansed.products.
        ;WITH src AS (
            SELECT
                p.product_id,
                ISNULL(p.product_category_name, 'UNKNOWN') AS product_category_name,
                CASE COALESCE(t.product_category_name_english, p.product_category_name)
                    WHEN 'agro_industry_and_commerce'          THEN 'Agro Industry & Commerce'
                    WHEN 'air_conditioning'                    THEN 'Air Conditioning'
                    WHEN 'art'                                 THEN 'Art'
                    WHEN 'arts_and_craftmanship'               THEN 'Arts & Craftsmanship'
                    WHEN 'audio'                               THEN 'Audio'
                    WHEN 'auto'                                THEN 'Auto'
                    WHEN 'baby'                                THEN 'Baby'
                    WHEN 'bed_bath_table'                      THEN 'Bed, Bath & Table'
                    WHEN 'books_general_interest'              THEN 'Books: General Interest'
                    WHEN 'books_imported'                      THEN 'Books: Imported'
                    WHEN 'books_technical'                     THEN 'Books: Technical'
                    WHEN 'cds_dvds_musicals'                   THEN 'CDs, DVDs & Musicals'
                    WHEN 'christmas_supplies'                  THEN 'Christmas Supplies'
                    WHEN 'cine_photo'                          THEN 'Cinema & Photo'
                    WHEN 'computers'                           THEN 'Computers'
                    WHEN 'computers_accessories'               THEN 'Computers & Accessories'
                    WHEN 'consoles_games'                      THEN 'Consoles & Games'
                    WHEN 'construction_tools_construction'     THEN 'Construction Tools'
                    WHEN 'construction_tools_lights'           THEN 'Construction: Lights'
                    WHEN 'construction_tools_safety'           THEN 'Construction: Safety'
                    WHEN 'cool_stuff'                          THEN 'Cool Stuff'
                    WHEN 'costruction_tools_garden'            THEN 'Construction: Garden'
                    WHEN 'costruction_tools_tools'             THEN 'Construction: General'
                    WHEN 'diapers_and_hygiene'                 THEN 'Diapers & Hygiene'
                    WHEN 'drinks'                              THEN 'Drinks'
                    WHEN 'dvds_blu_ray'                        THEN 'DVDs & Blu-Ray'
                    WHEN 'electronics'                         THEN 'Electronics'
                    WHEN 'fashio_female_clothing'              THEN 'Fashion: Female Clothing'
                    WHEN 'fashion_bags_accessories'            THEN 'Fashion: Bags'
                    WHEN 'fashion_childrens_clothes'           THEN 'Fashion: Children'
                    WHEN 'fashion_male_clothing'               THEN 'Fashion: Male Clothing'
                    WHEN 'fashion_shoes'                       THEN 'Fashion: Shoes'
                    WHEN 'fashion_sport'                       THEN 'Fashion: Sport'
                    WHEN 'fashion_underwear_beach'             THEN 'Fashion: Underwear'
                    WHEN 'fixed_telephony'                     THEN 'Fixed Telephony'
                    WHEN 'flowers'                             THEN 'Flowers'
                    WHEN 'food'                                THEN 'Food'
                    WHEN 'food_drink'                          THEN 'Food & Drink'
                    WHEN 'furniture_bedroom'                   THEN 'Furniture: Bedroom'
                    WHEN 'furniture_decor'                     THEN 'Furniture & Decor'
                    WHEN 'furniture_living_room'               THEN 'Furniture: Living Room'
                    WHEN 'furniture_mattress_and_upholstery'   THEN 'Furniture: Mattress'
                    WHEN 'garden_tools'                        THEN 'Garden Tools'
                    WHEN 'health_beauty'                       THEN 'Health & Beauty'
                    WHEN 'home_appliances'                     THEN 'Home Appliances'
                    WHEN 'home_appliances_2'                   THEN 'Home Appliances II'
                    WHEN 'home_comfort_2'                      THEN 'Home Comfort II'
                    WHEN 'home_confort'                        THEN 'Home Comfort'
                    WHEN 'home_construction'                   THEN 'Home Construction'
                    WHEN 'housewares'                          THEN 'Housewares'
                    WHEN 'industry_commerce_and_business'      THEN 'Industry & Commerce'
                    WHEN 'kitchen_dining_laundry_garden_furniture' THEN 'Furniture: Kitchen & Dining'
                    WHEN 'la_cuisine'                          THEN 'La Cuisine'
                    WHEN 'luggage_accessories'                 THEN 'Luggage & Accessories'
                    WHEN 'market_place'                        THEN 'Marketplace'
                    WHEN 'music'                               THEN 'Music'
                    WHEN 'musical_instruments'                 THEN 'Musical Instruments'
                    WHEN 'office_furniture'                    THEN 'Office Furniture'
                    WHEN 'party_supplies'                      THEN 'Party Supplies'
                    WHEN 'pc_gaming'                           THEN 'PC Gaming'
                    WHEN 'pc_gamer'                            THEN 'PC Gaming'
                    WHEN 'portable_kitchen_and_food_processors' THEN 'Portable Kitchen'
                    WHEN 'portateis_cozinha_e_preparadores_de_alimentos' THEN 'Portable Kitchen'
                    WHEN 'perfumery'                           THEN 'Perfumery'
                    WHEN 'pet_shop'                            THEN 'Pet Shop'
                    WHEN 'security_and_services'               THEN 'Security & Services'
                    WHEN 'signaling_and_security'              THEN 'Signaling & Security'
                    WHEN 'small_appliances'                    THEN 'Small Appliances'
                    WHEN 'small_appliances_home_oven_and_coffee' THEN 'Small Appliances: Oven'
                    WHEN 'sports_leisure'                      THEN 'Sports & Leisure'
                    WHEN 'stationery'                          THEN 'Stationery'
                    WHEN 'tablets_printing_image'              THEN 'Tablets & Printing'
                    WHEN 'telephony'                           THEN 'Telephony'
                    WHEN 'toys'                                THEN 'Toys'
                    WHEN 'watches_gifts'                       THEN 'Watches & Gifts'
                    ELSE COALESCE(t.product_category_name_english, p.product_category_name, 'UNKNOWN')
                END                              AS product_category_name_english,
                p.product_name_lenght        AS product_name_length,
                p.product_description_lenght AS product_description_length,
                p.product_photos_qty,
                p.product_weight_g,
                p.product_length_cm,
                p.product_height_cm,
                p.product_width_cm,
                HASHBYTES('SHA2_256', CONCAT(
                    ISNULL(p.product_category_name,                                        'UNKNOWN'), '|',
                    ISNULL(COALESCE(t.product_category_name_english, p.product_category_name, 'UNKNOWN'), 'UNKNOWN'), '|',
                    ISNULL(CAST(p.product_name_lenght        AS NVARCHAR(10)), ''), '|',
                    ISNULL(CAST(p.product_description_lenght AS NVARCHAR(10)), ''), '|',
                    ISNULL(CAST(p.product_photos_qty         AS NVARCHAR(10)), ''), '|',
                    ISNULL(CAST(p.product_weight_g           AS NVARCHAR(10)), ''), '|',
                    ISNULL(CAST(p.product_length_cm          AS NVARCHAR(10)), ''), '|',
                    ISNULL(CAST(p.product_height_cm          AS NVARCHAR(10)), ''), '|',
                    ISNULL(CAST(p.product_width_cm           AS NVARCHAR(10)), '')
                )) AS row_hash
            FROM cleansed.products p
            LEFT JOIN cleansed.product_category_name_translation t
                ON t.product_category_name = p.product_category_name
               AND t.is_deleted = 0
            WHERE p.is_deleted = 0
        )
        MERGE mart.dim_product AS tgt
        USING src
            ON tgt.product_id = src.product_id
        -- Data changed (according to row_hash) or row is reactivating after a soft delete.
        WHEN MATCHED AND (
            tgt.row_hash <> src.row_hash OR tgt.is_deleted = 1
        ) THEN
            UPDATE SET
                product_category_name         = src.product_category_name,
                product_category_name_english = src.product_category_name_english,
                product_name_length           = src.product_name_length,
                product_description_length    = src.product_description_length,
                product_photos_qty            = src.product_photos_qty,
                product_weight_g              = src.product_weight_g,
                product_length_cm             = src.product_length_cm,
                product_height_cm             = src.product_height_cm,
                product_width_cm              = src.product_width_cm,
                is_deleted                    = 0,
                row_hash                      = src.row_hash,
                updated_at                    = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                product_id,                    product_category_name,
                product_category_name_english, product_name_length,
                product_description_length,    product_photos_qty,
                product_weight_g,              product_length_cm,
                product_height_cm,             product_width_cm,
                row_hash
            )
            VALUES (
                src.product_id,                    src.product_category_name,
                src.product_category_name_english, src.product_name_length,
                src.product_description_length,    src.product_photos_qty,
                src.product_weight_g,              src.product_length_cm,
                src.product_height_cm,             src.product_width_cm,
                src.row_hash
            )
        -- The unknown member row (product_key = -1) is excluded from soft deletion.
        WHEN NOT MATCHED BY SOURCE AND tgt.product_key <> -1 THEN
            UPDATE SET
                is_deleted = 1,
                updated_at = SYSUTCDATETIME()
        OUTPUT $action INTO @merge_output;

        SET @rows_inserted = (SELECT COUNT(*) FROM @merge_output WHERE action = 'INSERT');
        SET @rows_updated  = (SELECT COUNT(*) FROM @merge_output WHERE action = 'UPDATE');
        SET @rows          = @rows_inserted + @rows_updated;
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, SYSUTCDATETIME());

        UPDATE audit.load_log
        SET rows_processed        = @rows,
            rows_inserted         = @rows_inserted,
            rows_updated          = @rows_updated,
            rows_deleted          = 0,
            status                = 'SUCCESS',
            processed_duration_ms = @duration_ms
        WHERE log_id = @log_id;

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
        WHERE log_id = @log_id;

        INSERT INTO audit.error_log (
            job_run_id,  pipeline_id, sp_name,
            error_message, error_ts,
            error_severity, error_procedure, error_line
        )
        VALUES (
            @job_run_id, @pipeline_id, 'mart.sp_load_dim_product',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
