USE OlistDWH;
GO

-- Migration: V005
-- Description: Update pipeline_config.file_path and file_name for geolocation and
--              order_reviews to point to the pipe-delimited files produced by preprocess_all.ps1.
--              raw.sp_load_geolocation and raw.sp_load_order_reviews now expect FIELDTERMINATOR='|'
--              to avoid parsing errors caused by embedded commas in city names (geolocation)
--              and free-text review comments (order_reviews).
-- Applied: manually in SSMS

DECLARE @DatasetRoot NVARCHAR(500);
SELECT @DatasetRoot = LEFT(file_path, LEN(file_path) - LEN(file_name))
FROM orchestration.pipeline_config
WHERE table_name = 'orders' AND layer = 'RAW';

UPDATE orchestration.pipeline_config
SET file_path  = @DatasetRoot + 'olist_geolocation_dataset_pipe.csv',
    file_name  = 'olist_geolocation_dataset_pipe.csv'
WHERE table_name = 'geolocation'
  AND layer      = 'RAW';

UPDATE orchestration.pipeline_config
SET file_path  = @DatasetRoot + 'olist_order_reviews_dataset_pipe.csv',
    file_name  = 'olist_order_reviews_dataset_pipe.csv'
WHERE table_name = 'order_reviews'
  AND layer      = 'RAW';
GO
