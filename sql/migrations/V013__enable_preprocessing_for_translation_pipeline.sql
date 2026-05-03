USE OlistDWH;
GO

-- Migration V013: Enable preprocessing for product_category_name_translation pipeline.
-- Description: The CSV has no quoted fields, so the last column retains a trailing \r from
--              Windows CRLF line endings on each row. Preprocessing strips \r and converts
--              to pipe-delimited output, eliminating the trailing \r before the RAW load.
-- Applied: manually in SSMS

UPDATE orchestration.pipeline_config
SET needs_preprocessing = 1,
    file_name = 'product_category_name_translation_pipe.csv',
    file_path = 'D:\Code\VCS Projects\olist-ecommerce-dwh\data\product_category_name_translation_pipe.csv'
WHERE pipeline_id = 1029;
GO
