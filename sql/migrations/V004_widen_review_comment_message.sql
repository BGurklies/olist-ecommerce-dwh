USE OlistDWH;
GO

-- Migration: V004
-- Description: Widen raw.order_reviews.review_comment_message from NVARCHAR(255)
--              to NVARCHAR(MAX). Review comments can exceed 255 characters; the
--              prior size risked silent truncation or insert failures.
-- Applied: manually in SSMS

ALTER TABLE raw.order_reviews
    ALTER COLUMN review_comment_message NVARCHAR(MAX);
GO
