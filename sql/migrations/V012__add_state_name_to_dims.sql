USE OlistDWH;
GO

-- Migration V012: Add state_name columns to dim_customer and dim_seller.
-- Description: Brazilian state codes (CHAR(2)) are resolved to full Portuguese state names
--              in the SP src CTE.
-- Applied: manually in SSMS

ALTER TABLE mart.dim_customer
    ADD customer_state_name NVARCHAR(50) NOT NULL DEFAULT 'Unknown';
GO

ALTER TABLE mart.dim_seller
    ADD seller_state_name NVARCHAR(50) NOT NULL DEFAULT 'Unknown';
GO
