USE OlistDWH;
GO

-- DDL: cleansed schema tables

CREATE TABLE cleansed.customers (
    customer_id              NVARCHAR(32)  NOT NULL,
    customer_unique_id       NVARCHAR(32)  NOT NULL,
    customer_zip_code_prefix CHAR(5)       NOT NULL,
    customer_city            NVARCHAR(100) NOT NULL,
    customer_state           CHAR(2)       NOT NULL,
    row_hash                 BINARY(32)    NOT NULL,
    is_deleted               BIT           NOT NULL DEFAULT 0,
    deleted_at               DATETIME2(3)  NULL,
    updated_at               DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_cleansed_customers PRIMARY KEY (customer_id)
);
GO

CREATE TABLE cleansed.orders (
    order_id                       NVARCHAR(32)  NOT NULL,
    customer_id                    NVARCHAR(32)  NOT NULL,
    order_status                   NVARCHAR(25)  NOT NULL,
    order_purchase_timestamp       DATETIME2(0)  NOT NULL,
    order_approved_at              DATETIME2(0)  NULL,
    order_delivered_carrier_date   DATETIME2(0)  NULL,
    order_delivered_customer_date  DATETIME2(0)  NULL,
    order_estimated_delivery_date  DATETIME2(0)  NOT NULL,
    row_hash                       BINARY(32)    NOT NULL,
    is_deleted                     BIT           NOT NULL DEFAULT 0,
    deleted_at                     DATETIME2(3)  NULL,
    updated_at                     DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_cleansed_orders PRIMARY KEY (order_id)
);
GO

CREATE TABLE cleansed.order_items (
    order_id             NVARCHAR(32)   NOT NULL,
    order_item_id        NVARCHAR(25)   NOT NULL,
    product_id           NVARCHAR(32)   NOT NULL,
    seller_id            NVARCHAR(32)   NOT NULL,
    shipping_limit_date  DATETIME2(0)   NOT NULL,
    price                DECIMAL(10,2)  NOT NULL,
    freight_value        DECIMAL(10,2)  NOT NULL,
    row_hash             BINARY(32)     NOT NULL,
    is_deleted           BIT            NOT NULL DEFAULT 0,
    deleted_at           DATETIME2(3)   NULL,
    updated_at           DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_cleansed_order_items PRIMARY KEY (order_id, order_item_id)
);
GO
