USE OlistDWH;
GO

-- Migration: V006
-- Description: Refactor audit.dq_log from row-level to aggregated DQ logging.
--              1. Add affected_row_count INT NULL — one row per (column_name, issue)
--                 category per batch, with COUNT(*) of affected source rows.
--              2. Drop row-level trace columns raw_key, raw_value, raw_row_id —
--                 no longer populated by any SP after the cleansed layer redeployment.
--              All cleansed SPs were redeployed before this migration was applied.
-- Applied: manually in SSMS

ALTER TABLE audit.dq_log
    ADD affected_row_count INT NULL;
GO

ALTER TABLE audit.dq_log DROP COLUMN raw_key;
ALTER TABLE audit.dq_log DROP COLUMN raw_value;
ALTER TABLE audit.dq_log DROP COLUMN raw_row_id;
GO
