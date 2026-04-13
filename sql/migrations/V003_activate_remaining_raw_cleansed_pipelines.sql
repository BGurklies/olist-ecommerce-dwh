USE OlistDWH;
GO

-- Migration: V003
-- Description: Activate pipelines for the remaining 7 entities (RAW + CLEANSED). 
-- Applied: manually in SSMS

UPDATE orchestration.pipeline_config
SET    is_active = 1
WHERE layer <> 'MART';
GO
