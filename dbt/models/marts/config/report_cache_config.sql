{{ config(materialized='table') }}

WITH config AS (
    SELECT 'tv' AS "report_slug", 'today' AS "period", 300::NUMBER AS "refresh_seconds"
    UNION ALL SELECT 'tv', 'yesterday', 86400
    UNION ALL SELECT 'yolo', 'today', 900  -- 15 min
    -- Add more:
UNION ALL SELECT 'pidk', 'today', 300
UNION ALL SELECT 'pidk', 'historical', 86400
UNION ALL SELECT 'grower_bins', 'today', 1800  -- 30 min
)
SELECT * FROM config
