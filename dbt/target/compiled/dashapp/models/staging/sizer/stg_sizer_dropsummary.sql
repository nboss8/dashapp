-- Staging: sizer drop summary by grade/size - DQ_APPLE_SIZER_DROPSUMMARY_03


select
    "EventId" as event_id,
    "GradeName" as grade_name,
    "SizeName" as size_name,
    coalesce(nullif(trim("PACKOUT_GROUP"), ''), 'Unclassified') as packout_group,
    coalesce("weight_dec", "WEIGHT", 0)::number(18,2) as weight_dec,
    "WEIGHT" as weight,
    "DropSummaryId",
    "ProductName",
    "QualityName",
    current_timestamp() as dbt_loaded_at
from FROSTY.STAGING.DQ_APPLE_SIZER_DROPSUMMARY_03