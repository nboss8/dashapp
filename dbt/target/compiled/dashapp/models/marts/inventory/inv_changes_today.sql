-- Row-level changes for today. App filters by GROUP_CATEGORY, VARIETY, etc. and aggregates.
-- Packed: P/R with PACK_DATE_FORMATTED = today
-- Shipped: S with LAST_UPDATE_2 (ship date) = today
-- Staged: FINAL_STAGE_STATUS = 'STAGED' and PACK_DATE_FORMATTED = today
--   (Staged = current STAGED status on pallets packed today; no event timestamp yet)


select
  case
    when PALLET_TR_CODE in ('P', 'R') and PACK_DATE_FORMATTED = current_date then 'Packed'
    when PALLET_TR_CODE = 'S' and TRY_TO_DATE(LAST_UPDATE_2, 'YYYY-MM-DD') = current_date then 'Shipped'
    when FINAL_STAGE_STATUS = 'STAGED' and PACK_DATE_FORMATTED = current_date then 'Staged'
  end as change_type,
  GROUP_CATEGORY,
  TRIM(VARIETY_ABBR) as VARIETY_ABBR,
  SKU,
  TRIM(PACK_ABBR) as PACK_ABBR,
  TRIM(GRADE_ABBR) as GRADE_ABBR,
  TRIM(SIZE_ABBR) as SIZE_ABBR,
  TRIM(POOL) as POOL,
  "Process Code",
  FINAL_STAGE_STATUS,
  CARTONS,
  EQ_ON_HAND
from FROSTY.STAGING.DQ_EQ_WITH_KEYS03
where GROUP_CATEGORY in ('AP', 'OA', 'CH', 'OC')
  and (
    (PALLET_TR_CODE in ('P', 'R') and PACK_DATE_FORMATTED = current_date)
    or (PALLET_TR_CODE = 'S' and TRY_TO_DATE(LAST_UPDATE_2, 'YYYY-MM-DD') = current_date)
    or (FINAL_STAGE_STATUS = 'STAGED' and PACK_DATE_FORMATTED = current_date)
  )