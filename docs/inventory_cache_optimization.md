# Pallet Inventory Cache Optimization

## 1. Filters / Slicers / Dropdowns

| Component | Dash ID | Column Filtered | Typical Options | Cache Key (Current) | Cache Key (Proposed) |
|-----------|---------|-----------------|-----------------|---------------------|----------------------|
| Group | `dcc.Dropdown(id="inv-filter-group")` | GROUP_CATEGORY | 2–5 (AP, OA, CH, OC, etc.) | ✅ In key | ✅ In key |
| Variety | `dcc.Dropdown(id="inv-filter-variety")` | VARIETY_ABBR | 10–50 | ❌ Excluded | ❌ In-memory |
| Pack | `dcc.Dropdown(id="inv-filter-pack")` | PACK_ABBR | 5–20 | ✅ In key | ❌ In-memory |
| Grade | `dcc.Dropdown(id="inv-filter-grade")` | GRADE_ABBR | 3–15 | ✅ In key | ❌ In-memory |
| Size | `dcc.Dropdown(id="inv-filter-size")` | SIZE_ABBR | 5–15 | ✅ In key | ❌ In-memory |
| Stage | `dcc.Dropdown(id="inv-filter-stage")` | FINAL_STAGE_STATUS | 3–10 | ✅ In key | ✅ In key |
| Metric | `dcc.RadioItems(id="inv-metric-toggle")` | — (Cartons vs EQs) | 2 | ✅ In key | ✅ In key |
| Pivot cell click | `Button(id={"type":"inv-pivot-cell", ...})` | variety + week_bucket | — | ❌ Excluded | ❌ In-memory |

**Hidden in UI:** pool, process_code (in store but no dropdowns).

---

## 2. Current Callback Structure

```
inv-interval ─┐
inv-filters-store ─┬─► _update_inventory ─► pivot_table, sku_table, pagination, Packed/Shipped/Staged
inv-metric-toggle ─┤
inv-sku-page ─────┘
```

- **inv-filters-store** changes whenever any of the 6 dropdowns change → callback runs.
- **inv_cache_identifier(base_filters, use_eq)** uses base_filters = filters minus (week_bucket, variety).
- So changing group, pack, grade, size, or stage → **cache MISS** → Snowflake rebuild.
- Changing variety or week_bucket (pivot click) → cache HIT → in-memory filter on sku_all_df only.

---

## 3. Recommended Coarse vs Fine Split

### Coarse (in cache key, ~10–50 payloads)
- **group_category** — few values, cuts by business unit, changes rarely
- **final_stage_status (stage)** — few values, cuts by workflow stage
- **use_eq** — cartons vs eqs (2 values)

Estimate: 4 groups × 5 stages × 2 metrics = **40 cache keys** (worst case). Often fewer due to empty combos.

### Fine (in-memory pandas on cached payload)
- variety, pack, grade, size, week_bucket (pivot-cell)

Reasoning: these have many options and change often. Filtering in memory on `sku_all_df` (and `changes_detail_df`) is sub-200 ms.

---

## 4. Payload Extension

- **sku_all_df**: Add `pack_abbr`, `grade_abbr`, `size_abbr` (and keep variety_abbr, week_bucket) for in-memory filtering.
- **changes_detail_df**: New field — raw rows (change_type, pack_abbr, grade_abbr, size_abbr, variety_abbr, cartons, eq_on_hand) so we aggregate after filtering in memory.
- **pivot_df**: Keep as Variety × Week; derive from filtered sku_all_df in callback (or keep pre-aggregated with coarse filters and recompute from sku_all_df when fine filters apply — simpler to always derive pivot from filtered sku_all_df for consistency).

---

## 5. Trade-offs

| Trade-off | Detail |
|-----------|--------|
| Bigger payloads | sku_all_df includes pack/grade/size → more columns. changes_detail has detail rows. Slight memory increase. |
| 5k row cap | If base (group+stage) returns >5k SKU×week rows, we cap. Fine filtering happens on capped set. |
| Changes accuracy | changes_detail filtered in memory = correct Packed/Shipped/Staged for all slicers. |

---

## 6. Implementation Summary

**inventory_data.py**
- `IN_CACHE_KEY_DIMS` = (group_category, final_stage_status)
- `IN_MEMORY_FILTER_DIMS` = (variety, week_bucket, pack, grade, size)
- `get_sku_all_by_week`: adds PACK_ABBR, GRADE_ABBR, SIZE_ABBR; uses base filters only
- `get_changes_today_detail`: returns detail rows for in-memory aggregation
- `build_inv_payload`: returns `changes_detail_df` + `sku_all_df` (no pivot_df/total)
- `_apply_fine_filters_to_df`, `derive_changes_from_detail`: helpers for callback

**callbacks/inventory.py**
- `_update_inventory`: uses `_base_filters_only(f)` for cache key; applies fine filters in-memory
- `effective_payload`: built from filtered sku_all_df and changes_detail_df

**register_report** (unchanged)
- `historical_refresh_keys=["default", "default_eq"]` — refreshes most common keys
- Other keys (group+stage combos) built on first access

---

## 7. Test Plan

1. Load page with defaults → verify pivot + SKU table + Packed/Shipped/Staged.
2. Change variety → <200 ms, no Snowflake.
3. Change pack, grade, size → <200 ms (in-memory).
4. Change stage or group → cache miss on first load of that combo, then <200 ms.
5. Click pivot cell → filters to variety+week, instant.
6. Toggle Cartons/EQs → instant (cache hit on default_eq).
7. Export Pivot CSV / Export SKU CSV → respects filters (hits Snowflake for export).
8. Clear cache and reload → verify prewarm rebuilds default + default_eq.
