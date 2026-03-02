# Columbia Fruit Analytics — Schema Reference

Shared reference for keys, identifiers, and how data flows. Use this so we stay aligned.

---

## Two Key Systems

Your app uses **two different run identifiers** depending on the context:

| Context | Key | Format | Example |
|--------|-----|--------|---------|
| **PIDK / Intra-day** | `RUN_KEY` | `{date}-{shift}-{lot}` | `2026-02-21-1-0007` |
| **PIDK / Intra-day** | `PACKDATE_RUN_KEY` | `{date}-{shift}` | `2026-02-21-1` |
| **PFR / Finalized** | `UNIQUE_RUN_KEY` | Compact (date+shift+line+grower) | `2026021152CC1157` |

---

## PIDK (Production Intra Day KPIs)

**Sources:** FROSTY.STAGING (dynamic tables)
- `DQ_PTRUN_N_REPORT_03` — day labels, run keys, shift, lot
- `VW_RUN_TOTALS_FAST_03` — bins, BPH, PPMH per run
- `VW_SHIFT_TOTALS_FAST_03` — shift-level totals
- `DT_SHIFT_10MIN_KPI_A_PER_RUN03_DT` — 10‑min bucket KPIs
- `DQ_APPLE_SIZER_HEADER_VIEW_03` — sizer batch/events
- `DQ_APPLE_SIZER_DROPSUMMARY_03` — sizer drop by grade/size
- `DQ_EQ_WITH_KEYS03` — EQ cartons with run keys
- `PACK_CLASSIFICATION` — pack abbreviation → classification
- `VW_LOT_DUMPER_TIME_03` — lot dumper time, IS_CURRENT_LOT

**Key fields (intra-day):**
- `RUN_KEY` — Unique per row in `DQ_PTRUN_N_REPORT_03`. Format: `{date}-{shift}-{grower_number}` (e.g. `2026-02-21-1-0007`). When a grower runs multiple times in a shift, those run sequences are aggregated into one row; the `RUNS` column lists them (e.g. `"3,4"`).
- `PACKDATE_RUN_KEY` — One shift on one day. Format: `{date}-{shift}` (e.g. `2026-02-21-1`). Multiple lots share the same PACKDATE_RUN_KEY.
- `grower_number` — Lot identifier (e.g. `0007`, `4450`)
- `day_label` — Display date (e.g. `TODAY`, `2026-02-21`)

**Relationships:**
- `pidk_run_totals`: join ptrun + run_totals on `packdate_run_key` + `grower_number` → one row per RUN_KEY (unique)
- `pidk_shift_totals`: one row per `packdate_run_key` + `shift`
- Sizer / EQ data link via `RUN_KEY` or `PACKDATE_RUN_KEY`

---

## PFR (Production Finalized Report)

**Sources:** FROSTY.APP
- `POWERBI_PRODUCTION_HEADER_MAT` — finalized run metadata
- `PTRUN_SIZER_DROP_SNAPSHOT`, `PTRUN_SIZER_PACKED`, etc.

**Key fields (finalized):**
- `UNIQUE_RUN_KEY` — Compact format (e.g. `2026021152CC1157`)
- `RUN_DATE`, `GROWER`, `VARIETY_USER_CD`, `POOL` — filter dimensions
- `SHIFT`, `RUN_NUMBER` — ordering

---

## Pallet Inventory

**Page:** `/production/pallet-inventory`

**Sources:**
- `DT_INV_ON_HAND_SKU_GRAIN` — Pre-aggregated on-hand at SKU x week grain (dynamic table, target_lag 15 min dev / 5 min prod). Run `scripts/snowflake/create_dt_inv_on_hand_sku_grain.sql` to create.
- `DQ_EQ_WITH_KEYS03` — Used by `inv_changes_today` for packed/shipped/staged today

**dbt marts:**
- `inv_on_hand_pivot` — Thin wrapper over DT; pivot and SKU detail both query this
- `inv_changes_today` — Packed, Shipped, Staged row-level data for today (filter-scoped)

**Key fields:**
- `PACK_DATE_FORMATTED` — Pack date
- `LAST_UPDATE_2` — Ship date (ignore `_LAST_UPDATE`)
- `FINAL_STAGE_STATUS` — ON HAND vs STAGED
- `WEEK_BUCKET` — Age in weeks (0, 1, 2, ..., 10+)
- `GROUP_CATEGORY` — AP, OA, CH, OC

---

## dbt Model Layers

```
FROSTY.STAGING / FROSTY.APP (sources, read-only)
         ↓
staging/* (ephemeral: renames, casts, dbt_loaded_at)
         ↓
marts/core, marts/pidk, marts/tv, marts/pfr, marts/inventory
         ↓
FROSTY.DBT_DEV (views)
         ↓
Dash app (SELECT from DBT_DEV)
```

---

## Open Questions

- **PACKDATE_RUN_KEY vs shift** — Is `PACKDATE_RUN_KEY` always one-to-one with (date, shift)?
- **Sizer / EQ linking** — Sizer events link via `run_key = shift_key` or `shift_key LIKE packdate_run_key || '%'`. Confirm intended join logic?

---

*Last updated from dbt models, snowflake_catalog.json, and DQ_PTRUN_N_REPORT_03 definition*
