# dashapp
Plotly Dash - Columbia Fruit Analytics

## Setup

### Schema configuration

The app queries dbt mart views. dbt creates them in `FROSTY.DBT_DEV_DBT_DEV` (target schema + custom schema). Ensure your `.env` includes:

```
DBT_SCHEMA=DBT_DEV_DBT_DEV
```

If this is missing, the app will default to `DBT_DEV` and you’ll see errors like:
`Object 'FROSTY.DBT_DEV.INV_ON_HAND_PIVOT' does not exist or not authorized.`

### Prerequisites

1. Run dbt to create mart views: `dbt run` (from the `dbt/` folder)
2. For Pallet Inventory: run `scripts/snowflake/create_dt_inv_on_hand_sku_grain.sql` to create the DT source

## Development standards

The single source of truth for how to build new Dash pages and backend/query logic is the **Cursor rules** in `.cursor/rules/`.

- **001-core-principles.mdc** — Project-wide principles and reference to the golden backend pattern.
- **002-golden-backend-pattern.mdc** — Backend and query pattern template for every new report (schema, queries, caching, callbacks, Snowflake service).

When adding a new page or report, follow these rules. The golden reference implementation is the Production Intra Day KPIs page: `pages/production_intra_day_kpis.py`, `services/pidk_data.py`, `callbacks/pidk.py`.

**After adding or changing rules:** Reload the Cursor window (e.g. Developer: Reload Window) so rules are picked up. To verify, open a new Composer/Agent chat and run a test prompt (e.g. “Add a new report page that shows X from Snowflake”); suggestions should align with the golden backend pattern (data module, query layer, cache, payload builder).

## DBT Commands
Agents/future: Use these (loads .env auto via run_dbt.ps1):

From root:
```
.\venv_dbt\Scripts\Activate.ps1
.\scripts\run_dbt.ps1 run  # All models
.\scripts\run_dbt.ps1 run --select +marts.inventory  # Pallet
```

From dbt/:
```
dbt run --profiles-dir .
```

`.env` hidden from agents (ignored file). Edit manually.
