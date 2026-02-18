# Snowflake Schema Catalog

`schemas/snowflake_catalog.json` contains object names, columns, and sample rows for all Snowflake objects used by the app.

## Refresh

```bash
python scripts/sf_schema_capture.py
```

Run when:
- New tables/views are added to the app
- Column definitions change in Snowflake
- You want updated sample payloads

## Contents

- `generated_at` – ISO timestamp of last capture
- `objects` – keyed by full object name (e.g. `FROSTY.STAGING.DQ_PTRUN_N_REPORT_03`)
  - `columns` – list of `{name, type, ordinal}`
  - `sample` – up to 2 sample rows (JSON-serializable)
