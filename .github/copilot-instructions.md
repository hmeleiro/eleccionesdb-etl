# GitHub Copilot Instructions for `eleccionesdb-etl`

These instructions are for AI coding agents working in this repo.
Keep them short, concrete, and specific to this project.

## Project Overview

- This is an R project that builds a PostgreSQL election database (`eleccionesdb`) from CSV/RDS source files.
- The workflow is **ETL-style** orchestrated with [{targets}](https://docs.ropensci.org/targets/):
  - **Raw data** in `data-raw/` (CSV + RDS; INE/Infoelectoral codifications).
  - **Processed intermediates** in `data-processed/` (RDS, e.g. `data-processed/hechos/01-andalucia/*.rds`).
  - **Final tabular outputs** in `tablas-finales/` (CSV and RDS ready to load into Postgres).
- Pipeline orchestration: `_targets.R` defines the DAG, `R/pipeline_helpers.R` wraps scripts, `run.R` provides convenience functions.
- Database loading is done by `R/03-writedb/write-db.R`, which connects to Postgres and uses `DBI::dbWriteTable`.

## Key Files and Responsibilities

- `_targets.R`: {targets} pipeline definition with 23 targets across 5 phases.
- `R/pipeline_helpers.R`: wrapper functions that `source()` each script and return output paths.
- `run.R`: entry point with `run_all()`, `run_dims()`, `run_hechos()`, `run_bind()`, `run_writedb()`, `run_export()`.
- `new-eleccionesdb.Rproj`: RStudio project; assume R working directory is the repo root when running scripts.
- `R/utils.R`: defines `connect()` which reads DB credentials from `.env` and returns a `RPostgreSQL` connection. **Always use this helper instead of creating new connections directly.**
- `R/db_schema.sql`: DDL for the target Postgres schema; align column names and types in generated tables with this file.
- `R/01-generate-data/dimensiones/`:
  - `elecciones/fechas-elecciones-format.R`: reads `data-raw/fechas_elecciones.csv`, normalizes Spanish dates, assigns `codigo_ccaa`, and writes `tablas-finales/dimensiones/elecciones`.
  - `elecciones/elecciones-fuentes-format.R`: maps election metadata to sources.
  - `territorios/territorios.R`: builds the hierarchical territorial dimension.
  - `partidos/sync-partidos.R`: builds party dimensions from `data-raw/partidos_recodes.xlsx`.
- `R/01-generate-data/hechos/*/format.R`: one format script per region, all named `format.R`. Each reads from `data-raw/hechos/<region>/`, joins with dimensions, validates, and writes `data-processed/hechos/<region>/{info.rds, votos.rds}`.
- `R/02-clean-and-bind/02-bind-hechos.R`: binds all regional hechos, assigns `partido_id`, validates, writes `tablas-finales/hechos/{info.rds, votos.rds}`.
- `R/03-writedb/write-db.R`: truncates and reloads all Postgres tables from `tablas-finales/`.
- `R/04-export/export-descargas.R`: exports to Parquet, SQLite, and CSV in `descargas/`.

## Data & Naming Conventions

- Territorial codes follow INE/Infoelectoral patterns: `codigo_ccaa`, `codigo_provincia`, `codigo_municipio`, `codigo_distrito`, `codigo_seccion` plus a concatenated `codigo_completo`.
- Hierarchies are encoded using `id` / `parent_id` numeric keys (`territorios.R` is the canonical reference for this pattern).
- Election types are encoded as single-character codes:
  - `G` = generales, `A` = autonomicas, `L` = locales, `E` = europeas.
- `fechas-elecciones-format.R` shows how to normalize free-text Spanish dates (e.g., fix typos, convert to `Date`, derive `year`, `mes`, `dia`, `slug`, and human-readable `descripcion`). Follow this pattern for any new date-like metadata.
- When reading CSVs with `readr::read_csv`, the project often suppresses column type messages using `show_col_types = FALSE` and uses bare filenames in `tablas-finales/dimensiones/` without extensions.

## DB & Environment Expectations

- DB connection parameters are read from `.env` via `dotenv::load_dot_env()` inside `connect()`:
  - `DB_NAME`, `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`.
- Assume Postgres and the target schema already exist and match `R/db_schema.sql`.
- When modifying load scripts, keep the following pattern:
  - `con <- connect()`
  - Optional `dbExecute(con, "TRUNCATE ... RESTART IDENTITY")` for full reloads.
  - `tryCatch({ dbWriteTable(...) }, error = function(e) { message(...) }, finally = dbDisconnect(con))`.

## How to Extend the Pipeline

When adding new data flows (e.g., a new region or dimension):

1. **Raw ingestion**: Place new input files under `data-raw/` or generate RDS/CSV there.
2. **Transformation**: Create `R/01-generate-data/hechos/<region>/format.R` that:
   - Reads from `data-raw/hechos/<region>/`.
   - Joins with `tablas-finales/dimensiones/{elecciones, territorios}`.
   - Sources `R/tests/validate_data_processed.R` and calls `validate_info()`, `validate_votos()`, `validate_info_votos_consistency()`.
   - Writes `data-processed/hechos/<region>/{info.rds, votos.rds}`.
3. **Register in pipeline**: Add the region to `hechos_regions` in `_targets.R`.
4. **DB load**: No changes needed — `R/03-writedb/write-db.R` and `R/02-clean-and-bind/02-bind-hechos.R` auto-discover all regions via `list.files("data-processed/")`.

## Agent-Specific Guidance

- Assume execution from the repo root; use relative paths consistent with existing scripts.
- Reuse existing helpers and patterns (especially `connect()` and the `tryCatch` around `dbWriteTable`) instead of inventing new abstractions.
- Prefer `dplyr` verbs and `readr`/`stringr` over base R where existing code does so.
- Do **not** commit or rely on real DB credentials; `.env` should remain untracked.
- Before changing schemas or table column sets, inspect and align with `R/db_schema.sql` and all scripts that read/write those tables.
- All regional format scripts must be named `format.R`.
