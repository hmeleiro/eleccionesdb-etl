# GitHub Copilot Instructions for `new-eleccionesdb`

These instructions are for AI coding agents working in this repo.
Keep them short, concrete, and specific to this project.

## Project Overview

- This is an R project that builds a PostgreSQL election database (`eleccionesdb`) from CSV/RDS source files.
- The workflow is **ETL-style** in three main phases:
  - **Raw data** in `data-raw/` (CSV + RDS; INE/Infoelectoral codifications).
  - **Processed intermediates** in `data-processed/` (RDS, e.g. `data-processed/00-congreso/*.rds`).
  - **Final tabular outputs** in `tablas-finales/` (CSV and RDS ready to load into Postgres).
- Database loading is done by the scripts in `write-to-db/`, which connect to Postgres and use `DBI::dbWriteTable`.

## Key Files and Responsibilities

- `new-eleccionesdb.Rproj`: RStudio project; assume R working directory is the repo root when running scripts.
- `code/utils.R`: defines `connect()` which reads DB credentials from `.env` and returns a `RPostgreSQL` connection. **Always use this helper instead of creating new connections directly.**
- `code/db_schema.sql`: DDL for the target Postgres schema; align column names and types in generated tables with this file.
- `code/00-congreso/`:
  - `sauron_formats.R` sources `format_provincias.R`, `format_municipios.R`, `format_secciones.R`. These scripts transform raw INE/Infoelectoral data into standardized territorial formats.
- `code/dimensiones/`:
  - `elecciones/fechas-elecciones-scrap.R`: (if present) scraping/ingestion of election date metadata into `data-raw/`.
  - `elecciones/fechas-elecciones-format.R`: reads `data-raw/fechas_elecciones.csv`, normalizes Spanish dates, assigns `codigo_ccaa`, and writes `tablas-finales/dimensiones/elecciones`.
  - `territorios/territorios.R`: builds the hierarchical territorial dimension (`ccaa`, `provincia`, `municipio`, `distrito`, `seccion`) from `data-raw/codigos_secciones*.rds` and `data-raw/nombres_municipios.csv` and writes `tablas-finales/dimensiones/territorios`.
  - `partidos/*.R`: build party-related dimensions into `tablas-finales/dimensiones/partidos`.
- `code/bind-data/` and `code/dimensiones/*`: scripts that combine processed data into final dimension and fact tables (`tablas-finales/`). Follow their patterns for any new election types.
- `write-to-db/writedb-dimensiones.R`:
  - Loads `DBI`, `RPostgreSQL`, `dotenv`, `readr` and `source("code/utils.R")`.
  - Reads `tablas-finales/dimensiones/{tipos_eleccion,elecciones,territorios,partidos}` via `readr::read_csv`.
  - Uses `connect()` and `dbWriteTable(..., append = TRUE)` to load into Postgres.
- `write-to-db/writedb-hechos.R`:
  - Reads `tablas-finales/hechos/{info.rds,votos.rds}` via `readRDS`.
  - Truncates `resumen_territorial` and `votos_territoriales`, then writes tables with `dbWriteTable`.

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
- Assume Postgres and the target schema already exist and match `code/db_schema.sql`.
- When modifying load scripts, keep the following pattern:
  - `con <- connect()`
  - Optional `dbExecute(con, "TRUNCATE ... RESTART IDENTITY")` for full reloads.
  - `tryCatch({ dbWriteTable(...) }, error = function(e) { message(...) }, finally = dbDisconnect(con))`.

## How to Extend the Pipeline

When adding new data flows (e.g., a new dimension or fact table):

1. **Raw ingestion**: Place new input files under `data-raw/` or generate RDS/CSV there.
2. **Transformation**: Create a script under `code/dimensiones/<domain>/` or `code/bind-data/` that:
   - Reads from `data-raw/` or `data-processed/`.
   - Uses `dplyr` pipes, `mutate`, `case_when`, `group_by`/`summarise`, and `stringr::str_*` helpers as seen in `territorios.R` and `fechas-elecciones-format.R`.
   - Writes final output to `tablas-finales/dimensiones/` or `tablas-finales/hechos/` using `readr::write_csv` or `saveRDS`.
3. **DB load**: Update `write-to-db/writedb-dimensiones.R` or `write-to-db/writedb-hechos.R` to read the new file(s) and `dbWriteTable` into appropriate Postgres tables.

## Agent-Specific Guidance

- Assume execution from the repo root; use relative paths consistent with existing scripts.
- Reuse existing helpers and patterns (especially `connect()` and the `tryCatch` around `dbWriteTable`) instead of inventing new abstractions.
- Prefer `dplyr` verbs and `readr`/`stringr` over base R where existing code does so.
- Do **not** commit or rely on real DB credentials; `.env` should remain untracked.
- Before changing schemas or table column sets, inspect and align with `code/db_schema.sql` and all scripts that read/write those tables.
