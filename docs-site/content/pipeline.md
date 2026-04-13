---
title: "Arquitectura del pipeline"
description: "Flujo ETL completo de eleccionesdb"
---

## Visión general

El proyecto sigue un pipeline ETL en cuatro fases, implementado íntegramente en R. El flujo transforma datos electorales en bruto procedentes de múltiples fuentes heterogéneas (CSV, Excel, `.px`, APIs) en una base de datos relacional PostgreSQL normalizada y en formatos descargables (Parquet, SQLite, CSV).

<div class="mermaid">
flowchart LR
  A["📁 data-raw/\n(Datos en bruto)"] --> B["⚙️ 01-generate-data/\n(Generación)"]
  B --> C["📂 data-processed/\n(Intermedios)"]
  C --> D["🔗 02-clean-and-bind/\n(Limpieza y unión)"]
  D --> E["📋 tablas-finales/\n(Tablas listas)"]
  E --> F["🗄️ 03-writedb/\n(Carga a BD)"]
  F --> G["🐘 PostgreSQL"]
  E --> H["📦 04-export/\n(Exportación)"]
  H --> I["💾 descargas/\n(Parquet, SQLite, CSV)"]
</div>

## Fase 1: Datos en bruto (`data-raw/`)

Contiene los ficheros originales tal como se obtienen de cada fuente: CSV, RDS, Excel (`.xls`/`.xlsx`), ficheros estadísticos PX-WEB (`.px`) y datos descargados vía API. Cada comunidad autónoma tiene su propia carpeta en `data-raw/hechos/`.

Además incluye:

- `fechas_elecciones.csv` — Fechas de todas las convocatorias electorales.
- `codigos_territorios/` — Códigos de secciones censales (INE/Infoelectoral), circunscripciones sub-provinciales y correspondencias municipio–circunscripción.
- `nombres_municipios.csv` — Nomenclátor de municipios del INE.
- `representantes/` — Escaños asignados y representantes electos por provincia y municipio.
- `partidos_recodes.xlsx` — Tabla maestra de recodificación/agrupación de partidos.

<div class="callout callout-tip">
<strong>¿Quieres reproducir el ETL completo?</strong>
<p>Consulta cómo poblar <code>data-raw/</code> en la guía: <a href="../restaurar-data-raw/">Restaurar data-raw/</a></p>
</div>

## Fase 2: Generación de datos intermedios (`R/01-generate-data/`)

Transforma los datos en bruto en tablas intermedias estandarizadas (formato RDS) en `data-processed/`.

### Dimensiones

| Script | Salida | Descripción |
|--------|--------|-------------|
| `dimensiones/elecciones/fechas-elecciones-format.R` | `tablas-finales/dimensiones/elecciones` | Normaliza fechas en español, codifica tipo de elección, genera slug |
| `dimensiones/territorios/territorios.R` | `tablas-finales/dimensiones/territorios` | Construye jerarquía territorial (CCAA → provincia → circunscripción → municipio → distrito → sección) con `parent_id` |
| `dimensiones/partidos/sync-partidos.R` | `tablas-finales/dimensiones/partidos`, `partidos_recode` | Sincroniza partidos desde `partidos_recodes.xlsx` |
| `dimensiones/partidos/new-partidos.R` | `data-processed/partidos_recodes_pending.xlsx` | Detecta partidos nuevos sin recodificación asignada |

### Hechos (por comunidad autónoma)

Cada subcarpeta de `R/01-generate-data/hechos/` contiene scripts específicos que generan dos ficheros RDS por autonomía en `data-processed/hechos/`:

- `info.rds` — Resumen territorial: censo, participación, votos válidos, blancos, nulos.
- `votos.rds` — Votos por partido y territorio.

| Carpeta | Ámbito | Scripts principales |
|---------|--------|---------------------|
| `00-congreso/` | Congreso (nacional) | `sauron_formats.R` → `format_provincias.R`, `format_municipios.R`, `format_secciones.R` |
| `01-andalucia/` | Andalucía | `00_fetch-nomenclators.R` → `01-06_fetch-*.R` → `07-format-data.R` |
| `02-aragon/` | Aragón | `01_format-info.R`, `02_format-votos.R` |
| `03-asturias/` | Asturias | `00_get-nomenclator-sadei.R`, `01_format-data.R` |
| `04-baleares/` | Baleares | `format.R` |
| `05-canarias/` | Canarias | `00_download-data-parcan.R`, `format.R` |
| `07-cyl/` | Castilla y León | `format.R` |
| `09-catalunya/` | Cataluña | `01_format-data.R` |
| `10-comunidad-valenciana/` | C. Valenciana | `format.R` |
| `13-comunidad-madrid/` | C. de Madrid | `format.R` |

## Fase 3: Limpieza y unión (`R/02-clean-and-bind/`)

Une todos los datos intermedios de las distintas autonomías en las tablas finales del modelo relacional.

### `01-partidos-sin-id.R`
Detecta partidos que aparecen en los datos de votos pero aún no tienen un ID asignado en la dimensión `partidos`. Los incorpora automáticamente a `tablas-finales/dimensiones/partidos`.

### `02-bind-hechos.R`
Script principal que:

1. Lee todos los `info.rds` y `votos.rds` de `data-processed/hechos/`.
2. Asigna `partido_id` por matching case-insensitive de `(siglas, denominacion)`. **Falla si quedan votos sin partido asignado.**
3. Incorpora `nrepresentantes` (escaños asignables) y `representantes` (electos) desde las funciones auxiliares de `utils.R`.
4. Agrupa votos a granularidad `(eleccion_id, territorio_id, partido_id)`.
5. Genera las tablas finales:
   - `tablas-finales/hechos/info.rds` → `resumen_territorial`
   - `tablas-finales/hechos/votos.rds` → `votos_territoriales`

### Validación

Antes de la carga en BD, se ejecutan validaciones automáticas (`R/tests/`):

- **`validate_data_processed.R`**: valida estructura, tipos, unicidad y coherencia lógica de los datos intermedios.
- **`validate_tablas_finales.R`**: valida dimensiones (estructura, FKs, unicidad) y hechos (FKs a elecciones/territorios/partidos, rangos numéricos).

## Fase 4: Carga y exportación

### Carga a PostgreSQL (`R/03-writedb/write-db.R`)

1. Ejecuta las validaciones de dimensiones y hechos.
2. Trunca todas las tablas con `RESTART IDENTITY`.
3. Recarga dimensiones y hechos completos con `DBI::dbWriteTable(..., append = TRUE)`.

### Exportación a formatos descargables (`R/04-export/export-descargas.R`)

Genera tres formatos en `descargas/`:

| Formato | Ruta | Descripción |
|---------|------|-------------|
| **Parquet** | `descargas/parquet/dimensiones/`, `descargas/parquet/hechos/` | Tablas individuales en formato columnar eficiente |
| **SQLite** | `descargas/eleccionesdb.sqlite` | Esquema relacional completo con PKs, FKs, UNIQUE e índices |
| **CSV planos** | `descargas/csv/resumen_territorial.csv`, `descargas/csv/votos_territoriales.csv` | Tablas de hechos con todas las dimensiones pre-joineadas |

## Flujo típico desde cero

1. Configurar `.env` con credenciales de PostgreSQL.
2. Crear esquema en BD ejecutando `R/db_schema.sql`.
3. Preparar ficheros de datos en `data-raw/`.
4. Ejecutar scripts de generación por autonomía (`R/01-generate-data/hechos/*`).
5. Ejecutar scripts de dimensiones (elecciones, territorios, partidos).
6. Detectar y asignar partidos nuevos.
7. Unir hechos finales (`R/02-clean-and-bind/`).
8. Validar.
9. Cargar en BD (`R/03-writedb/write-db.R`).
10. Exportar (`R/04-export/export-descargas.R`).

## Flujo para nuevas elecciones

1. Incorporar nuevos ficheros de resultados en `data-raw/hechos/<autonomía>/`.
2. Ejecutar el script de formato correspondiente en `R/01-generate-data/hechos/<autonomía>/`.
3. Ejecutar `new-partidos.R` para detectar nuevos partidos → revisar `partidos_recodes_pending.xlsx`.
4. Incorporar nuevos partidos a `partidos_recodes.xlsx` y ejecutar `sync_partidos()`.
5. Ejecutar `01-partidos-sin-id.R` y `02-bind-hechos.R`.
6. Validar, cargar en BD y exportar.
