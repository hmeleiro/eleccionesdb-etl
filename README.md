# eleccionesdb

Proyecto R para construir y cargar una base de datos PostgreSQL (`eleccionesdb`) con resultados electorales de España: elecciones generales (Congreso) y autonómicas de 9 comunidades.

## Cobertura electoral

| Tipo | Ámbito | Años aprox. |
|------------------|---------------------|----------------------------------|
| Congreso (G) | Nacional | Todas las convocatorias |
| Autonómicas (A) | Andalucía, Aragón, Asturias, Baleares, Canarias, Castilla y León, Cataluña, Comunidad Valenciana, Comunidad de Madrid | Variable por CCAA (1980–2023) |

Otros tipos definidos en el esquema (`L` = Locales, `E` = Europeas, `S` = Senado) aún no tienen datos cargados.

> Para el detalle completo de fuentes de datos por comunidad autónoma, ver [docs/fuentes-datos.md](docs/fuentes-datos.md).

## Arquitectura general

El proyecto sigue un pipeline ETL en cuatro fases:

```         
data-raw/  →  code/01-generate-data/  →  data-processed/
                                             ↓
                                      code/02-clean-and-bind/  →  tablas-finales/
                                                                       ↓
                                                                code/03-writedb/  →  PostgreSQL
                                                                       ↓
                                                                code/04-export/   →  descargas/
```

1.  **Datos en bruto (`data-raw/`)**
    -   Ficheros originales (CSV, RDS, Excel, `.px`) con codificaciones INE/Infoelectoral, fechas de elecciones, nombres de municipios, diccionarios de códigos, etc.
2.  **Generación de datos intermedios (`data-processed/` + `code/01-generate-data/`)**
    -   `code/01-generate-data/dimensiones/` genera dimensiones (elecciones, territorios, partidos).
    -   `code/01-generate-data/hechos/{00-congreso,01-andalucia,...}` genera tablas `info.rds` y `votos.rds` por autonomía en `data-processed/hechos/`.
3.  **Tablas finales (`tablas-finales/` + `code/02-clean-and-bind/`)**
    -   `code/02-clean-and-bind/01-partidos-sin-id.R`: detecta partidos nuevos sin ID y los incorpora a la dimensión.
    -   `code/02-clean-and-bind/02-bind-hechos.R`: une todos los hechos de todas las autonomías, asigna `partido_id`, `nrepresentantes` y `representantes`, valida y genera las tablas finales:
        -   `tablas-finales/dimensiones/`: `tipos_eleccion`, `elecciones`, `territorios`, `partidos`, `partidos_recode`.
        -   `tablas-finales/hechos/`: `info.rds` (→ `resumen_territorial`), `votos.rds` (→ `votos_territoriales`).
4.  **Escritura a BD y exportación**
    -   `code/03-writedb/write-db.R`: trunca todas las tablas y recarga desde `tablas-finales/` a PostgreSQL.
    -   `code/04-export/export-descargas.R`: exporta a Parquet, SQLite (con esquema relacional) y CSV planos pre-joineados en `descargas/`.

## Esquema de base de datos

El DDL completo está en `code/db_schema.sql`. Tablas principales:

**Dimensiones:**

-   `tipos_eleccion(codigo, descripcion)` — `G`, `A`, `L`, `E`, `S`.
-   `elecciones(id, tipo_eleccion, year, mes, dia, fecha, codigo_ccaa, numero_vuelta, descripcion, ambito, slug)`
-   `territorios(id, tipo, codigo_ccaa, codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion, codigo_circunscripcion, nombre, codigo_completo, parent_id)` — jerarquía con `parent_id`.
-   `partidos_recode(id, partido_recode, agrupacion, color)` — agrupaciones/recodificaciones de partidos.
-   `partidos(id, partido_recode_id, siglas, denominacion)` — FK a `partidos_recode`.

**Hechos:**

-   `resumen_territorial(eleccion_id, territorio_id, censo_ine, participacion_1/2/3, votos_validos, abstenciones, votos_blancos, votos_nulos, nrepresentantes)`
-   `votos_territoriales(eleccion_id, territorio_id, partido_id, votos, representantes)`
-   `resumen_cera` y `votos_cera` — tablas análogas para voto CERA (residentes ausentes).

## Conexión a la base de datos

La conexión se centraliza en `code/utils.R` mediante la función `connect()`:

-   Lee credenciales de `.env` (`DB_NAME`, `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`) con `dotenv::load_dot_env()`.
-   Devuelve una conexión `RPostgreSQL` que debe cerrarse con `DBI::dbDisconnect()`.

`code/utils.R` también define funciones auxiliares: - `get_nrepresentantes()`: lee `data-raw/representantes/nrepresentantes_prov.xlsx` y `nrepresentantes_muni.xlsx` para asignar escaños por (elección, territorio). - `get_representantes()`: lee `data-raw/representantes/representantes_*.xlsx` para asignar representantes electos por (elección, territorio, partido).

## Dimensiones

### Tipos de elección y elecciones

-   Script: `code/01-generate-data/dimensiones/elecciones/fechas-elecciones-format.R`.
-   Script de scraping alternativo: `code/01-generate-data/dimensiones/elecciones/fechas-elecciones-scrap.R` (extrae fechas de `juntaelectoralcentral.es`).
-   Lee `data-raw/fechas_elecciones.csv`, normaliza fechas en español y codifica `tipo_eleccion` como:
    -   `G` = Generales (Congreso), `A` = Autonómicas, `L` = Locales, `E` = Europeas, `S` = Senado.
-   Genera `tablas-finales/dimensiones/elecciones` con columnas acordes al esquema.

### Territorios

-   Script: `code/01-generate-data/dimensiones/territorios/territorios.R`.
-   Scripts auxiliares de generación de códigos: `genera-codigos-01.R` (Andalucía), `genera-codigos-03.R` (Asturias), `genera-codigos-07.R` (CyL), `genera-codigos-infoelectoral.R`.
-   Construye la jerarquía territorial (CCAA → provincia → circunscripción → municipio → distrito → sección) a partir de:
    -   `data-raw/codigos_territorios/codigos_secciones*.rds`
    -   `data-raw/codigos_territorios/circunscripciones.csv`
    -   `data-raw/codigos_territorios/correspondencia_municipio_circunscripcion.csv`
    -   `data-raw/nombres_municipios.csv`
-   Las circunscripciones sub-provinciales (Canarias: islas; Asturias: Occidente/Centro/Oriente) se gestionan mediante `correspondencia_municipio_circunscripcion.csv`.
-   Genera `tablas-finales/dimensiones/territorios` con `id`, `parent_id`, y códigos estandarizados.

### Partidos: workflow completo

Los partidos se gestionan en dos dimensiones separadas:

1.  **Recodificación base (`partidos_recode`)**
    -   Fichero fuente: `data-raw/partidos_recodes.xlsx`.
    -   Contiene `partido_recode`, `agrupacion`, `color`.
    -   Script: `code/01-generate-data/dimensiones/partidos/sync-partidos.R`.
    -   Genera `tablas-finales/dimensiones/partidos_recode`.
2.  **Partidos (`partidos`)**
    -   Cada par único `(siglas, denominacion)` recibe un `id` y se enlaza a `partidos_recode` mediante `partido_recode_id`.
    -   Script: `code/01-generate-data/dimensiones/partidos/sync-partidos.R`.
    -   Genera `tablas-finales/dimensiones/partidos`.
3.  **Detección de partidos nuevos**
    -   Script: `code/01-generate-data/dimensiones/partidos/new-partidos.R`.
    -   Lee todos los `votos_*` de `data-processed/`, detecta combinaciones nuevas de `(siglas, denominacion)` y genera `data-processed/partidos_recodes_pending.xlsx` para revisión manual.
    -   Tras incorporar los nuevos partidos a `partidos_recodes.xlsx`, se vuelve a ejecutar `sync_partidos()`.
4.  **Asignación automática en bind**
    -   `code/02-clean-and-bind/01-partidos-sin-id.R` también detecta partidos sin ID y los añade directamente a `tablas-finales/dimensiones/partidos`.

## Hechos

-   Script principal de unión/limpieza: `code/02-clean-and-bind/02-bind-hechos.R`.
-   Lee todos los `info*.rds` y `votos*.rds` de `data-processed/hechos/`.
-   Asigna `partido_id` por matching case-insensitive de `(siglas, denominacion)`. **Falla si quedan votos sin `partido_id`**.
-   Incorpora `nrepresentantes` (escaños asignables) y `representantes` (electos) desde `get_nrepresentantes()` y `get_representantes()`.
-   Agrupa votos a granularidad `(eleccion_id, territorio_id, partido_id)`.
-   Salida:
    -   `tablas-finales/hechos/info.rds` → `resumen_territorial`.
    -   `tablas-finales/hechos/votos.rds` → `votos_territoriales`.

## Validación

### Validación de datos intermedios

-   Script: `code/tests/validate_data_processed.R`.
-   Funciones: `validate_info()`, `validate_votos()`, `validate_info_votos_consistency()`, `run_data_processed_checks()`.
-   Valida estructura, tipos, unicidad `(eleccion_id, territorio_id)`, coherencia lógica (ej: `votos_blancos ≤ votos_validos`), y consistencia info↔votos.

### Validación de tablas finales

-   Script: `code/tests/validate_tablas_finales.R`.
-   `run_dimension_checks()`: valida estructura, tipos, unicidad y foreign keys de todas las dimensiones (`tipos_eleccion`, `elecciones`, `territorios`, `partidos_recode`, `partidos`).
-   `run_fact_checks()`: valida `info.rds` y `votos.rds` (unicidad, FKs a `elecciones`, `territorios`, `partidos`, rangos numéricos).

``` r
source("code/tests/validate_tablas_finales.R")
run_dimension_checks()
run_fact_checks()
```

## Carga en la base de datos

-   Script único: `code/03-writedb/write-db.R`.
-   Ejecuta primero `run_dimension_checks()` y `run_fact_checks()`.
-   Hace `TRUNCATE` de todas las tablas con `RESTART IDENTITY` y recarga dimensiones y hechos completos.
-   Patrón: `tryCatch({ dbWriteTable(..., append = TRUE) }, finally = dbDisconnect(con))`.

## Exportación a formatos descargables

-   Script: `code/04-export/export-descargas.R`.
-   Genera tres formatos en `descargas/`:
    1.  **Parquet**: `descargas/parquet/dimensiones/*.parquet` + `descargas/parquet/hechos/*.parquet`.
    2.  **SQLite**: `descargas/eleccionesdb.sqlite` con esquema relacional completo (PKs, FKs, UNIQUE, índices).
    3.  **CSV planos** (pre-joineados): `descargas/csv/resumen_territorial.csv`, `descargas/csv/votos_territoriales.csv` — tablas de hechos con todas las dimensiones ya unidas.

## Flujo típico desde cero

1.  Configurar `.env` con credenciales de Postgres.
2.  Crear esquema en BD ejecutando `code/db_schema.sql`.
3.  Preparar ficheros de datos en `data-raw/` (incluyendo `partidos_recodes.xlsx`).
4.  Ejecutar scripts de generación de datos por autonomía (`code/01-generate-data/hechos/*`).
5.  Ejecutar scripts de dimensiones:
    -   `code/01-generate-data/dimensiones/elecciones/fechas-elecciones-format.R`
    -   `code/01-generate-data/dimensiones/territorios/territorios.R`
    -   `code/01-generate-data/dimensiones/partidos/sync-partidos.R`
6.  Detectar y asignar partidos nuevos: `code/02-clean-and-bind/01-partidos-sin-id.R`.
7.  Unir hechos finales: `code/02-clean-and-bind/02-bind-hechos.R`.
8.  Ejecutar validaciones: `run_dimension_checks()` y `run_fact_checks()`.
9.  Cargar en BD: `code/03-writedb/write-db.R`.
10. Exportar: `code/04-export/export-descargas.R`.

## Flujo para nuevas elecciones

1.  Incorporar nuevos ficheros de resultados en `data-raw/hechos/<autonomía>/`.
2.  Ejecutar el script de formato correspondiente en `code/01-generate-data/hechos/<autonomía>/`.
3.  Ejecutar `code/01-generate-data/dimensiones/partidos/new-partidos.R` para detectar nuevos partidos → revisar `partidos_recodes_pending.xlsx`.
4.  Incorporar nuevos partidos a `partidos_recodes.xlsx` y ejecutar `sync_partidos()`.
5.  Ejecutar `code/02-clean-and-bind/01-partidos-sin-id.R` y `02-bind-hechos.R`.
6.  Validar: `run_dimension_checks()` y `run_fact_checks()`.
7.  Cargar en BD: `code/03-writedb/write-db.R`.
8.  Exportar: `code/04-export/export-descargas.R`.
