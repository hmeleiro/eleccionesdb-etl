# eleccionesdb-etl

[![CI](https://github.com/hmeleiro/eleccionesdb-etl/actions/workflows/ci.yml/badge.svg)](https://github.com/hmeleiro/eleccionesdb-etl/actions/workflows/ci.yml)
[![ETL Export](https://github.com/hmeleiro/eleccionesdb-etl/actions/workflows/etl-export.yml/badge.svg)](https://github.com/hmeleiro/eleccionesdb-etl/actions/workflows/etl-export.yml)
[![Deploy Docs](https://github.com/hmeleiro/eleccionesdb-etl/actions/workflows/deploy-docs.yml/badge.svg)](https://github.com/hmeleiro/eleccionesdb-etl/actions/workflows/deploy-docs.yml)
[![Deploy DB](https://github.com/hmeleiro/eleccionesdb-etl/actions/workflows/deploy-db.yml/badge.svg)](https://github.com/hmeleiro/eleccionesdb-etl/actions/workflows/deploy-db.yml)

## CI/CD

El proyecto usa GitHub Actions para validar el ETL, construir la documentacion y publicar artefactos de forma controlada:

- `CI`: se ejecuta en pull requests y pushes a `main`. Restaura `renv`, comprueba la sintaxis de los scripts R, carga el manifiesto de `{targets}`, ejecuta `lintr` en modo no bloqueante y construye el sitio Hugo.
- `ETL Export`: se ejecuta manualmente, semanalmente y en cambios relevantes de `main`. Restaura `data-raw/` desde cache exacta o lo descarga desde Cloudflare R2, ejecuta `run_export()` y `run_export_calidad()`, valida los ZIP de Parquet/SQLite/CSV y el manifiesto del snapshot SQLite, sube los resultados como artifacts y publica automáticamente en Cloudflare R2 las ejecuciones completas.
- `Deploy Docs`: publica el sitio Hugo en GitHub Pages.
- `Deploy DB`: solo manual, asociado al environment protegido `production-db`. Requiere escribir `TRUNCATE_AND_LOAD_ELECCIONESDB`, ejecuta los targets pesados en procesos R separados en la maquina remota y carga PostgreSQL con staging temporal + transaccion final.

La carga a PostgreSQL no forma parte del CI ordinario. Los pull requests y pushes validan y exportan datos, pero no ejecutan `TRUNCATE` ni escriben en la base de datos real.

### Reproducibilidad local

La via recomendada para reproducir el entorno es `renv`:

``` r
renv::restore(prompt = FALSE)
```

Comandos equivalentes a los workflows principales:

``` r
source("R/00-setup/download_data_raw.R")
source("run.R")
run_export()
run_export_calidad()
```

``` sh
Rscript R/ci/check_project.R
Rscript R/ci/check_exports.R
Rscript R/ci/write_summary.R
hugo --source docs-site --destination public
```

### Secretos usados por CI/CD

- PostgreSQL (`Deploy DB`): `DB_NAME`, `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`.
- Cloudflare R2 (`ETL Export`): `CF_S3_ACCESS_KEY`, `CF_S3_SECRET_KEY`, `CF_S3_ENDPOINT`, `CF_S3_BUCKET`, `CF_S3_PUBLIC_BASE_URL`.
- `DATA_INDEX_URL` puede definirse como input manual de los workflows para usar un manifiesto de datos alternativo; si no se define, se usa `data-manifest.csv`.

Proyecto R para construir y cargar una base de datos PostgreSQL (`eleccionesdb`) con resultados electorales de España: elecciones generales (Congreso), europeas, autonómicas y municipales.

## Cobertura electoral

| Tipo | Ámbito | Años aprox. |
|------------------|---------------------|---------------------------------|
| Congreso (G) | Nacional | Todas las convocatorias |
| Europeas (E) | Nacional | Todas las convocatorias desde 1987 |
| Municipales (L) | Nacional | Todas las convocatorias |
| Autonómicas (A) | Andalucía, Aragón, Asturias, Baleares, Canarias, Cantabria, Castilla y León, Castilla-La Mancha, Cataluña, Comunidad Valenciana, Extremadura, Galicia, Comunidad de Madrid, Región de Murcia, Navarra, País Vasco, La Rioja | Todas las convocatorias |

El tipo definido en el esquema `S` (Senado) aún no tiene datos cargados.

> Para el detalle completo de fuentes de datos por comunidad autónoma, ver [docs/fuentes-datos.md](docs/fuentes-datos.md).

## Arquitectura general

El proyecto sigue un pipeline ETL en cuatro fases:

```         
data-raw/  →  R/01-generate-data/  →  data-processed/
                                             ↓
                                      R/02-clean-and-bind/  →  tablas-finales/
                                                                       ↓
                                                                R/03-writedb/  →  PostgreSQL
                                                                       ↓
                                                                R/04-export/   →  descargas/
```

1.  **Datos en bruto (`data-raw/`)**
    -   Ficheros originales (CSV, RDS, Excel, `.px`) con codificaciones INE/Infoelectoral, fechas de elecciones, nombres de municipios, diccionarios de códigos, etc.
2.  **Generación de datos intermedios (`data-processed/` + `R/01-generate-data/`)**
    -   `R/01-generate-data/dimensiones/` genera dimensiones (elecciones, territorios, partidos).
    -   `R/01-generate-data/hechos/{00-congreso,01-andalucia,...}` genera tablas `info.rds` y `votos.rds` por ámbito electoral (Congreso, autonomías, municipales, etc...) y las guarda en `data-processed/hechos/`.
3.  **Tablas finales (`tablas-finales/` + `R/02-clean-and-bind/`)**
    -   `R/02-clean-and-bind/01-partidos-sin-id.R`: detecta partidos nuevos sin ID y los incorpora a la tabla de dimensión de partidos.
    -   `R/02-clean-and-bind/02-bind-hechos.R`: une todos los hechos de todas las autonomías, asigna `partido_id`, `nrepresentantes` y `representantes`, valida y genera las tablas finales:
        -   `tablas-finales/dimensiones/`: `tipos_eleccion`, `elecciones`, `elecciones_fuentes`, `territorios`, `partidos`, `partidos_recode`.
        -   `tablas-finales/hechos/`: `info.rds` (→ `resumen_territorial`), `votos.rds` (→ `votos_territoriales`).
4.  **Escritura a BD y exportación**
    -   `R/03-writedb/write-db.R`: valida `tablas-finales/`, carga staging temporal en PostgreSQL y reemplaza las tablas finales dentro de una transaccion.
    -   `R/04-export/export-descargas.R`: exporta a Parquet, SQLite (con esquema relacional) y CSV planos pre-joineados en `descargas/`.

## Esquema de base de datos

El DDL completo está en `R/00-setup/db_schema.sql`. Tablas principales:

**Dimensiones:**

-   `tipos_eleccion(codigo, descripcion)` — `G`, `A`, `L`, `E`, `S`.
-   `elecciones(id, tipo_eleccion, year, mes, dia, fecha, codigo_ccaa, numero_vuelta, descripcion, ambito, slug)`
-   `elecciones_fuentes(eleccion_id, fuente, url_fuente, observaciones)` — fuente oficial y URL de cada elección.
-   `territorios(id, tipo, codigo_ccaa, codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion, codigo_circunscripcion, nombre, codigo_completo, parent_id)` — jerarquía con `parent_id`.
-   `partidos_recode(id, partido_recode, agrupacion, color)` — agrupaciones/recodificaciones de partidos.
-   `partidos(id, partido_recode_id, siglas, denominacion)` — FK a `partidos_recode`.

**Hechos:**

-   `resumen_territorial(eleccion_id, territorio_id, censo_ine, participacion_1/2/3, votos_validos, abstenciones, votos_blancos, votos_nulos, nrepresentantes)`
-   `votos_territoriales(eleccion_id, territorio_id, partido_id, votos, representantes)`
-   `resumen_cera` y `votos_cera` — tablas análogas para voto CERA (residentes ausentes).

## Conexión a la base de datos

La conexión se centraliza en `R/utils.R` mediante la función `connect()`:

-   Lee credenciales de `.env` (`DB_NAME`, `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`) con `dotenv::load_dot_env()`.
-   Devuelve una conexión `RPostgreSQL` que debe cerrarse con `DBI::dbDisconnect()`.

`R/utils.R` también define funciones auxiliares:

-   `get_nrepresentantes()`: lee `data-raw/representantes/nrepresentantes_prov.xlsx` y `nrepresentantes_muni.xlsx` para asignar escaños por (elección, territorio).

-   `get_representantes()`: lee `data-raw/representantes/representantes_*.xlsx` para asignar representantes electos por (elección, territorio, partido).

## Dimensiones

### Tipos de elección y elecciones

-   Script: `R/01-generate-data/dimensiones/elecciones/fechas-elecciones-format.R`.
-   Script de scraping alternativo: `R/01-generate-data/dimensiones/elecciones/fechas-elecciones-scrap.R` (extrae fechas de `juntaelectoralcentral.es`).
-   Lee `data-raw/fechas_elecciones.csv`, normaliza fechas en español y codifica `tipo_eleccion` como:
    -   `G` = Generales (Congreso), `A` = Autonómicas, `L` = Locales, `E` = Europeas, `S` = Senado.
-   Genera `tablas-finales/dimensiones/elecciones` con columnas acordes al esquema.

### Fuentes de elecciones

-   Script: `R/01-generate-data/dimensiones/elecciones/elecciones-fuentes-format.R`.
-   Lee `data-raw/elecciones_fuentes.csv` (una fila por tipo de elección/CCAA) y lo une con la tabla de elecciones para generar una fila por cada `eleccion_id`.
-   Genera `tablas-finales/dimensiones/elecciones_fuentes` con columnas: `eleccion_id`, `fuente`, `url_fuente`, `observaciones`.
-   Emite un **warning** si hay elecciones sin fuente documentada, pero no impide continuar.

### Territorios

-   Script: `R/01-generate-data/dimensiones/territorios/territorios.R`.
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
    -   Script: `R/01-generate-data/dimensiones/partidos/sync-partidos.R`.
    -   Genera `tablas-finales/dimensiones/partidos_recode`.
2.  **Partidos (`partidos`)**
    -   Cada par único `(siglas, denominacion)` recibe un `id` y se enlaza a `partidos_recode` mediante `partido_recode_id`.
    -   Script: `R/01-generate-data/dimensiones/partidos/sync-partidos.R`.
    -   Genera `tablas-finales/dimensiones/partidos`.
3.  **Detección de partidos nuevos**
    -   Script: `R/01-generate-data/dimensiones/partidos/new-partidos.R`.
    -   Lee todos los `votos_*` de `data-processed/`, detecta combinaciones nuevas de `(siglas, denominacion)` y genera `data-processed/partidos_recodes_pending.xlsx` para revisión manual.
    -   Tras incorporar los nuevos partidos a `partidos_recodes.xlsx`, se vuelve a ejecutar `sync_partidos()`.
4.  **Asignación automática en bind**
    -   `R/02-clean-and-bind/01-partidos-sin-id.R` también detecta partidos sin ID y los añade directamente a `tablas-finales/dimensiones/partidos`.

## Hechos

-   Script principal de unión/limpieza: `R/02-clean-and-bind/02-bind-hechos.R`.
-   Lee todos los `info*.rds` y `votos*.rds` de `data-processed/hechos/`.
-   Asigna `partido_id` por matching case-insensitive de `(siglas, denominacion)`. **Falla si quedan votos sin `partido_id`**.
-   Incorpora `nrepresentantes` (escaños asignables) y `representantes` (electos) desde `get_nrepresentantes()` y `get_representantes()`.
-   Agrupa votos a granularidad `(eleccion_id, territorio_id, partido_id)`.
-   Salida:
    -   `tablas-finales/hechos/info.rds` → `resumen_territorial`.
    -   `tablas-finales/hechos/votos.rds` → `votos_territoriales`.

## Validación

### Validación de datos intermedios

-   Script: `R/tests/validate_data_processed.R`.
-   Funciones: `validate_info()`, `validate_votos()`, `validate_info_votos_consistency()`, `run_data_processed_checks()`.
-   Valida estructura, tipos, unicidad `(eleccion_id, territorio_id)`, coherencia lógica (ej: `votos_blancos ≤ votos_validos`), y consistencia info↔votos.

### Validación de tablas finales

-   Script: `R/tests/validate_tablas_finales.R`.
-   `run_dimension_checks()`: valida estructura, tipos, unicidad y foreign keys de todas las dimensiones (`tipos_eleccion`, `elecciones`, `elecciones_fuentes`, `territorios`, `partidos_recode`, `partidos`).
-   `run_fact_checks()`: valida `info.rds` y `votos.rds` (unicidad, FKs a `elecciones`, `territorios`, `partidos`, rangos numéricos).

``` r
source("R/tests/validate_tablas_finales.R")
run_dimension_checks()
run_fact_checks()
```

## Carga en la base de datos

-   Script unico: `R/03-writedb/write-db.R`.
-   Ejecuta primero `run_dimension_checks()` y `run_fact_checks()`.
-   Carga dimensiones y hechos en tablas temporales de PostgreSQL usando chunks.
-   Reemplaza las tablas finales con `TRUNCATE ... RESTART IDENTITY` e `INSERT ... SELECT` dentro de una unica transaccion.
-   Si falla la fase final, hace rollback y conserva el estado anterior de la base de datos.

## Exportación a formatos descargables

-   Script: `R/04-export/export-descargas.R`.
-   Genera tres formatos en `descargas/`:
    1.  **Parquet**: `descargas/parquet/dimensiones/*.parquet` + `descargas/parquet/hechos/*.parquet`.
    2.  **SQLite**: `descargas/eleccionesdb_sqlite.zip` con esquema relacional versionado y `descargas/eleccionesdb_sqlite.json` con URL, tamaños y checksums SHA-256 para actualizaciones seguras.
    3.  **CSV planos** (pre-joineados): `descargas/csv/resumen_territorial.csv`, `descargas/csv/votos_territoriales.csv` — tablas de hechos con todas las dimensiones ya unidas.

## Orquestación con {targets} {#orquestación-con-targets}

El pipeline completo se orquesta con el paquete [{targets}](https://docs.ropensci.org/targets/), que proporciona:

-   **Caché automática**: solo re-ejecuta los pasos cuyas dependencias han cambiado.
-   **DAG de dependencias**: define el orden de ejecución basado en las relaciones entre targets.
-   **Ejecución selectiva**: permite ejecutar solo una fase o un target específico.

### Estructura del pipeline

El pipeline se define en `_targets.R` y organiza las dimensiones, hechos, controles y exportaciones en 6 fases:

```         
Phase 1: Dimensiones (independientes entre sí)
  dim_territorios
  dim_elecciones
  dim_elecciones_fuentes (← dim_elecciones)
  dim_partidos

Phase 2: Hechos regionales (dependen de dims)
  hechos_congreso, hechos_municipales, hechos_europeas,
  hechos_andalucia, hechos_aragon, hechos_asturias, hechos_baleares, hechos_canarias, hechos_cantabria,
  hechos_cyl, hechos_clm, hechos_catalunya, hechos_valencia,
  hechos_extremadura, hechos_galicia, hechos_madrid, hechos_murcia,
  hechos_navarra, hechos_pais_vasco, hechos_la_rioja, hechos_minsait

Phase 3a: Completar partidos (← todos los hechos + partidos)
  partidos_sin_id

Phase 3b: Bind (← todos los hechos + partidos)
  bind_hechos

Phase 3c: Cobertura de representantes (← bind + Excel manuales)
  representantes_coverage

Phase 4: Write DB (← bind + dims)
  writedb

Phase 5: Export (← bind + dims)
  export

Phase 6: Diagnósticos de calidad
  gen_diagnosticos, export_calidad
```

### Uso básico

``` r
source("run.R")

# Ejecutar todo el pipeline (solo lo que necesite actualizarse)
run_all()

# Ejecutar solo fases específicas
run_dims()      # solo dimensiones
run_hechos()    # dimensiones + hechos regionales
run_bind()      # dimensiones + hechos + bind + cobertura de representantes
run_representantes_coverage() # genera data-processed/representantes_pending.xlsx
run_writedb()   # todo hasta escritura a BD
run_export()    # todo hasta exportación

# Visualizar el grafo de dependencias
show_pipeline()
```

### Cobertura de representantes

El target `representantes_coverage` comprueba los repartos de elecciones generales,
autonómicas y locales; las europeas quedan excluidas. Para G/A utiliza las
circunscripciones explícitas cuando existen y, en otro caso, la provincia. Para
locales utiliza el municipio.

El control avisa, sin detener el pipeline, cuando falta `nrepresentantes`, no hay
ningún representante electo o la suma de electos no coincide con los escaños. Además,
genera `data-processed/representantes_pending.xlsx`, con cuatro hojas para revisar
`representantes_prov.xlsx`, `representantes_muni.xlsx`,
`nrepresentantes_prov.xlsx` y `nrepresentantes_muni.xlsx`. Los cuatro Excel de origen
son dependencias de {targets}: al modificarlos se vuelven a ejecutar el bind y este
diagnóstico.

### Comandos {targets} útiles

``` r
library(targets)

# Ver qué targets necesitan re-ejecutarse
tar_outdated()

# Ver la lista completa de targets
tar_manifest()

# Ejecutar un target específico (con sus dependencias)
tar_make(names = "hechos_andalucia")

# Leer el resultado de un target ya ejecutado
tar_read(dim_elecciones)

# Invalidar un target para forzar su re-ejecución
tar_invalidate(names = "hechos_aragon")

# Destruir la caché completa (re-ejecuta todo)
tar_destroy()
```

### Cómo funciona internamente

1.  `_targets.R` define el pipeline: qué targets existen y de qué dependen.
2.  `R/pipeline_helpers.R` contiene las funciones wrapper (`run_dim_*()`, `run_hechos()`, etc.) que hacen `source()` de los scripts originales.
3.  `run.R` proporciona helpers de conveniencia que llaman a `tar_make()` con los targets apropiados.
4.  La caché se almacena en `_targets/` (ignorado por git). Si se borra, todo se re-ejecuta desde cero.

> **Nota**: Los scripts de fetch/download de datos (Andalucía, Asturias, Canarias) **no forman parte del pipeline**. Son operaciones manuales que se ejecutan puntualmente cuando hay elecciones nuevas. Sus outputs se guardan en `data-raw/` y el pipeline solo lee de ahí.

## Flujo típico desde cero

1.  Configurar `.env` con credenciales de Postgres.
2.  Crear esquema en BD ejecutando `R/00-setup/db_schema.sql`.
3.  Preparar ficheros de datos en `data-raw/` (incluyendo `partidos_recodes.xlsx`).
4.  Ejecutar el pipeline completo con {targets} (ver [Orquestación con {targets}](#orquestación-con-targets)):

``` r
source("run.R")
run_all()
```

Alternativamente, ejecutar paso a paso:

4.  Ejecutar scripts de dimensiones y hechos: `run_dims()` + `run_hechos()`.
5.  Unir hechos finales: `run_bind()`.
6.  Cargar en BD: `run_writedb()`.
7.  Exportar: `run_export()`.

## Flujo para nuevas elecciones

1.  Incorporar nuevos ficheros de resultados en `data-raw/hechos/<ambito>/`.
2.  Si la fuente requiere descarga (Andalucía, Asturias, Canarias), ejecutar manualmente el script de fetch correspondiente.
3.  Ejecutar `R/01-generate-data/dimensiones/partidos/new-partidos.R` para detectar nuevos partidos → revisar `partidos_recodes_pending.xlsx`.
4.  Incorporar nuevos partidos a `partidos_recodes.xlsx` y ejecutar `sync_partidos()`.
5.  Ejecutar el pipeline:

``` r
source("run.R")
run_all()  # solo re-ejecutará lo que haya cambiado (caché de {targets})
```

## Restaurar data-raw/ para reproducir el ETL

Los archivos originales de datos (`data-raw/`) no se incluyen en el repositorio por cuestiones de tamaño. Los maestros viven en Cloudflare R2 bajo el prefijo `eleccionesdb-etl/data-raw/`.

### Descarga automática de datos (sin credenciales)

1.  Asegúrate de tener R y los paquetes necesarios (`readr`, `httr`, `fs`).
2.  Ejecuta:

``` r
source("R/00-setup/download_data_raw.R")
```

Esto descarga todos los archivos listados en `data-manifest.csv` y los coloca bajo `data-raw/`, conservando la estructura de directorios. El script es idempotente: solo descarga archivos que faltan o que no coinciden con el tamaño esperado.

Puedes validar una restauración local con:

``` sh
Rscript R/00-setup/check_data_raw.R
```

### Caché en GitHub Actions

`ETL Export` cachea la carpeta completa `data-raw/` con `actions/cache@v5`. La clave exacta es:

``` text
data-raw-${{ runner.os }}-${{ hashFiles('data-manifest.csv') }}
```

No se usan `restore-keys` para `data-raw/`, asi que GitHub Actions solo reutiliza una cache cuyo manifiesto coincide exactamente. Si se usa el input manual `DATA_INDEX_URL`, el workflow descarga desde ese manifiesto alternativo.

`Deploy DB` se ejecuta por SSH en la maquina remota: no puede reutilizar la cache de GitHub Actions para `data-raw/`, pero valida la carpeta remota y descarga solo los ficheros ausentes o con tamano distinto segun `data-manifest.csv`.

### Cómo invalidar la caché

Cuando cambien los datos en R2, modifica o regenera `data-manifest.csv` y commitea ese cambio. Al cambiar el hash del manifiesto, GitHub Actions creara una clave nueva y volvera a descargar `data-raw/` antes de ejecutar el pipeline de exportacion.

### Cómo se genera el manifiesto (solo admins)

1.  Configura las variables en `.env` (`CF_S3_ACCESS_KEY`, `CF_S3_SECRET_KEY`, `CF_S3_ENDPOINT`, `CF_S3_BUCKET`, `CF_S3_PUBLIC_BASE_URL`).
2.  Ejecuta:

``` r
source("R/00-setup/generate_data_index.R")
```

Esto crea o actualiza `data-manifest.csv` con los objetos públicos de `eleccionesdb-etl/data-raw/`.
