---
title: "CI/CD"
description: "Automatizacion de validaciones, exportaciones, documentacion y despliegues"
---

El proyecto usa GitHub Actions para separar validacion, exportacion, documentacion y despliegue de base de datos. La regla principal es que el CI ordinario nunca escribe en PostgreSQL: la carga real queda aislada en un workflow manual protegido.

## Workflows

| Workflow | Disparador | Proposito |
|---|---|---|
| `CI` | Pull requests y pushes a `main` | Restaura `renv`, comprueba scripts R, carga el manifiesto de `{targets}`, ejecuta lint no bloqueante y construye Hugo. |
| `ETL Export` | Manual, semanal y cambios relevantes en `main` | Descarga `data-raw/`, ejecuta `run_export()` y `run_export_calidad()`, valida ZIPs y sube artefactos. |
| `Deploy Docs` | Manual y cambios en `docs-site/` en `main` | Construye el sitio Hugo y lo publica en GitHub Pages. |
| `Deploy DB` | Manual | Descarga datos, ejecuta el pipeline hasta `run_writedb()` y carga PostgreSQL con secretos. |

## Reproducibilidad

La fuente canonica de dependencias es `renv.lock`:

```r
renv::restore(prompt = FALSE)
```

El script `install_deps.R` se mantiene como bootstrap secundario para entornos sin `renv`, pero no sustituye al lockfile.

## Artefactos de exportacion

`ETL Export` genera y valida:

- `descargas/eleccionesdb_parquet.zip`
- `descargas/eleccionesdb_sqlite.zip`
- `descargas/eleccionesdb_csv.zip`
- `ci-summary.md`, con metricas basicas de tablas finales, diagnosticos y tamaños de artefactos.

La validacion abre el SQLite y ejecuta `PRAGMA integrity_check`, comprueba tablas obligatorias, extrae los ZIP y lee muestras de Parquet y CSV.

## Publicacion de descargas

La publicacion a Cloudflare R2 es manual. En `ETL Export`, activa el input `publish_downloads` para sincronizar `descargas/` contra:

```text
s3://$CF_S3_BUCKET/eleccionesdb-etl/descargas/
```

Secretos requeridos:

- `CF_S3_ACCESS_KEY`
- `CF_S3_SECRET_KEY`
- `CF_S3_ENDPOINT`
- `CF_S3_BUCKET`
- `CF_S3_PUBLIC_BASE_URL`

## Carga PostgreSQL

`Deploy DB` usa el environment protegido `production-db` y requiere confirmar manualmente con:

```text
TRUNCATE_AND_LOAD_ELECCIONESDB
```

Secretos requeridos:

- `DB_NAME`
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`

Este workflow ejecuta `run_writedb()`, que valida tablas finales y recarga PostgreSQL. No debe habilitarse en pull requests.
