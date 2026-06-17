---
title: "Restaurar data-raw/"
description: "Como poblar el directorio data-raw/ para reproducir el ETL"
---

Para ejecutar el pipeline ETL necesitas poblar el directorio `data-raw/` con los datos originales. Por motivos de tamaño, estos archivos no estan en el repositorio. Los maestros viven en Cloudflare R2 bajo `eleccionesdb-etl/data-raw/`.

## Descarga automatica de datos

1. Restaura el entorno R del proyecto:

```r
renv::restore(prompt = FALSE)
```

2. Ejecuta el script:

```r
source("R/00-setup/download_data_raw.R")
```

Por defecto, el script lee el manifiesto versionado `data-manifest.csv`, descarga todos los archivos listados y los coloca bajo `data-raw/`. Si las claves del manifiesto incluyen el prefijo `eleccionesdb-etl/data-raw/`, el script lo normaliza para restaurar la estructura local correcta.

El script es idempotente: solo descarga archivos que no existen localmente o que no coinciden con el tamaño esperado.

Puedes validar una restauracion con:

```r
source("R/00-setup/check_data_raw.R")
```

## Usar un indice alternativo

En CI/CD o en reproducciones puntuales puedes usar otro indice local o remoto con `DATA_INDEX_URL`:

```r
Sys.setenv(DATA_INDEX_URL = "https://data.spainelectoralproject.com/eleccionesdb-etl/data-manifest.csv")
source("R/00-setup/download_data_raw.R")
```

Tambien puede apuntar a un CSV local:

```r
Sys.setenv(DATA_INDEX_URL = "tmp/data-manifest.csv")
source("R/00-setup/download_data_raw.R")
```

El CSV debe contener al menos las columnas `key` y `url`. La columna `size` es opcional, pero permite verificar que la restauracion esta completa.

## Cache en GitHub Actions

Los workflows `ETL Export` y `Deploy DB` restauran `data-raw/` desde `actions/cache@v4` antes de descargar. La clave depende exactamente de `data-manifest.csv`:

```text
data-raw-${{ runner.os }}-${{ hashFiles('data-manifest.csv') }}
```

No hay `restore-keys` para esta carpeta. Para invalidar la cache, regenera o modifica `data-manifest.csv` y commitea el cambio.

## Como se genera el indice

Solo los administradores con credenciales S3/R2 pueden generar o actualizar el manifiesto:

1. Configura las variables en `.env`: `CF_S3_ACCESS_KEY`, `CF_S3_SECRET_KEY`, `CF_S3_ENDPOINT`, `CF_S3_BUCKET`, `CF_S3_PUBLIC_BASE_URL`.
2. Ejecuta:

```r
source("R/00-setup/generate_data_index.R")
```

Esto crea o actualiza `data-manifest.csv` con la lista de archivos y URLs publicas de `eleccionesdb-etl/data-raw/`.

Si los datos del bucket cambian, regenera `data-manifest.csv` antes de ejecutar el ETL en CI.
