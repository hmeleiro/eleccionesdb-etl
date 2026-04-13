---
title: "Restaurar data-raw/"
description: "Cómo poblar el directorio data-raw/ para reproducir el ETL"
---

Para ejecutar el pipeline ETL necesitas poblar el directorio `data-raw/` con los datos originales. Por motivos de tamaño, estos archivos no están en el repositorio.

## Descarga automática de datos

1. Asegúrate de tener R y los paquetes necesarios (`readr`, `httr`, `fs`, `purrr`).
2. Ejecuta el script:

```r
source("R/00-setup/download_data_raw.R")
```

Esto descargará todos los archivos listados en el índice público y los colocará en su ruta correspondiente bajo `data-raw/`.

- El índice de archivos se encuentra en: `docs-site/data_index.csv` (ajusta la URL si lo alojas en otro sitio).
- El script es idempotente: solo descarga archivos que no existen localmente.

## ¿Cómo se genera el índice?

Solo los administradores (con credenciales S3) pueden generar o actualizar el índice:

1. Configura las variables en `.env` (`CF_S3_ACCESS_KEY`, `CF_S3_SECRET_KEY`, `CF_S3_ENDPOINT`, `CF_S3_BUCKET`, `CF_S3_PUBLIC_BASE_URL`).
2. Ejecuta:

```r
source("R/00-setup/generate_data_index.R")
```

Esto crea/actualiza el archivo `docs-site/data_index.csv` con la lista de archivos y URLs públicas.

---

**Importante:** Si los datos del bucket cambian, recuerda regenerar y volver a publicar el índice.
