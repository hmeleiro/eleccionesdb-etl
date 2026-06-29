# Versionado del ETL

Este proyecto sigue una logica tipo SemVer: `MAJOR.MINOR.PATCH`. La version del
codigo ETL no se incrementa en cada push o commit; solo cambia cuando se publica
un estado identificable del proceso.

Mientras el proyecto este en `0.x`, las versiones representan releases internas
o estados publicados del ETL. Estas versiones ayudan a comunicar que codigo,
validaciones y contratos de datos se consideran referencia en un momento dado.

## Cuando incrementar la version

- `PATCH`: correcciones, ajustes menores o mejoras compatibles publicadas. Por
  ejemplo, corregir una validacion, normalizar un campo existente o reparar un
  export ya definido sin cambiar su contrato.
- `MINOR`: nuevas funcionalidades compatibles o cambios relevantes del proceso
  ETL. Por ejemplo, ampliar cobertura electoral, incorporar una nueva validacion
  de calidad o anadir un paso del pipeline que mantiene compatibles las salidas.
- `MAJOR`: cambios incompatibles en la API, estructura del proyecto, contratos de
  datos o formato esperado de salida. Por ejemplo, renombrar columnas publicadas,
  eliminar tablas, cambiar tipos de datos incompatibles o modificar el formato de
  los artefactos esperados por consumidores.

## Version, ejecucion y artefactos

La version del codigo ETL identifica un release del repositorio: el conjunto de
scripts, validaciones, documentacion y contratos de salida que se publican juntos.

Una ejecucion concreta del pipeline es un run operativo de ese codigo. Dos runs
pueden usar la misma version del codigo y aun asi diferir por el commit exacto,
la fecha, los datos de entrada o los parametros de ejecucion.

Los artefactos generados son los outputs de una ejecucion concreta, como ZIP,
SQLite, Parquet, CSV o resumenes de calidad. Deben poder trazarse mediante
metadatos de ejecucion como commit SHA, fecha, GitHub Actions `run_id` o
`run_number`.

Por ahora no deben modificarse los nombres actuales de los artefactos automaticos
generados por GitHub Actions. La trazabilidad debe resolverse con metadatos del
run y del commit, no creando una nueva version del codigo para cada artefacto.

## Releases manuales

No hay que crear tags automaticamente desde CI mientras no exista una politica
explicita para ello. Para publicar una release manual basada en un estado del ETL:

``` sh
git tag v0.1.0
git push origin v0.1.0
```

Despues de publicar el tag, se puede crear la release correspondiente en GitHub
Releases y completar las notas a partir de `CHANGELOG.md`.
