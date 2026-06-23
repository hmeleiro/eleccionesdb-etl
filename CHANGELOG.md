# Changelog

Todos los cambios relevantes de releases del ETL se documentan en este archivo.
El changelog recoge estados publicados del proyecto, no cada commit ni cada push.

El formato se inspira en Keep a Changelog y el versionado sigue una logica tipo
SemVer (`MAJOR.MINOR.PATCH`). Mientras el proyecto este en `0.x`, las versiones
representan releases internas o estados publicados del ETL.

## [0.1.0] - 2026-03-27

Primera version documentada del ETL como referencia de release interna.

### Added

- Pipeline ETL para generar, validar, cargar y exportar `eleccionesdb`.
- Exportaciones en formatos Parquet, SQLite y CSV.
- Validaciones basicas de tablas finales y artefactos de descarga.
- Workflows de GitHub Actions para CI, exportacion ETL, documentacion y carga de
  base de datos.
- Documentacion inicial de la estrategia de versionado del ETL.

### Changed

### Fixed

### Removed
