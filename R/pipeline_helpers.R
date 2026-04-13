# R/pipeline_helpers.R
# Wrapper functions for {targets} pipeline.
# Each function source()s the original script and returns output file paths.

# =============================================================================
# DIMENSIONES
# =============================================================================

run_dim_territorios <- function() {
  source("R/01-generate-data/dimensiones/territorios/territorios.R", encoding = "UTF-8")
  list.files("tablas-finales/dimensiones/territorios", full.names = TRUE)
}

run_dim_elecciones <- function() {
  source("R/01-generate-data/dimensiones/elecciones/fechas-elecciones-format.R", encoding = "UTF-8")
  list.files("tablas-finales/dimensiones/elecciones", full.names = TRUE)
}

run_dim_elecciones_fuentes <- function(dim_elecciones) {
  source("R/01-generate-data/dimensiones/elecciones/elecciones-fuentes-format.R", encoding = "UTF-8")
  list.files("tablas-finales/dimensiones/elecciones_fuentes", full.names = TRUE)
}

run_dim_partidos <- function() {
  source("R/01-generate-data/dimensiones/partidos/sync-partidos.R", encoding = "UTF-8")
  list.files("data-processed/partidos", full.names = TRUE, recursive = TRUE)
}

# =============================================================================
# HECHOS (regionales)
# =============================================================================

# Generic wrapper: source format.R and return info.rds + votos.rds paths
run_hechos <- function(region_dir, dim_elecciones, dim_territorios) {
  script_path <- file.path("R/01-generate-data/hechos", region_dir, "format.R")
  source(script_path, encoding = "UTF-8")
  c(
    file.path("data-processed/hechos", region_dir, "info.rds"),
    file.path("data-processed/hechos", region_dir, "votos.rds")
  )
}

# =============================================================================
# PARTIDOS SIN ID (completar dimensión partidos con los que faltan)
# =============================================================================

run_partidos_sin_id <- function(all_hechos, dim_partidos) {
  source("R/02-clean-and-bind/01-partidos-sin-id.R", encoding = "UTF-8")
  "tablas-finales/dimensiones/partidos"
}

# =============================================================================
# BIND
# =============================================================================

run_bind_hechos <- function(all_hechos, partidos_sin_id) {
  source("R/02-clean-and-bind/02-bind-hechos.R", encoding = "UTF-8")
  c(
    "tablas-finales/hechos/info.rds",
    "tablas-finales/hechos/votos.rds"
  )
}

# =============================================================================
# WRITE DB
# =============================================================================

run_writedb <- function(bind_hechos, dim_elecciones, dim_elecciones_fuentes,
                        dim_territorios, dim_partidos) {
  source("R/03-writedb/write-db.R", encoding = "UTF-8")
  invisible(NULL)
}

# =============================================================================
# EXPORT
# =============================================================================

run_export <- function(bind_hechos, dim_elecciones, dim_elecciones_fuentes,
                       dim_territorios, dim_partidos) {
  source("R/04-export/export-descargas.R", encoding = "UTF-8")
  list.files("descargas", full.names = TRUE, recursive = TRUE)
}
