# R/pipeline_helpers.R
# Wrapper functions for {targets} pipeline.
# Each function source()s the original script and returns output file paths.

ensure_pipeline_dirs <- function() {
  dirs <- c(
    "data-processed",
    "data-processed/hechos",
    "tablas-finales",
    "tablas-finales/dimensiones",
    "tablas-finales/hechos",
    "descargas",
    "docs-site/data/diagnosticos"
  )

  invisible(lapply(dirs, dir.create, recursive = TRUE, showWarnings = FALSE))
}

source_pipeline_env <- function(path) {
  env <- new.env(parent = parent.frame())
  env$source <- function(file, local = parent.frame(), ..., encoding = "UTF-8") {
    base::source(file, local = local, ..., encoding = encoding)
  }

  base::source(path, local = env, encoding = "UTF-8")
  env
}

run_pipeline_script <- function(path) {
  env <- source_pipeline_env(path)
  rm(env)
  invisible(gc())
}

# =============================================================================
# DIMENSIONES
# =============================================================================

run_dim_territorios <- function() {
  run_pipeline_script("R/01-generate-data/dimensiones/territorios/territorios.R")
  list.files("tablas-finales/dimensiones/territorios", full.names = TRUE)
}

run_dim_elecciones <- function() {
  run_pipeline_script("R/01-generate-data/dimensiones/elecciones/fechas-elecciones-format.R")
  list.files("tablas-finales/dimensiones/elecciones", full.names = TRUE)
}

run_dim_elecciones_fuentes <- function(dim_elecciones) {
  run_pipeline_script("R/01-generate-data/dimensiones/elecciones/elecciones-fuentes-format.R")
  list.files("tablas-finales/dimensiones/elecciones_fuentes", full.names = TRUE)
}

run_dim_partidos <- function() {
  run_pipeline_script("R/01-generate-data/dimensiones/partidos/sync-partidos.R")
  list.files("data-processed/partidos", full.names = TRUE, recursive = TRUE)
}

# =============================================================================
# HECHOS (regionales)
# =============================================================================

# Generic wrapper: source format.R and return info.rds + votos.rds paths
run_hechos <- function(region_dir, dim_elecciones, dim_territorios) {
  script_path <- file.path("R/01-generate-data/hechos", region_dir, "format.R")
  run_pipeline_script(script_path)
  c(
    file.path("data-processed/hechos", region_dir, "info.rds"),
    file.path("data-processed/hechos", region_dir, "votos.rds")
  )
}

# =============================================================================
# PARTIDOS SIN ID (completar dimensión partidos con los que faltan)
# =============================================================================

run_partidos_sin_id <- function(all_hechos, dim_partidos) {
  run_pipeline_script("R/02-clean-and-bind/01-partidos-sin-id.R")
  "tablas-finales/dimensiones/partidos"
}

# =============================================================================
# BIND
# =============================================================================

run_bind_hechos <- function(all_hechos, partidos_sin_id) {
  run_pipeline_script("R/02-clean-and-bind/02-bind-hechos.R")
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
  run_pipeline_script("R/03-writedb/write-db.R")
  invisible(NULL)
}

# =============================================================================
# EXPORT
# =============================================================================

run_export <- function(bind_hechos, dim_elecciones, dim_elecciones_fuentes,
                       dim_territorios, dim_partidos) {
  run_pipeline_script("R/04-export/export-descargas.R")
  list.files("descargas", full.names = TRUE, recursive = TRUE)
}

# =============================================================================
# DIAGNOSTICOS DE CALIDAD
# =============================================================================

# Detecta incidencias automáticas, las combina con las manuales y escribe
# docs-site/data/diagnosticos/incidencias.csv
run_gen_diagnosticos <- function(bind_hechos) {
  env <- source_pipeline_env("R/tests/generate_diagnosticos.R")
  env$generate_diagnosticos()
  rm(env)
  invisible(gc())
  "docs-site/data/diagnosticos/incidencias.csv"
}

# Lee incidencias.csv y genera docs-site/content/calidad.md
run_export_calidad <- function(gen_diagnosticos) {
  env <- source_pipeline_env("R/04-export/export-calidad.R")
  env$export_calidad()
  rm(env)
  invisible(gc())
  "docs-site/content/calidad.md"
}
