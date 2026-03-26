# validate_tablas_finales.R
#
# Validates final dimension and fact tables in tablas-finales/.
#
# ----- Usage A: inside a generation script (validates in memory) -----
#
#   source("code/tests/validate_tablas_finales.R")
#
#   # After building the dataframe, before write_csv() / saveRDS():
#   validate_dim_elecciones(elecciones)
#   write_csv(elecciones, "tablas-finales/dimensiones/elecciones")
#
#   validate_hechos_info(info)
#   validate_hechos_votos(votos)
#   saveRDS(info,  "tablas-finales/hechos/info.rds")
#   saveRDS(votos, "tablas-finales/hechos/votos.rds")
#
# ----- Usage B: batch validation of all tablas-finales/ -----
#
#   source("code/tests/validate_tablas_finales.R")
#   run_dimension_checks()
#   run_fact_checks()
#   # or:
#   Rscript code/tests/validate_tablas_finales.R
#
# ----- Usage C: mixed — pass some dfs, let others fall back to files -----
#
#   run_fact_checks(info = info_df, votos = votos_df)

library(readr)
library(dplyr)

# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

stop_if_not_all <- function(cond, msg) {
  if (!all(cond)) stop(msg, call. = FALSE)
}

# Read from path only if df is NULL; otherwise use df as-is.
.resolve_df <- function(df, path, read_fn, ...) {
  if (!is.null(df)) {
    return(df)
  }
  if (!file.exists(path)) stop(sprintf("No se encontró el archivo '%s'", path), call. = FALSE)
  read_fn(path, ...)
}

# ---------------------------------------------------------------------------
# Core validators — work on dataframes (call these from generation scripts)
# ---------------------------------------------------------------------------

#' @examples
#' source("code/tests/validate_tablas_finales.R")
#' validate_dim_tipos_eleccion(tipos_eleccion_df)
#' write_csv(tipos_eleccion_df, "tablas-finales/dimensiones/tipos_eleccion")
validate_dim_tipos_eleccion <- function(df, label = "tipos_eleccion") {
  expected_cols <- c("codigo", "descripcion")
  stop_if_not_all(
    setequal(names(df), expected_cols),
    paste0(
      "[", label, "] Columnas inesperadas o faltantes: ",
      paste(setdiff(
        union(names(df), expected_cols),
        intersect(names(df), expected_cols)
      ), collapse = ", ")
    )
  )
  stop_if_not_all(
    nchar(df$codigo) == 1,
    paste0("[", label, "] $codigo debe tener longitud 1")
  )
  stop_if_not_all(
    !is.na(df$codigo) & df$codigo != "",
    paste0("[", label, "] $codigo no debe estar vacío")
  )
  stop_if_not_all(
    !duplicated(df$codigo),
    paste0("[", label, "] $codigo debe ser único")
  )
  message(sprintf("  [OK] %s (%d filas)", label, nrow(df)))
  invisible(df)
}

#' @examples
#' validate_dim_elecciones(elecciones_df)
#' write_csv(elecciones_df, "tablas-finales/dimensiones/elecciones")
validate_dim_elecciones <- function(df, label = "elecciones") {
  expected_cols <- c(
    "id", "tipo_eleccion", "fecha", "codigo_ccaa", "year", "mes",
    "dia", "numero_vuelta", "descripcion", "ambito", "slug"
  )
  stop_if_not_all(
    setequal(names(df), expected_cols),
    paste0(
      "[", label, "] Columnas inesperadas o faltantes: ",
      paste(setdiff(
        union(names(df), expected_cols),
        intersect(names(df), expected_cols)
      ), collapse = ", ")
    )
  )
  stop_if_not_all(
    is.numeric(df$id),
    paste0("[", label, "] $id debe ser numérico")
  )
  stop_if_not_all(
    !duplicated(df$id),
    paste0("[", label, "] $id debe ser único")
  )
  stop_if_not_all(
    nchar(df$tipo_eleccion) == 1,
    paste0("[", label, "] $tipo_eleccion debe tener longitud 1")
  )
  stop_if_not_all(
    nchar(df$year) == 4,
    paste0("[", label, "] $year debe tener longitud 4")
  )
  stop_if_not_all(
    nchar(df$mes) == 2,
    paste0("[", label, "] $mes debe tener longitud 2")
  )
  stop_if_not_all(
    nchar(df$dia) == 2,
    paste0("[", label, "] $dia debe tener longitud 2")
  )
  stop_if_not_all(
    inherits(df$fecha, "Date") | all(is.na(df$fecha)),
    paste0("[", label, "] $fecha debe ser Date o NA")
  )
  stop_if_not_all(
    !duplicated(df[, c("tipo_eleccion", "year", "mes", "codigo_ccaa", "numero_vuelta")]),
    paste0("[", label, "] UNIQUE (tipo_eleccion, year, mes, codigo_ccaa, numero_vuelta) violado")
  )
  message(sprintf("  [OK] %s (%d filas)", label, nrow(df)))
  invisible(df)
}

#' @examples
#' validate_dim_territorios(territorios_df)
#' write_csv(territorios_df, "tablas-finales/dimensiones/territorios")
validate_dim_territorios <- function(df, label = "territorios") {
  expected_cols <- c(
    "id", "tipo", "codigo_ccaa", "codigo_provincia", "codigo_municipio",
    "codigo_distrito", "codigo_seccion", "codigo_circunscripcion",
    "nombre", "codigo_completo", "parent_id"
  )
  stop_if_not_all(
    setequal(names(df), expected_cols),
    paste0(
      "[", label, "] Columnas inesperadas o faltantes: ",
      paste(setdiff(
        union(names(df), expected_cols),
        intersect(names(df), expected_cols)
      ), collapse = ", ")
    )
  )
  stop_if_not_all(
    is.numeric(df$id),
    paste0("[", label, "] $id debe ser numérico")
  )
  stop_if_not_all(
    !duplicated(df$id),
    paste0("[", label, "] $id debe ser único")
  )
  stop_if_not_all(
    nchar(df$codigo_ccaa) == 2 | is.na(df$codigo_ccaa),
    paste0("[", label, "] $codigo_ccaa debe tener longitud 2 o ser NA")
  )
  stop_if_not_all(
    nchar(df$codigo_provincia) == 2 | is.na(df$codigo_provincia),
    paste0("[", label, "] $codigo_provincia debe tener longitud 2 o ser NA")
  )
  stop_if_not_all(
    nchar(df$codigo_municipio) == 3 | is.na(df$codigo_municipio),
    paste0("[", label, "] $codigo_municipio debe tener longitud 3 o ser NA")
  )
  stop_if_not_all(
    nchar(df$codigo_distrito) == 2 | is.na(df$codigo_distrito),
    paste0("[", label, "] $codigo_distrito debe tener longitud 2 o ser NA")
  )
  stop_if_not_all(
    nchar(df$codigo_seccion) == 4 | is.na(df$codigo_seccion),
    paste0("[", label, "] $codigo_seccion debe tener longitud 4 o ser NA")
  )
  stop_if_not_all(
    nchar(df$codigo_completo) <= 13 | is.na(df$codigo_completo),
    paste0("[", label, "] $codigo_completo debe tener longitud <= 13 o ser NA")
  )
  cols_unique <- c(
    "tipo", "codigo_ccaa", "codigo_provincia", "codigo_municipio",
    "codigo_distrito", "codigo_seccion", "codigo_circunscripcion"
  )
  stop_if_not_all(
    !duplicated(df[, cols_unique]),
    paste0("[", label, "] UNIQUE (tipo, codigos territoriales) violado")
  )
  message(sprintf("  [OK] %s (%d filas)", label, nrow(df)))
  invisible(df)
}

#' @examples
#' validate_dim_partidos_recode(partidos_recode_df)
#' write_csv(partidos_recode_df, "tablas-finales/dimensiones/partidos_recode")
validate_dim_partidos_recode <- function(df, label = "partidos_recode") {
  expected_cols <- c("id", "partido_recode", "agrupacion", "color")
  stop_if_not_all(
    setequal(names(df), expected_cols),
    paste0(
      "[", label, "] Columnas inesperadas o faltantes: ",
      paste(setdiff(
        union(names(df), expected_cols),
        intersect(names(df), expected_cols)
      ), collapse = ", ")
    )
  )
  stop_if_not_all(
    !is.na(df$partido_recode) & df$partido_recode != "",
    paste0("[", label, "] $partido_recode no debe estar vacío")
  )
  stop_if_not_all(
    is.numeric(df$id),
    paste0("[", label, "] $id debe ser numérico")
  )
  stop_if_not_all(
    !duplicated(df$id),
    paste0("[", label, "] $id debe ser único")
  )
  stop_if_not_all(
    !duplicated(df$partido_recode),
    paste0("[", label, "] $partido_recode debe ser único (UNIQUE constraint)")
  )
  message(sprintf("  [OK] %s (%d filas)", label, nrow(df)))
  invisible(df)
}

#' @examples
#' validate_dim_partidos(partidos_df)
#' write_csv(partidos_df, "tablas-finales/dimensiones/partidos")
validate_dim_partidos <- function(df, label = "partidos") {
  expected_cols <- c("id", "partido_recode_id", "denominacion", "siglas")
  stop_if_not_all(
    setequal(names(df), expected_cols),
    paste0(
      "[", label, "] Columnas inesperadas o faltantes: ",
      paste(setdiff(
        union(names(df), expected_cols),
        intersect(names(df), expected_cols)
      ), collapse = ", ")
    )
  )
  stop_if_not_all(
    is.numeric(df$id),
    paste0("[", label, "] $id debe ser numérico")
  )
  stop_if_not_all(
    !duplicated(df$id),
    paste0("[", label, "] $id debe ser único")
  )
  stop_if_not_all(
    !duplicated(df[, c("siglas", "denominacion")]),
    paste0("[", label, "] UNIQUE (siglas, denominacion) violado")
  )
  message(sprintf("  [OK] %s (%d filas)", label, nrow(df)))
  invisible(df)
}

#' @examples
#' validate_hechos_info(info_df)
#' saveRDS(info_df, "tablas-finales/hechos/info.rds")
validate_hechos_info <- function(df, label = "hechos/info") {
  expected_cols <- c(
    "eleccion_id", "territorio_id", "censo_ine", "participacion_1",
    "participacion_2", "votos_validos", "abstenciones",
    "votos_blancos", "votos_nulos", "nrepresentantes"
  )
  stop_if_not_all(
    all(expected_cols %in% names(df)),
    paste0(
      "[", label, "] Columnas faltantes: ",
      paste(expected_cols[!expected_cols %in% names(df)], collapse = ", ")
    )
  )
  stop_if_not_all(
    is.numeric(df$eleccion_id),
    paste0("[", label, "] $eleccion_id debe ser numérico")
  )
  stop_if_not_all(
    is.numeric(df$territorio_id),
    paste0("[", label, "] $territorio_id debe ser numérico")
  )
  stop_if_not_all(
    !duplicated(df[, c("eleccion_id", "territorio_id")]),
    paste0("[", label, "] UNIQUE (eleccion_id, territorio_id) violado")
  )
  message(sprintf("  [OK] %s (%d filas)", label, nrow(df)))
  invisible(df)
}

#' @examples
#' validate_hechos_votos(votos_df)
#' saveRDS(votos_df, "tablas-finales/hechos/votos.rds")
validate_hechos_votos <- function(df, label = "hechos/votos") {
  expected_cols <- c(
    "eleccion_id", "territorio_id", "partido_id", "votos", "representantes"
  )
  stop_if_not_all(
    all(expected_cols %in% names(df)),
    paste0(
      "[", label, "] Columnas faltantes: ",
      paste(expected_cols[!expected_cols %in% names(df)], collapse = ", ")
    )
  )
  stop_if_not_all(
    is.numeric(df$eleccion_id),
    paste0("[", label, "] $eleccion_id debe ser numérico")
  )
  stop_if_not_all(
    is.numeric(df$territorio_id),
    paste0("[", label, "] $territorio_id debe ser numérico")
  )
  stop_if_not_all(
    is.numeric(df$partido_id),
    paste0("[", label, "] $partido_id debe ser numérico")
  )
  stop_if_not_all(
    !duplicated(df[, c("eleccion_id", "territorio_id", "partido_id")]),
    paste0("[", label, "] UNIQUE (eleccion_id, territorio_id, partido_id) violado")
  )
  message(sprintf("  [OK] %s (%d filas)", label, nrow(df)))
  invisible(df)
}

# ---------------------------------------------------------------------------
# Batch runners — accept optional dfs; fall back to reading from files
# ---------------------------------------------------------------------------

#' Validate all dimension tables.
#'
#' Each parameter accepts either a dataframe (in-memory) or NULL (reads from
#' the default file path in tablas-finales/dimensiones/).
#'
#' @examples
#' # Batch mode (reads all files):
#' run_dimension_checks()
#'
#' # After building elecciones in memory:
#' run_dimension_checks(elecciones = elecciones_df)
run_dimension_checks <- function(
    tipos_eleccion = NULL,
    elecciones = NULL,
    territorios = NULL,
    partidos_recode = NULL,
    partidos = NULL) {
  message("[VALIDACION] Comprobando dimensiones...")

  tipos_df <- .resolve_df(tipos_eleccion,
    "tablas-finales/dimensiones/tipos_eleccion", readr::read_csv,
    show_col_types = FALSE
  )
  elec_df <- .resolve_df(elecciones,
    "tablas-finales/dimensiones/elecciones", readr::read_csv,
    show_col_types = FALSE, col_types = cols(fecha = col_date())
  )
  terr_df <- .resolve_df(territorios,
    "tablas-finales/dimensiones/territorios", readr::read_csv,
    show_col_types = FALSE
  )
  precode_df <- .resolve_df(partidos_recode,
    "tablas-finales/dimensiones/partidos_recode", readr::read_csv,
    show_col_types = FALSE, na = "NNNNAAAA"
  )
  part_df <- .resolve_df(partidos,
    "tablas-finales/dimensiones/partidos", readr::read_csv,
    show_col_types = FALSE, na = "NNNNAAAA"
  )

  validate_dim_tipos_eleccion(tipos_df)
  validate_dim_elecciones(elec_df)
  validate_dim_territorios(terr_df)
  validate_dim_partidos_recode(precode_df)
  validate_dim_partidos(part_df)

  # FK: elecciones$tipo_eleccion -> tipos_eleccion$codigo
  stop_if_not_all(
    elec_df$tipo_eleccion %in% tipos_df$codigo,
    "FK: elecciones$tipo_eleccion debe existir en tipos_eleccion$codigo"
  )
  # FK: partidos$partido_recode_id -> partidos_recode$id  (NAs permitidos)
  bad_recode <- !is.na(part_df$partido_recode_id) &
    !(part_df$partido_recode_id %in% precode_df$id)
  stop_if_not_all(
    !bad_recode,
    "FK: partidos$partido_recode_id debe existir en partidos_recode$id"
  )

  message("[VALIDACION] Dimensiones OK.")
}

#' Validate fact tables, optionally also checking FK integrity against dimensions.
#'
#' Each parameter accepts either a dataframe or NULL (reads from the default
#' file path). Dimension parameters (elecciones, territorios, partidos) are
#' only needed for FK checks; if all three are NULL, FK checks are skipped
#' when the dimension files do not exist yet.
#'
#' @examples
#' # Batch mode (reads all files):
#' run_fact_checks()
#'
#' # After building info and votos in memory (e.g. in 02-bind-hechos.R):
#' run_fact_checks(info = info_df, votos = votos_df)
run_fact_checks <- function(
    info = NULL,
    votos = NULL,
    elecciones = NULL,
    territorios = NULL,
    partidos = NULL) {
  message("[VALIDACION] Comprobando hechos...")

  info_df <- .resolve_df(info, "tablas-finales/hechos/info.rds", readRDS)
  votos_df <- .resolve_df(votos, "tablas-finales/hechos/votos.rds", readRDS)

  validate_hechos_info(info_df)
  validate_hechos_votos(votos_df)

  # FK checks — load dimension tables only if needed / available
  dim_paths_exist <-
    file.exists("tablas-finales/dimensiones/elecciones") &&
      file.exists("tablas-finales/dimensiones/territorios") &&
      file.exists("tablas-finales/dimensiones/partidos")

  if (is.null(elecciones) && is.null(territorios) && is.null(partidos) && !dim_paths_exist) {
    message("[FK] Tablas de dimensiones no encontradas; omitiendo checks de FK.")
    message("     Genera primero las dimensiones con code/01-generate-data/dimensiones/.")
  } else {
    elec_df <- .resolve_df(elecciones, "tablas-finales/dimensiones/elecciones",
      readr::read_csv,
      show_col_types = FALSE,
      col_types = cols(fecha = col_date())
    )
    terr_df <- .resolve_df(territorios, "tablas-finales/dimensiones/territorios",
      readr::read_csv,
      show_col_types = FALSE
    )
    part_df <- .resolve_df(partidos, "tablas-finales/dimensiones/partidos",
      readr::read_csv,
      show_col_types = FALSE, na = "NNNNAAAA"
    )

    stop_if_not_all(
      info_df$eleccion_id %in% elec_df$id,
      "FK: info$eleccion_id debe existir en elecciones$id"
    )
    stop_if_not_all(
      info_df$territorio_id %in% terr_df$id,
      "FK: info$territorio_id debe existir en territorios$id"
    )
    stop_if_not_all(
      votos_df$eleccion_id %in% elec_df$id,
      "FK: votos$eleccion_id debe existir en elecciones$id"
    )
    stop_if_not_all(
      votos_df$territorio_id %in% terr_df$id,
      "FK: votos$territorio_id debe existir en territorios$id"
    )
    stop_if_not_all(
      votos_df$partido_id %in% part_df$id,
      "FK: votos$partido_id debe existir en partidos$id"
    )

    message("[FK] Foreign keys OK.")
  }

  message("[VALIDACION] Hechos OK.")
}

if (sys.nframe() == 0) {
  run_dimension_checks()
  run_fact_checks()
}
