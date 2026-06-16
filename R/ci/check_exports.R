# check_exports.R
# Valida los artefactos generados por R/04-export/export-descargas.R.

library(DBI)
library(RSQLite)
library(arrow)
library(readr)

required_zips <- c(
    parquet = "descargas/eleccionesdb_parquet.zip",
    sqlite = "descargas/eleccionesdb_sqlite.zip",
    csv = "descargas/eleccionesdb_csv.zip"
)

stop_if_missing <- function(path) {
    if (!file.exists(path)) {
        stop("No existe el artefacto requerido: ", path, call. = FALSE)
    }
    if (file.info(path)$size <= 0) {
        stop("El artefacto esta vacio: ", path, call. = FALSE)
    }
}

extract_zip <- function(path) {
    listing <- utils::unzip(path, list = TRUE)
    if (nrow(listing) == 0) {
        stop("El ZIP no contiene archivos: ", path, call. = FALSE)
    }

    dest <- tempfile(pattern = paste0("check_", tools::file_path_sans_ext(basename(path))))
    dir.create(dest, recursive = TRUE)
    utils::unzip(path, exdir = dest)
    dest
}

for (path in required_zips) {
    stop_if_missing(path)
}

message("[ci] ZIPs requeridos presentes")

sqlite_dir <- extract_zip(required_zips[["sqlite"]])
sqlite_files <- list.files(
    sqlite_dir,
    pattern = "eleccionesdb\\.sqlite$",
    recursive = TRUE,
    full.names = TRUE
)
if (length(sqlite_files) != 1) {
    stop("No se encontro un unico SQLite dentro del ZIP", call. = FALSE)
}

con <- DBI::dbConnect(RSQLite::SQLite(), sqlite_files[[1]])
on.exit(DBI::dbDisconnect(con), add = TRUE)

integrity <- DBI::dbGetQuery(con, "PRAGMA integrity_check")[[1]]
if (!identical(integrity, "ok")) {
    stop("SQLite integrity_check fallo: ", integrity, call. = FALSE)
}

required_tables <- c(
    "tipos_eleccion",
    "elecciones",
    "elecciones_fuentes",
    "territorios",
    "partidos_recode",
    "partidos",
    "resumen_territorial",
    "votos_territoriales"
)
tables <- DBI::dbGetQuery(
    con,
    "SELECT name FROM sqlite_master WHERE type = 'table'"
)$name
missing_tables <- setdiff(required_tables, tables)
if (length(missing_tables) > 0) {
    stop("Faltan tablas SQLite: ", paste(missing_tables, collapse = ", "), call. = FALSE)
}

row_counts <- vapply(
    required_tables,
    function(tbl) {
        DBI::dbGetQuery(con, sprintf("SELECT COUNT(*) AS n FROM %s", tbl))$n[[1]]
    },
    numeric(1)
)
if (any(row_counts <= 0)) {
    stop(
        "Hay tablas SQLite sin filas: ",
        paste(names(row_counts)[row_counts <= 0], collapse = ", "),
        call. = FALSE
    )
}

message("[ci] SQLite valido")

parquet_dir <- extract_zip(required_zips[["parquet"]])
parquet_files <- list.files(parquet_dir, pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)
required_parquet <- c(
    "tipos_eleccion.parquet",
    "elecciones.parquet",
    "elecciones_fuentes.parquet",
    "territorios.parquet",
    "partidos_recode.parquet",
    "partidos.parquet",
    "resumen_territorial.parquet",
    "votos_territoriales.parquet"
)
missing_parquet <- setdiff(required_parquet, basename(parquet_files))
if (length(missing_parquet) > 0) {
    stop("Faltan Parquet: ", paste(missing_parquet, collapse = ", "), call. = FALSE)
}

sample_parquet <- parquet_files[basename(parquet_files) == "elecciones.parquet"][[1]]
sample_tbl <- arrow::read_parquet(sample_parquet)
if (nrow(sample_tbl) == 0) {
    stop("El Parquet de elecciones no contiene filas", call. = FALSE)
}

message("[ci] Parquet validos")

csv_dir <- extract_zip(required_zips[["csv"]])
csv_files <- list.files(csv_dir, pattern = "\\.csv$", recursive = TRUE, full.names = TRUE)
required_csv <- c("resumen_territorial.csv", "votos_territoriales.csv")
missing_csv <- setdiff(required_csv, basename(csv_files))
if (length(missing_csv) > 0) {
    stop("Faltan CSV: ", paste(missing_csv, collapse = ", "), call. = FALSE)
}

for (csv_name in required_csv) {
    csv_path <- csv_files[basename(csv_files) == csv_name][[1]]
    sample_csv <- readr::read_csv(csv_path, n_max = 10, show_col_types = FALSE)
    if (nrow(sample_csv) == 0) {
        stop("El CSV no contiene filas: ", csv_name, call. = FALSE)
    }
}

message("[ci] CSV validos")
message("[ci] Exportaciones OK")
