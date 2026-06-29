# check_exports.R
# Valida los artefactos generados por R/04-export/export-descargas.R.

library(DBI)
library(RSQLite)
library(arrow)
library(readr)
library(digest)
library(jsonlite)

required_zips <- c(
    parquet = "descargas/eleccionesdb_parquet.zip",
    sqlite = "descargas/eleccionesdb_sqlite.zip",
    csv = "descargas/eleccionesdb_csv.zip"
)
sqlite_manifest_path <- "descargas/eleccionesdb_sqlite.json"
sqlite_download_url <- paste0(
    "https://data.spainelectoralproject.com/eleccionesdb-etl/descargas/",
    "eleccionesdb_sqlite.zip"
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
stop_if_missing(sqlite_manifest_path)

message("[ci] ZIPs y manifiesto requeridos presentes")

sha256_file <- function(path) {
    tolower(digest::digest(file = path, algo = "sha256", serialize = FALSE))
}

sqlite_manifest <- jsonlite::read_json(sqlite_manifest_path, simplifyVector = TRUE)
required_manifest_fields <- c(
    "schema_version",
    "generated_at",
    "url",
    "archive_size",
    "archive_sha256",
    "database_filename",
    "database_size",
    "database_sha256"
)
missing_manifest_fields <- setdiff(required_manifest_fields, names(sqlite_manifest))
if (length(missing_manifest_fields) > 0) {
    stop(
        "Faltan campos en el manifiesto SQLite: ",
        paste(missing_manifest_fields, collapse = ", "),
        call. = FALSE
    )
}
if (!identical(as.integer(sqlite_manifest$schema_version), 2L)) {
    stop("El manifiesto SQLite no usa schema_version 2", call. = FALSE)
}
if (!identical(sqlite_manifest$url, sqlite_download_url)) {
    stop("La URL del manifiesto SQLite no es la URL publica esperada", call. = FALSE)
}
if (!grepl(
    "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
    sqlite_manifest$generated_at
)) {
    stop("generated_at no es una fecha UTC valida", call. = FALSE)
}
manifest_hashes <- c(
    sqlite_manifest$archive_sha256,
    sqlite_manifest$database_sha256
)
if (any(!grepl("^[0-9a-f]{64}$", manifest_hashes))) {
    stop("El manifiesto SQLite contiene checksums SHA-256 invalidos", call. = FALSE)
}

sqlite_zip <- required_zips[["sqlite"]]
if (!identical(
    as.numeric(file.info(sqlite_zip)$size),
    as.numeric(sqlite_manifest$archive_size)
)) {
    stop("El tamano del ZIP SQLite no coincide con el manifiesto", call. = FALSE)
}
if (!identical(sha256_file(sqlite_zip), sqlite_manifest$archive_sha256)) {
    stop("El SHA-256 del ZIP SQLite no coincide con el manifiesto", call. = FALSE)
}

sqlite_listing <- utils::unzip(sqlite_zip, list = TRUE)
if (
    nrow(sqlite_listing) != 1 ||
    !identical(sqlite_listing$Name[[1]], sqlite_manifest$database_filename) ||
    !identical(
        basename(sqlite_manifest$database_filename),
        sqlite_manifest$database_filename
    )
) {
    stop(
        "El ZIP SQLite debe contener unicamente el fichero declarado en el manifiesto",
        call. = FALSE
    )
}

sqlite_dir <- extract_zip(sqlite_zip)
sqlite_files <- list.files(
    sqlite_dir,
    pattern = "eleccionesdb\\.sqlite$",
    recursive = TRUE,
    full.names = TRUE
)
if (length(sqlite_files) != 1) {
    stop("No se encontro un unico SQLite dentro del ZIP", call. = FALSE)
}
if (!identical(
    as.numeric(file.info(sqlite_files[[1]])$size),
    as.numeric(sqlite_manifest$database_size)
)) {
    stop("El tamano del SQLite no coincide con el manifiesto", call. = FALSE)
}
if (!identical(sha256_file(sqlite_files[[1]]), sqlite_manifest$database_sha256)) {
    stop("El SHA-256 del SQLite no coincide con el manifiesto", call. = FALSE)
}

con <- DBI::dbConnect(
    RSQLite::SQLite(),
    sqlite_files[[1]],
    flags = RSQLite::SQLITE_RO
)
on.exit(DBI::dbDisconnect(con), add = TRUE)

schema_version <- DBI::dbGetQuery(con, "PRAGMA user_version")[[1]][[1]]
if (!identical(as.integer(schema_version), 2L)) {
    stop("SQLite no usa PRAGMA user_version = 2", call. = FALSE)
}

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

required_partidos_recode_columns <- c(
    "id",
    "partido_recode",
    "agrupacion",
    "bloque",
    "color",
    "color_pastel",
    "color_oscuro"
)
missing_partidos_recode_columns <- setdiff(
    required_partidos_recode_columns,
    DBI::dbListFields(con, "partidos_recode")
)
if (length(missing_partidos_recode_columns) > 0) {
    stop(
        "Faltan columnas en partidos_recode: ",
        paste(missing_partidos_recode_columns, collapse = ", "),
        call. = FALSE
    )
}

invalid_years <- DBI::dbGetQuery(
    con,
    paste(
        "SELECT DISTINCT year FROM elecciones",
        "WHERE typeof(year) <> 'text'",
        "OR year NOT GLOB '[0-9][0-9][0-9][0-9]'"
    )
)
if (nrow(invalid_years) > 0) {
    stop(
        "SQLite contiene valores de year que no son texto de cuatro digitos: ",
        paste(invalid_years$year, collapse = ", "),
        call. = FALSE
  )
}

invalid_aggregate_circunscripciones <- DBI::dbGetQuery(
    con,
    paste(
        "SELECT tipo, codigo_ccaa, codigo_provincia, codigo_circunscripcion",
        "FROM territorios",
        "WHERE tipo IN ('ccaa', 'provincia')",
        "AND (codigo_circunscripcion IS NULL OR codigo_circunscripcion <> '99')"
    )
)
if (nrow(invalid_aggregate_circunscripciones) > 0) {
    stop(
        "SQLite contiene territorios ccaa/provincia sin codigo_circunscripcion = '99'",
        call. = FALSE
    )
}

invalid_real_circunscripciones <- DBI::dbGetQuery(
    con,
    paste(
        "SELECT tipo, codigo_ccaa, codigo_provincia, codigo_circunscripcion",
        "FROM territorios",
        "WHERE tipo NOT IN ('ccaa', 'provincia')",
        "AND (codigo_circunscripcion IS NULL OR codigo_circunscripcion = '99')"
    )
)
if (nrow(invalid_real_circunscripciones) > 0) {
    stop(
        "SQLite ha sustituido por NULL/'99' algun codigo_circunscripcion real",
        call. = FALSE
    )
}

expected_circunscripciones <- readr::read_csv(
    "data-raw/codigos_territorios/circunscripciones.csv",
    show_col_types = FALSE,
    col_types = readr::cols(.default = "c")
)
sqlite_circunscripciones <- DBI::dbGetQuery(
    con,
    paste(
        "SELECT codigo_ccaa, codigo_provincia, codigo_circunscripcion, nombre",
        "FROM territorios WHERE tipo = 'circunscripcion'"
    )
)
circunscripcion_key <- function(df) {
    do.call(paste, c(df, sep = "\r"))
}
if (!identical(
    sort(circunscripcion_key(expected_circunscripciones)),
    sort(circunscripcion_key(sqlite_circunscripciones))
)) {
    stop(
        "Los codigos de circunscripcion reales del SQLite no coinciden con su definicion",
        call. = FALSE
    )
}

required_resumen_columns <- c(
    "id",
    "eleccion_id",
    "territorio_id",
    "censo_ine",
    "participacion_1",
    "participacion_2",
    "participacion_3",
    "votos_validos",
    "abstenciones",
    "votos_blancos",
    "votos_nulos",
    "nrepresentantes"
)
missing_resumen_columns <- setdiff(
    required_resumen_columns,
    DBI::dbListFields(con, "resumen_territorial")
)
if (length(missing_resumen_columns) > 0) {
    stop(
        "Faltan columnas en resumen_territorial: ",
        paste(missing_resumen_columns, collapse = ", "),
        call. = FALSE
    )
}

foreign_key_violations <- DBI::dbGetQuery(con, "PRAGMA foreign_key_check")
if (nrow(foreign_key_violations) > 0) {
    stop("SQLite contiene violaciones de claves foraneas", call. = FALSE)
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
