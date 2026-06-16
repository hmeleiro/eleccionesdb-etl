# Script: download_data_raw.R
# Descarga todos los archivos listados en el indice publico a data-raw/.
# Por defecto usa docs-site/data_index.csv; DATA_INDEX_URL permite usar otro
# indice local o remoto en CI/CD.

library(readr)
library(purrr)
library(fs)
library(httr)

default_index_path <- "docs-site/data_index.csv"
index_source <- Sys.getenv("DATA_INDEX_URL", unset = "")
if (!nzchar(index_source)) {
    index_source <- default_index_path
}

is_remote <- function(path) {
    grepl("^https?://", path, ignore.case = TRUE)
}

read_data_index <- function(source) {
    if (is_remote(source)) {
        temp_idx <- tempfile(fileext = ".csv")
        on.exit(unlink(temp_idx), add = TRUE)
        resp <- httr::GET(source, httr::write_disk(temp_idx, overwrite = TRUE))
        if (httr::status_code(resp) != 200) {
            stop(
                "No se pudo descargar el indice: ", source,
                " (status ", httr::status_code(resp), ")",
                call. = FALSE
            )
        }
        return(readr::read_csv(temp_idx, show_col_types = FALSE))
    }

    if (!file_exists(source)) {
        stop("No se encontro el indice de datos: ", source, call. = FALSE)
    }

    readr::read_csv(source, show_col_types = FALSE)
}

normalise_data_raw_key <- function(key) {
    key <- gsub("\\\\", "/", key)
    key <- sub("^/+", "", key)

    if (grepl("data-raw/", key, fixed = TRUE)) {
        key <- sub("^.*data-raw/", "", key)
    }

    key
}

index <- read_data_index(index_source)

required_cols <- c("key", "url")
missing_cols <- setdiff(required_cols, names(index))
if (length(missing_cols) > 0) {
    stop(
        "El indice de datos no contiene columnas requeridas: ",
        paste(missing_cols, collapse = ", "),
        call. = FALSE
    )
}

message("Indice de datos: ", index_source)

download_one <- function(key, url) {
    dest <- file.path("data-raw", normalise_data_raw_key(key))
    dir_create(dirname(dest))

    if (!file_exists(dest)) {
        resp <- httr::GET(url, httr::write_disk(dest, overwrite = TRUE))
        if (httr::status_code(resp) == 200) {
            message("Descargado: ", key)
            return(TRUE)
        }

        if (file_exists(dest)) file_delete(dest)
        message("Fallo: ", key, " (status ", httr::status_code(resp), ")")
        return(FALSE)
    }

    message("Ya existe: ", key)
    NA
}

res <- purrr::map2_lgl(index$key, index$url, download_one)

cat(
    sum(res, na.rm = TRUE), "descargados,",
    sum(is.na(res)), "ya existian,",
    sum(!res, na.rm = TRUE), "fallos\n"
)
