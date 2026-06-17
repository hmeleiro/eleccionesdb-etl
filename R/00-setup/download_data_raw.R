# Script: download_data_raw.R
# Restores data-raw/ from the versioned manifest or a DATA_INDEX_URL override.

library(readr)
library(fs)
library(httr)

default_manifest_path <- "data-manifest.csv"
data_dir <- "data-raw"

index_source <- Sys.getenv("DATA_INDEX_URL", unset = "")
if (!nzchar(index_source)) {
    index_source <- default_manifest_path
}

is_remote <- function(path) {
    grepl("^https?://", path, ignore.case = TRUE)
}

encode_remote_url <- function(url) {
    utils::URLencode(url)
}

format_bytes <- function(bytes) {
    bytes <- as.numeric(bytes)
    units <- c("B", "KB", "MB", "GB", "TB")
    unit <- 1
    while (bytes >= 1024 && unit < length(units)) {
        bytes <- bytes / 1024
        unit <- unit + 1
    }
    sprintf("%.2f %s", bytes, units[[unit]])
}

read_data_index <- function(source) {
    if (is_remote(source)) {
        temp_idx <- tempfile(fileext = ".csv")
        on.exit(unlink(temp_idx), add = TRUE)
        resp <- httr::GET(
            encode_remote_url(source),
            httr::write_disk(temp_idx, overwrite = TRUE),
            httr::timeout(120)
        )
        if (httr::status_code(resp) != 200) {
            stop(
                "No se pudo descargar el indice de datos (status ",
                httr::status_code(resp), ").",
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

    key <- sub("^/+", "", key)
    if (!nzchar(key) || grepl("(^|/)\\.\\.(/|$)", key)) {
        stop("Clave insegura o vacia en el manifiesto: ", key, call. = FALSE)
    }

    key
}

prepare_data_index <- function(index) {
    required_cols <- c("key", "url")
    missing_cols <- setdiff(required_cols, names(index))
    if (length(missing_cols) > 0) {
        stop(
            "El indice de datos no contiene columnas requeridas: ",
            paste(missing_cols, collapse = ", "),
            call. = FALSE
        )
    }

    index$key <- as.character(index$key)
    index$url <- as.character(index$url)
    index <- index[
        !is.na(index$key) & nzchar(index$key) &
            !is.na(index$url) & nzchar(index$url),
        ,
        drop = FALSE
    ]

    has_data_raw_prefix <- grepl("data-raw/", index$key, fixed = TRUE)
    if (any(has_data_raw_prefix)) {
        index <- index[has_data_raw_prefix, , drop = FALSE]
    }

    index$relative_key <- vapply(index$key, normalise_data_raw_key, character(1))

    duplicate_keys <- unique(index$relative_key[duplicated(index$relative_key)])
    if (length(duplicate_keys) > 0) {
        stop(
            "El manifiesto contiene claves duplicadas: ",
            paste(utils::head(duplicate_keys, 10), collapse = ", "),
            call. = FALSE
        )
    }

    if (!nrow(index)) {
        stop("El manifiesto no contiene ficheros para data-raw/.", call. = FALSE)
    }

    if ("size" %in% names(index)) {
        index$expected_size <- suppressWarnings(as.numeric(index$size))
    } else {
        index$expected_size <- NA_real_
    }

    index
}

file_matches_manifest <- function(path, expected_size) {
    if (!file_exists(path)) {
        return(FALSE)
    }

    if (!is.na(expected_size)) {
        return(as.numeric(file_info(path)$size) == expected_size)
    }

    TRUE
}

download_one <- function(row) {
    dest <- file.path(data_dir, row$relative_key)
    dir_create(dirname(dest))

    if (file_matches_manifest(dest, row$expected_size)) {
        message("Ya existe: ", row$relative_key)
        return("present")
    }

    if (file_exists(dest)) {
        file_delete(dest)
    }

    resp <- tryCatch(
        httr::GET(
            encode_remote_url(row$url),
            httr::write_disk(dest, overwrite = TRUE),
            httr::timeout(600)
        ),
        error = function(e) {
            message("Fallo: ", row$relative_key, " (", conditionMessage(e), ")")
            NULL
        }
    )

    if (is.null(resp) || httr::status_code(resp) != 200) {
        if (file_exists(dest)) file_delete(dest)
        status <- if (is.null(resp)) "sin respuesta" else paste("status", httr::status_code(resp))
        message("Fallo: ", row$relative_key, " (", status, ")")
        return("failed")
    }

    if (!file_matches_manifest(dest, row$expected_size)) {
        if (file_exists(dest)) file_delete(dest)
        message("Fallo: ", row$relative_key, " (tamano inesperado)")
        return("failed")
    }

    message("Descargado: ", row$relative_key)
    "downloaded"
}

validate_data_raw <- function(index) {
    expected_paths <- file.path(data_dir, index$relative_key)
    missing <- !file_exists(expected_paths)

    size_mismatch <- rep(FALSE, length(expected_paths))
    expected_sizes <- index$expected_size
    check_size <- !missing & !is.na(expected_sizes)
    if (any(check_size)) {
        actual_sizes <- as.numeric(file_info(expected_paths[check_size])$size)
        size_mismatch[check_size] <- actual_sizes != expected_sizes[check_size]
    }

    if (any(missing) || any(size_mismatch)) {
        bad_keys <- index$relative_key[missing | size_mismatch]
        stop(
            "data-raw/ esta incompleto o no coincide con el manifiesto. ",
            "Primeros ficheros afectados: ",
            paste(utils::head(bad_keys, 10), collapse = ", "),
            call. = FALSE
        )
    }

    files <- dir_ls(data_dir, recurse = TRUE, all = TRUE, type = "file")
    total_size <- sum(as.numeric(file_info(files)$size), na.rm = TRUE)

    if (!length(files) || total_size <= 0) {
        stop("data-raw/ esta vacio tras la restauracion.", call. = FALSE)
    }

    message(
        "data-raw/ verificado: ", length(files), " ficheros, ",
        format_bytes(total_size), "."
    )
}

message("Indice de datos: ", index_source)
dir_create(data_dir)
index <- prepare_data_index(read_data_index(index_source))

statuses <- vapply(
    seq_len(nrow(index)),
    function(i) download_one(index[i, , drop = FALSE]),
    character(1)
)

if (any(statuses == "failed")) {
    stop(
        "Fallaron ", sum(statuses == "failed"),
        " descargas de data-raw/.",
        call. = FALSE
    )
}

validate_data_raw(index)

cat(
    sum(statuses == "downloaded"), "descargados,",
    sum(statuses == "present"), "ya existian,",
    sum(statuses == "failed"), "fallos\n"
)
