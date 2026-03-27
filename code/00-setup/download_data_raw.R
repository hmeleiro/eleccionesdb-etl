# Script: download_data_raw.R
# Descarga todos los archivos listados en el índice público a data-raw/

library(readr)
library(purrr)
library(fs)
library(httr)

# URL pública del índice (ajusta si es necesario)
index_url <- "https://TU_DOMINIO_O_RUTA/docs-site/data_index.csv"

# Descargar y leer el índice
temp_idx <- tempfile(fileext = ".csv")
download.file(index_url, temp_idx, quiet = TRUE)
index <- readr::read_csv(temp_idx, show_col_types = FALSE)

# Descargar cada archivo
download_one <- function(key, url) {
    dest <- file.path("data-raw", key)
    dir_create(dirname(dest))
    if (!file_exists(dest)) {
        resp <- httr::GET(url, httr::write_disk(dest, overwrite = TRUE))
        if (httr::status_code(resp) == 200) {
            message("Descargado: ", key)
            return(TRUE)
        } else {
            message("Fallo: ", key, " (status ", httr::status_code(resp), ")")
            return(FALSE)
        }
    } else {
        message("Ya existe: ", key)
        return(NA)
    }
}

res <- purrr::map2_lgl(index$key, index$url, download_one)

cat(sum(res, na.rm = TRUE), "descargados,", sum(is.na(res)), "ya existían,", sum(!res, na.rm = TRUE), "fallos\n")
