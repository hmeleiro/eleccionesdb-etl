library(dplyr)
library(readr)
library(data.table)
library(purrr)
library(httr)
library(tibble)
library(tidyr)

# Defino la ruta del archivo de salida
OUTPUT_FILE <- "data-raw/hechos/01-andalucia/escrutinio_provincias.csv"
OUTPUT_FILE_CERA <- "data-raw/hechos/01-andalucia/escrutinio_provincias_cera.csv"
NOMENCLATOR <- "data-raw/hechos/01-andalucia/nomenclators/nomenclator_secciones.rds"

# Cargo las funciones y los datos necesarios
source("R/01-generate-data/hechos/01-andalucia/fetch-data/functions.R", encoding = "UTF-8")

nomenclator <- readRDS(NOMENCLATOR) %>%
  select(clave_eleccion, clave_provincia) %>%
  distinct()

# Defino la funcion que obtiene los resultados de una provincia
get_results <- function(clave_eleccion, clave_provincia, endpoint) {
  params <- list(
    cautonoma = 1,
    tconvocatoria = 5,
    provincia = clave_provincia,
    fconvocatoria = clave_eleccion
  )

  request_fun <- if (grepl("rausentes", endpoint)) get_once else get
  res <- request_fun(endpoint, query = params)

  if (res$status_code == 404) {
    message("Sin datos: ", res$status_code, " | URL:", res$url)
    return(NULL)
  }

  if (res$status_code != 200) {
    message("Error: ", res$status_code, " | URL:", res$url)
    return(NULL)
  }

  out <-
    tryCatch(
      {
        res <- content(res)

        out <- cbind(
          parse_idescrutinio(res$idescrutinio),
          parse_votos(res$votos)
        )

        return(out)
      },
      error = function(e) {
        message("Error: ", e$message)
        return(NULL)
      }
    )

  return(out)
}

fetch_results <- function(output_file, endpoint) {
  nomenclator_pending <- nomenclator


  # Excluyo del nomenclator las provincias que ya tengo
  existing_data <- if (file.exists(output_file)) {
    tryCatch(
      {
      read_csv(output_file, show_col_types = F)
      },
      error = function(e) {
        message("Error: ", e$message)
        return(NULL)
      }
    )
  } else {
    NULL
  }

  if (!is.null(existing_data)) {
    existing <- existing_data %>%
      transmute(
        clave_eleccion = as.integer(fconvocatoria_clave),
        clave_provincia = as.integer(provincia_clave)
      ) %>%
      distinct()

    nomenclator_pending <- anti_join(
      nomenclator_pending, existing,
      by = join_by(clave_eleccion, clave_provincia)
    )
  }

  data <- nomenclator_pending %>%
    select(clave_eleccion, clave_provincia) %>%
    # dplyr::slice_head(n = 100) %>%
    pmap(
      function(clave_eleccion, clave_provincia) {
        get_results(clave_eleccion, clave_provincia, endpoint)
      }
    ) %>%
    compact() %>%
    bind_rows()

  data <- bind_rows(existing_data, data)

  if (nrow(data) == 0 && ncol(data) == 0) {
    message("No hay datos para escribir en ", output_file)
    return(invisible(data))
  }

  dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)
  write_csv(data, output_file)
}

# Go! Go! Go!
system.time(
  fetch_results(OUTPUT_FILE, "escrutinio/ambito/provincia")
)

system.time(
  fetch_results(OUTPUT_FILE_CERA, "escrutinio/ambito/rausentes/provincia")
)
