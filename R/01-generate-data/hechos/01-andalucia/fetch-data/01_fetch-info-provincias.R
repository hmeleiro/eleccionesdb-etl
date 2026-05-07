library(dplyr)
library(readr)
library(data.table)
library(httr)
library(purrr)
library(tibble)
library(tidyr)

# Defino la ruta del archivo de salida
OUTPUT_FILE <- "data-raw/hechos/01-andalucia/resumen_provincias.csv"
OUTPUT_FILE_CERA <- "data-raw/hechos/01-andalucia/resumen_provincias_cera.csv"
NOMENCLATOR <- "data-raw/hechos/01-andalucia/nomenclators/nomenclator_secciones.rds"

# Cargo las funciones y los datos necesarios
source("R/01-generate-data/hechos/01-andalucia/fetch-data/functions.R", encoding = "UTF-8")

nomenclator <- readRDS(NOMENCLATOR) %>%
  select(clave_eleccion, clave_provincia) %>%
  distinct()

# Defino la funcion que obtiene el resumen de una provincia
get_resumen <- function(clave_eleccion, clave_provincia, endpoint) {
  params <- list(
    cautonoma = 1,
    tconvocatoria = 5,
    provincia = clave_provincia,
    fconvocatoria = clave_eleccion
  )

  res <- get(endpoint, query = params)

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

        res[sapply(res, is.null)] <- NULL

        info <- res %>%
          as_tibble() %>%
          select(any_of(c(
            "abstenciones", "votosblanco", "votosnulos", "votosvalidos",
            "votostotales", "censo_ine", "nummesas"
          ))) %>%
          distinct()

        out <- cbind(
          parse_idescrutinio(res$idescrutinio),
          info
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

fetch_resumen <- function(output_file, endpoint) {
  nomenclator_pending <- nomenclator

  if (grepl("rausentes", endpoint)) {
    nomenclator_pending <- nomenclator_pending %>%
      filter(substr(as.character(clave_eleccion), 1, 4) != "1982")
  }

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
    pmap(
      function(clave_eleccion, clave_provincia) {
        get_resumen(clave_eleccion, clave_provincia, endpoint)
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
  fetch_resumen(OUTPUT_FILE, "resumen/provincia")
)

system.time(
  fetch_resumen(OUTPUT_FILE_CERA, "resumen/rausentes/provincia")
)
