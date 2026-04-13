library(dplyr)
library(future)
library(furrr)
library(readr)
library(data.table)
library(httr)
library(tibble)
library(tidyr)

# Defino la ruta del archivo de salida
OUTPUT_FILE <- "data-raw/hechos/01-andalucia/resumen_provincias.csv"
NOMENCLATOR <- "data-raw/hechos/01-andalucia/nomenclators/nomenclator_secciones.rds"

# Cargo las funciones y los datos necesarios
source("R/01-generate-data/hechos/01-andalucia/functions.R", encoding = "UTF-8")

nomenclator <- readRDS(NOMENCLATOR) %>%
  select(clave_eleccion, clave_provincia) %>%
  distinct()


# Excluyo del nomenclator las secciones que ya tengo
existing <- tryCatch(
  {
    read_csv(OUTPUT_FILE, show_col_types = F) %>%
      transmute(
        clave_eleccion = as.integer(fconvocatoria_clave),
        clave_provincia = as.integer(provincia_clave)
      ) %>%
      distinct()
  },
  error = function(e) {
    message("Error: ", e$message)
    return(NULL)
  }
)

if (!is.null(existing)) {
  nomenclator <- anti_join(
    nomenclator, existing,
    by = join_by(clave_eleccion, clave_provincia)
  )
}


# Defino la función que obtiene los resultados de una sección
get_resumen <- function(clave_eleccion, clave_provincia) {
  endpoint <- "resumen/provincia"

  params <- list(
    cautonoma = 1,
    tconvocatoria = 5,
    provincia = clave_provincia,
    fconvocatoria = clave_eleccion
  )

  res <- get(endpoint, query = params)

  if (res$status_code != 200) {
    message("Error: ", res$status_code, " | URL:", res$url)
    return(NULL)
  }

  out <-
    tryCatch(
      {
        res <- content(res)

        info <- res %>%
          as_tibble() %>%
          select(abstenciones:nummesas) %>%
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

# Establezco el plan de ejecución en paralelo
n_cores <- availableCores() - 1
plan(multisession, workers = n_cores)

# Go! Go! Go!
system.time(
  data <-
    nomenclator %>%
    select(clave_eleccion, clave_provincia) %>%
    # dplyr::slice_head(n = 100) %>%
    future_pmap_dfr(get_resumen)
)

dir.create(dirname(OUTPUT_FILE), recursive = TRUE, showWarnings = FALSE)
write_csv(data, OUTPUT_FILE)
