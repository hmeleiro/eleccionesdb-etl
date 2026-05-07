library(dplyr)
library(future)
library(furrr)
library(readr)
library(data.table)

# Defino la ruta del archivo de salida
OUTPUT_FILE <- "data-raw/hechos/01-andalucia/escrutinio_secciones.csv"
NOMENCLATOR <- "data-raw/hechos/01-andalucia/nomenclators/nomenclator_secciones.rds"

# Cargo las funciones y los datos necesarios
source("R/01-generate-data/hechos/01-andalucia/fetch-data/functions.R", encoding = "UTF-8")

nomenclator_secciones <- readRDS(NOMENCLATOR)

# Excluyo del nomenclator_secciones las secciones que ya tengo
existing <- tryCatch(
  {
    fread(OUTPUT_FILE, fill = T, data.table = F) %>%
      transmute(
        clave_eleccion = as.integer(fconvocatoria_clave),
        clave_provincia = as.integer(provincia_clave),
        clave_municipio = as.integer(municipio_clave),
        clave_distrito = as.integer(distrito_clave),
        clave_seccion = as.integer(seccion_clave)
      ) %>%
      distinct()
  },
  error = function(e) {
    message("Error: ", e$message)
    return(NULL)
  }
)

if (!is.null(existing)) {
  nomenclator_secciones <- anti_join(
    nomenclator_secciones, existing,
    by = join_by(
      clave_eleccion, clave_provincia, clave_municipio,
      clave_distrito, clave_seccion
    )
  )
}

# Defino la función que obtiene los resultados de una sección
get_results <- function(clave_eleccion, clave_provincia,
                        clave_municipio, clave_distrito, seccion) {
  endpoint <- "escrutinio/ambito/seccion"

  params <- list(
    cautonoma = 1,
    tconvocatoria = 5,
    provincia = clave_provincia,
    municipio = clave_municipio,
    distrito = clave_distrito,
    seccion = seccion,
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

# Establezco el plan de ejecución en paralelo
n_cores <- availableCores() - 1
plan(multisession, workers = n_cores)

# Go! Go! Go!
system.time(
  data <-
    nomenclator_secciones %>%
    select(clave_eleccion, clave_provincia, clave_municipio, clave_distrito, seccion) %>%
    # dplyr::slice_head(n = 100) %>%
    future_pmap_dfr(get_results)
)

dir.create(dirname(OUTPUT_FILE), recursive = TRUE, showWarnings = FALSE)
write_csv(data, OUTPUT_FILE)
