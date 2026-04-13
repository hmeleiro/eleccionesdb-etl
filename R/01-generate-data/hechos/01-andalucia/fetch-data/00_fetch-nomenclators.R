library(dplyr)
library(httr)
library(rvest)
library(purrr)

source("R/01-generate-data/hechos/01-andalucia/functions.R", encoding = "UTF-8")

DATA_DIR <- "data-raw/hechos/01-andalucia/nomenclators/"

# RECABO LOS NOMENCLATORS DE...

# ...FECHAS DE CONVOCATORIA
res <- get("opciones/fconvocatoria", query = list(tconvocatoria = 5))
nomenclator_elecciones <- map_df(content(res), as_tibble)

# ...PROVINCIAS
res <- get("opciones/provincia", query = list(cautonoma = 1))
nomenclator_provincias <- map_df(content(res), as_tibble)

# ...MUNICIPIOS
nomenclator_municipios <-
  map_df(nomenclator_elecciones$clave, function(clave_eleccion) {
    endpoint <- "opciones/municipio"

    map_df(nomenclator_provincias$clave, function(clave_provincia) {
      params <- list(
        cautonoma = 1,
        provincia = clave_provincia,
        tconvocatoria = 5,
        fconvocatoria = clave_eleccion
      )
      res <- get(endpoint, query = params)

      nomenclator <- map_df(content(res), as_tibble) %>%
        mutate(clave_eleccion, clave_provincia, .before = 1)
      return(nomenclator)
    })
  })

nomenclator_municipios <-
  nomenclator_municipios %>%
  rename(clave_municipio = clave, municipio = valor)

# ...DISTRITOS
nomenclator_distrito <-
  map_df(1:nrow(nomenclator_municipios), function(i) {
    if (i == 1 | i %% 300 == 0) {
      message(i)
    }
    endpoint <- "opciones/distrito"

    municipio <- nomenclator_municipios[i, ]
    clave_eleccion <- municipio$clave_eleccion
    clave_provincia <- municipio$clave_provincia
    clave_municipio <- municipio$clave_municipio

    params <- list(
      cautonoma = 1,
      provincia = clave_provincia,
      tconvocatoria = 5,
      fconvocatoria = clave_eleccion,
      municipio = clave_municipio
    )

    res <- get(endpoint, query = params)
    if (res$status_code == 200) {
      nomenclator <-
        map_df(content(res), as_tibble) %>%
        mutate(
          clave_eleccion, clave_provincia,
          clave_municipio,
          municipio = municipio$municipio,
          .before = 1
        )
    } else {
      return(NULL)
    }

    return(nomenclator)
  })

nomenclator_distrito <-
  nomenclator_distrito %>%
  rename(clave_distrito = clave, distrito = valor)


# ...SECCIONES
nomenclator_secciones <-
  map_df(1:nrow(nomenclator_distrito), function(i) {
    if (i == 1 | i %% 300 == 0) {
      message(i)
    }

    endpoint <- "opciones/seccion"

    distrito <- nomenclator_distrito[i, ]
    clave_eleccion <- distrito$clave_eleccion
    clave_provincia <- distrito$clave_provincia
    clave_municipio <- distrito$clave_municipio
    clave_distrito <- distrito$clave_distrito

    params <- list(
      cautonoma = 1,
      provincia = clave_provincia,
      tconvocatoria = 5,
      fconvocatoria = clave_eleccion,
      municipio = clave_municipio,
      distrito = clave_distrito
    )

    res <- get(endpoint, query = params)
    if (res$status_code == 200) {
      nomenclator <-
        map_df(content(res), as_tibble) %>%
        mutate(
          clave_eleccion, clave_provincia,
          clave_municipio,
          municipio = distrito$municipio,
          clave_distrito,
          .before = 1
        )
    } else {
      return(NULL)
    }

    return(nomenclator)
  })

nomenclator_secciones <-
  nomenclator_secciones %>%
  rename(clave_seccion = clave, seccion = valor)


# GUARDO LOS NOMENCLATORS
dir.create(DATA_DIR, showWarnings = FALSE, recursive = TRUE)

saveRDS(nomenclator_elecciones, paste0(DATA_DIR, "nomenclator_elecciones.rds"))
saveRDS(nomenclator_provincias, paste0(DATA_DIR, "nomenclator_provincias.rds"))
saveRDS(nomenclator_municipios, paste0(DATA_DIR, "nomenclator_municipios.rds"))
saveRDS(nomenclator_distrito, paste0(DATA_DIR, "nomenclator_distrito.rds"))
saveRDS(nomenclator_secciones, paste0(DATA_DIR, "nomenclator_secciones.rds"))
