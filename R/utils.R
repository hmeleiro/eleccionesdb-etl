library(readxl)
library(DBI)

# Normalize a character vector for party matching: lowercase + collapse whitespace
normalize_lower <- function(x) tolower(trimws(gsub("\\s+", " ", x)))

connect <- function() {
  dotenv::load_dot_env()
  con <- dbConnect(
    RPostgreSQL::PostgreSQL(),
    dbname = Sys.getenv("DB_NAME"),
    host = Sys.getenv("DB_HOST"),
    port = Sys.getenv("DB_PORT"),
    user = Sys.getenv("DB_USER"),
    password = Sys.getenv("DB_PASSWORD")
  )
  return(con)
}

get_nrepresentantes <- function() {
  elecciones_id <-
    read_csv("tablas-finales/dimensiones/elecciones", show_col_types = F) %>%
    select(year, mes, codigo_ccaa, tipo_eleccion, eleccion_id = id) %>%
    mutate(year = as.character(year))

  territorios <- read_csv("tablas-finales/dimensiones/territorios", show_col_types = F)

  territorios_prov <- territorios %>%
    filter(tipo == "provincia") %>%
    select(codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion,
      territorio_id = id
    )

  territorios_circ <- territorios %>%
    filter(tipo == "circunscripcion") %>%
    select(codigo_circunscripcion, codigo_municipio, codigo_distrito,
      codigo_seccion,
      territorio_id = id
    )

  territorios_muni <- territorios %>%
    filter(tipo == "municipio") %>%
    select(codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion,
      territorio_id = id
    )

  nrepresentantes_raw <- read_xlsx("data-raw/representantes/nrepresentantes_prov.xlsx") %>%
    mutate(codigo_municipio = "999", codigo_distrito = "99", codigo_seccion = "9999")

  # Split prov-level data: circunscripcion (3-char) vs provincia (2-char)
  nrep_circ <- nrepresentantes_raw %>%
    filter(nchar(codigo_circunscripcion) >= 3) %>%
    mutate(codigo_ccaa = ifelse(tipo_eleccion %in% c("G", "L"), "99", codigo_ccaa)) %>%
    left_join(elecciones_id, by = join_by(year, mes, codigo_ccaa, tipo_eleccion)) %>%
    left_join(territorios_circ,
      by = join_by(codigo_circunscripcion, codigo_municipio, codigo_distrito, codigo_seccion)
    ) %>%
    select(eleccion_id, territorio_id, nrepresentantes)

  nrep_prov <- nrepresentantes_raw %>%
    filter(nchar(codigo_circunscripcion) < 3) %>%
    mutate(codigo_ccaa = ifelse(tipo_eleccion %in% c("G", "L"), "99", codigo_ccaa)) %>%
    left_join(elecciones_id, by = join_by(year, mes, codigo_ccaa, tipo_eleccion)) %>%
    left_join(territorios_prov,
      by = join_by(codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion)
    ) %>%
    select(eleccion_id, territorio_id, nrepresentantes)

  nrep_muni <- read_xlsx("data-raw/representantes/nrepresentantes_muni.xlsx") %>%
    mutate(codigo_distrito = "99", codigo_seccion = "9999") %>%
    mutate(codigo_ccaa = ifelse(tipo_eleccion %in% c("G", "L"), "99", codigo_ccaa)) %>%
    left_join(elecciones_id, by = join_by(year, mes, codigo_ccaa, tipo_eleccion)) %>%
    left_join(territorios_muni,
      by = join_by(codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion)
    ) %>%
    select(eleccion_id, territorio_id, nrepresentantes)

  nrepresentantes <- bind_rows(nrep_circ, nrep_prov, nrep_muni) %>%
    filter(!is.na(eleccion_id), !is.na(territorio_id))

  return(nrepresentantes)
}



get_representantes <- function() {
  elecciones_id <-
    read_csv("tablas-finales/dimensiones/elecciones", show_col_types = F) %>%
    select(year, mes, codigo_ccaa, tipo_eleccion, eleccion_id = id) %>%
    mutate(year = as.character(year))

  territorios_id <-
    read_csv("tablas-finales/dimensiones/territorios", show_col_types = F) %>%
    filter(
      tipo %in% c("provincia", "municipio")
    ) %>%
    select(codigo_provincia, codigo_municipio, codigo_distrito,
      codigo_seccion,
      territorio_id = id
    )

  partidos_id <-
    read_csv("tablas-finales/dimensiones/partidos", show_col_types = F) %>%
    mutate(across(c(siglas, denominacion), normalize_lower)) %>%
    select(-partido_recode_id) %>%
    rename(partido_id = id)

  representantes_prov <- read_xlsx("data-raw/representantes/representantes_prov.xlsx") %>%
    mutate(
      codigo_municipio = "999", codigo_distrito = "99", codigo_seccion = "9999",
      across(c(siglas, denominacion), tolower)
    ) %>%
    rename(codigo_provincia = codigo_circunscripcion) %>%
    filter(tipo_eleccion != "E") %>%
    filter(!is.na(representantes))

  representantes_muni <- read_xlsx("data-raw/representantes/representantes_muni.xlsx") %>%
    mutate(codigo_distrito = "99", codigo_seccion = "9999") %>%
    filter(!is.na(representantes))

  representantes <-
    bind_rows(representantes_prov, representantes_muni) %>%
    mutate(
      codigo_ccaa = ifelse(tipo_eleccion %in% c("G", "L"), "99", codigo_ccaa),
      across(c(siglas, denominacion), tolower)
    ) %>%
    left_join(elecciones_id,
      by = join_by(year, mes, codigo_ccaa, tipo_eleccion)
    ) %>%
    left_join(territorios_id,
      by = join_by(
        codigo_provincia, codigo_municipio,
        codigo_distrito, codigo_seccion
      )
    ) %>%
    left_join(partidos_id,
      by = join_by(
        siglas,
        denominacion
      )
    ) %>%
    select(eleccion_id, territorio_id, partido_id, representantes) %>%
    filter(!is.na(eleccion_id), !is.na(territorio_id), !is.na(partido_id))

  return(representantes)
}
