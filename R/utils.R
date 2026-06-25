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

  territorios <- read_csv("tablas-finales/dimensiones/territorios", show_col_types = F)

  territorios_prov <- territorios %>%
    filter(tipo == "provincia") %>%
    select(codigo_provincia, codigo_municipio, codigo_distrito,
           codigo_seccion,
           territorio_id = id
    )

  territorios_circ <- territorios %>%
    filter(tipo == "circunscripcion") %>%
    select(codigo_ccaa, codigo_circunscripcion, codigo_municipio,
           codigo_distrito, codigo_seccion,
           territorio_id = id
    )

  territorios_muni <- territorios %>%
    filter(tipo == "municipio") %>%
    select(codigo_provincia, codigo_municipio, codigo_distrito,
           codigo_seccion,
           territorio_id = id
    )

  representantes_prov_raw <- read_xlsx("data-raw/representantes/representantes_prov.xlsx") %>%
    mutate(
      codigo_municipio = "999", codigo_distrito = "99", codigo_seccion = "9999",
      across(c(siglas, denominacion), normalize_lower)
    ) %>%
    filter(tipo_eleccion != "E") %>%
    filter(!is.na(representantes))

  representantes_circ <- representantes_prov_raw %>%
    filter(nchar(codigo_circunscripcion) >= 3) %>%
    mutate(codigo_ccaa = ifelse(tipo_eleccion %in% c("G", "L"), "99", codigo_ccaa)) %>%
    left_join(elecciones_id,
              by = join_by(year, mes, codigo_ccaa, tipo_eleccion)
    ) %>%
    left_join(territorios_circ,
              by = join_by(
                codigo_ccaa, codigo_circunscripcion, codigo_municipio,
                codigo_distrito, codigo_seccion
              )
    )

  representantes_prov <- representantes_prov_raw %>%
    filter(nchar(codigo_circunscripcion) < 3) %>%
    rename(codigo_provincia = codigo_circunscripcion) %>%
    mutate(codigo_ccaa = ifelse(tipo_eleccion %in% c("G", "L"), "99", codigo_ccaa)) %>%
    left_join(elecciones_id,
              by = join_by(year, mes, codigo_ccaa, tipo_eleccion)
    ) %>%
    left_join(territorios_prov,
              by = join_by(
                codigo_provincia, codigo_municipio,
                codigo_distrito, codigo_seccion
              )
    )

  representantes_muni <- read_xlsx("data-raw/representantes/representantes_muni.xlsx") %>%
    mutate(
      codigo_distrito = "99", codigo_seccion = "9999",
      codigo_ccaa = ifelse(tipo_eleccion %in% c("G", "L"), "99", codigo_ccaa),
      across(c(siglas, denominacion), normalize_lower)
    ) %>%
    filter(!is.na(representantes)) %>%
    left_join(elecciones_id,
              by = join_by(year, mes, codigo_ccaa, tipo_eleccion)
    ) %>%
    left_join(territorios_muni,
              by = join_by(
                codigo_provincia, codigo_municipio,
                codigo_distrito, codigo_seccion
              )
    )

  representantes <-
    bind_rows(representantes_prov, representantes_circ, representantes_muni) %>%
    transmute(
      eleccion_id,
      territorio_id,
      denominacion_lower = denominacion,
      siglas_lower = siglas,
      representantes
    ) %>%
    filter(!is.na(eleccion_id), !is.na(territorio_id))

  return(representantes)
}


match_representantes_to_votos <- function(representantes, votos) {
  representantes_dt <- data.table::as.data.table(data.table::copy(representantes))
  votos_dt <- data.table::as.data.table(votos)
  representantes_dt[, representante_row_id := .I]

  context_keys <- unique(representantes_dt[, .(eleccion_id, territorio_id)])
  candidates <- unique(votos_dt[
    context_keys,
    on = .(eleccion_id, territorio_id),
    nomatch = 0L,
    .(
      eleccion_id,
      territorio_id,
      partido_id,
      denominacion_lower,
      siglas_lower
    )
  ])

  exact <- merge(
    representantes_dt,
    candidates,
    by = c(
      "eleccion_id", "territorio_id", "denominacion_lower", "siglas_lower"
    ),
    all = FALSE,
    allow.cartesian = TRUE
  )[
    , if (data.table::uniqueN(partido_id) == 1L) {
      .(partido_id = partido_id[[1L]])
    },
    by = representante_row_id
  ]

  unmatched <- representantes_dt[!exact, on = "representante_row_id"]
  denomination_lookup <- candidates[
    , if (data.table::uniqueN(partido_id) == 1L) {
      .(partido_id = partido_id[[1L]])
    },
    by = .(eleccion_id, territorio_id, denominacion_lower)
  ]
  by_denomination <- merge(
    unmatched,
    denomination_lookup,
    by = c("eleccion_id", "territorio_id", "denominacion_lower"),
    all = FALSE
  )[, .(representante_row_id, partido_id)]

  unmatched <- unmatched[!by_denomination, on = "representante_row_id"]
  acronym_lookup <- candidates[
    , if (data.table::uniqueN(partido_id) == 1L) {
      .(partido_id = partido_id[[1L]])
    },
    by = .(eleccion_id, territorio_id, siglas_lower)
  ]
  by_acronym <- merge(
    unmatched,
    acronym_lookup,
    by = c("eleccion_id", "territorio_id", "siglas_lower"),
    all = FALSE
  )[, .(representante_row_id, partido_id)]

  mapping <- data.table::rbindlist(
    list(exact, by_denomination, by_acronym),
    use.names = TRUE,
    fill = TRUE
  )
  mapped <- representantes_dt[mapping, on = "representante_row_id", nomatch = 0L]

  unmatched_positive <- representantes_dt[
    !mapping,
    on = "representante_row_id"
  ][representantes > 0]
  if (nrow(unmatched_positive) > 0L) {
    warning(sprintf(
      "[representantes] %d filas con representantes positivos no pudieron enlazarse con votos",
      nrow(unmatched_positive)
    ), call. = FALSE)
  }

  mapped[
    , .(representantes = sum(representantes, na.rm = TRUE)),
    by = .(eleccion_id, territorio_id, partido_id)
  ]
}
