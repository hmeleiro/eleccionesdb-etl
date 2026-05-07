library(readr)
library(dplyr)
library(purrr)
library(stringr)
library(tibble)

source("R/tests/validate_data_processed.R")

INPUT_DIR <- "data-raw/hechos/01-andalucia/"
OUTPUT_DIR <- "data-processed/hechos/01-andalucia/"

fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = F) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == "01") %>%
  transmute(eleccion_id = id, year = as.character(year))

territorios <- read_csv("tablas-finales/dimensiones/territorios", show_col_types = F) %>%
  select(territorio_id = id, codigo_ccaa, codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion)

files_info <- list.files(INPUT_DIR, pattern = "^resumen_(provincias|municipios|secciones)\\.csv$", full.names = T)
files_info_cera <- list.files(INPUT_DIR, pattern = "^resumen_provincias_cera\\.csv$", full.names = T)
files_votos <- list.files(INPUT_DIR, pattern = "^escrutinio_(provincias|municipios|secciones)\\.csv$", full.names = T)
files_votos_cera <- list.files(INPUT_DIR, pattern = "^escrutinio_provincias_cera\\.csv$", full.names = T)

ensure_territory_columns <- function(data) {
  territory_cols <- c("codigo_municipio", "codigo_distrito", "codigo_seccion")
  missing_cols <- setdiff(territory_cols, colnames(data))

  data[missing_cols] <- NA_character_
  data
}

format_info_files <- function(files) {
  if (length(files) == 0) {
    return(tibble())
  }

  map_df(files, function(file) {
    rename_cols <- c(
      "year" = "fconvocatoria_valor",
      "codigo_provincia" = "provincia_clave",
      "codigo_municipio" = "municipio_clave",
      "codigo_distrito" = "distrito_clave",
      "codigo_seccion" = "seccion_clave",
      "votos_totales" = "votostotales",
      "votos_blancos" = "votosblanco",
      "votos_nulos" = "votosnulos",
      "votos_validos" = "votosvalidos",
      "abstenciones" = "abstenciones"
    )

    tmp <-
      read_csv(file, show_col_types = F) %>%
      janitor::clean_names() %>%
      rename(any_of(rename_cols)) %>%
      mutate(
        codigo_ccaa = "01",
        across(
          any_of(c("codigo_distrito", "codigo_provincia")),
          ~ str_pad(.x, 2, "left", "0")
        ),
        across(any_of("codigo_municipio"), ~ str_pad(.x, 3, "left", "0")),
        across(any_of("codigo_seccion"), ~ str_pad(.x, 4, "left", "0")),
        year = substr(year, 1, 4), .before = 1
      )

    if (!"censo_ine" %in% colnames(tmp) && all(c("votos_totales", "abstenciones") %in% colnames(tmp))) {
      tmp <- tmp %>% mutate(censo_ine = votos_totales + abstenciones)
    }

    tmp
  }) %>%
    select(any_of(
      c(
        "year", "codigo_ccaa", "codigo_provincia", "codigo_municipio",
        "codigo_distrito", "codigo_seccion",
        "censo_ine", "votos_validos", "votos_blancos", "votos_nulos", "abstenciones"
      )
    )) %>%
    ensure_territory_columns() %>%
    mutate(
      codigo_circunscripcion = codigo_provincia,
      codigo_municipio = ifelse(is.na(codigo_municipio), "999", codigo_municipio),
      codigo_distrito = ifelse(is.na(codigo_distrito), "99", codigo_distrito),
      codigo_seccion = ifelse(is.na(codigo_seccion), "9999", codigo_seccion)
    )
}

format_votos_files <- function(files) {
  if (length(files) == 0) {
    return(tibble())
  }

  map_df(files, function(file) {
    rename_cols <- c(
      "year" = "fconvocatoria_valor",
      "codigo_provincia" = "provincia_clave",
      "codigo_municipio" = "municipio_clave",
      "codigo_distrito" = "distrito_clave",
      "codigo_seccion" = "seccion_clave",
      "votos" = "numvotos"
    )

    read_delim(file, show_col_types = F) %>%
      janitor::clean_names() %>%
      rename(any_of(rename_cols)) %>%
      mutate(
        siglas = str_split(conjunto, " - ", n = 2, simplify = T)[, 1],
        denominacion = str_split(conjunto, " - ", n = 2, simplify = T)[, 2],
        across(c(siglas, denominacion), str_squish),
        codigo_ccaa = "01",
        across(
          any_of(c("codigo_distrito", "codigo_provincia")),
          ~ str_pad(.x, 2, "left", "0")
        ),
        across(any_of("codigo_municipio"), ~ str_pad(.x, 3, "left", "0")),
        across(any_of("codigo_seccion"), ~ str_pad(.x, 4, "left", "0")),
        year = substr(year, 1, 4), .before = 1
      )
  }) %>%
    select(any_of(
      c(
        "year", "codigo_ccaa", "codigo_provincia",
        "codigo_municipio", "codigo_distrito", "codigo_seccion",
        "censo_ine", "votos_blancos", "votos_nulos",
        "siglas", "denominacion", "votos"
      )
    )) %>%
    ensure_territory_columns() %>%
    mutate(
      codigo_circunscripcion = codigo_provincia,
      codigo_municipio = ifelse(is.na(codigo_municipio), "999", codigo_municipio),
      codigo_distrito = ifelse(is.na(codigo_distrito), "99", codigo_distrito),
      codigo_seccion = ifelse(is.na(codigo_seccion), "9999", codigo_seccion)
    )
}

subtract_cera_from_info <- function(info_total, info_cera) {
  if (nrow(info_cera) == 0) {
    return(info_total)
  }

  keys <- c(
    "year", "codigo_ccaa", "codigo_provincia", "codigo_municipio",
    "codigo_distrito", "codigo_seccion", "codigo_circunscripcion"
  )
  value_cols <- intersect(
    c("censo_ine", "votos_validos", "votos_blancos", "votos_nulos", "abstenciones"),
    colnames(info_total)
  )

  cera <- info_cera %>%
    filter(
      codigo_municipio == "999",
      codigo_distrito == "99",
      codigo_seccion == "9999"
    ) %>%
    group_by(across(all_of(keys))) %>%
    summarise(across(all_of(value_cols), ~ sum(.x, na.rm = T)), .groups = "drop") %>%
    rename_with(~ paste0(.x, "_cera"), all_of(value_cols))

  out <- info_total %>%
    left_join(cera, by = keys)

  for (col in value_cols) {
    cera_col <- paste0(col, "_cera")
    out[[col]] <- out[[col]] - coalesce(out[[cera_col]], 0)
  }

  out %>%
    select(-all_of(paste0(value_cols, "_cera")))
}

subtract_cera_from_votos <- function(votos_total, votos_cera) {
  if (nrow(votos_cera) == 0) {
    return(votos_total)
  }

  keys <- c(
    "year", "codigo_ccaa", "codigo_provincia", "codigo_municipio",
    "codigo_distrito", "codigo_seccion", "codigo_circunscripcion", "siglas"
  )

  cera <- votos_cera %>%
    filter(
      codigo_municipio == "999",
      codigo_distrito == "99",
      codigo_seccion == "9999"
    ) %>%
    group_by(across(all_of(keys))) %>%
    summarise(votos_cera = sum(votos, na.rm = T), .groups = "drop")

  votos_total %>%
    left_join(cera, by = keys) %>%
    mutate(votos = votos - coalesce(votos_cera, 0)) %>%
    select(-votos_cera)
}

info <- format_info_files(files_info)
info_cera <- format_info_files(files_info_cera)
votos <- format_votos_files(files_votos)
votos_cera <- format_votos_files(files_votos_cera)

info <- subtract_cera_from_info(info, info_cera)
votos <- subtract_cera_from_votos(votos, votos_cera)


info_cer <- info %>% filter(codigo_municipio != "901")
votos_cer <- votos %>% filter(codigo_municipio != "901")

# INFO
# CCAA
info_ccaa <-
  info_cer %>%
  filter(codigo_municipio == "999") %>%
  select(-c(codigo_provincia, codigo_circunscripcion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = T)), .groups = "drop") %>%
  ungroup() %>%
  arrange(year) %>%
  mutate(
    codigo_provincia = "99",
    codigo_circunscripcion = "99",
    .after = codigo_ccaa
  )


info_cer <-
  bind_rows(info_ccaa, info_cer) %>%
  left_join(fechas, by = "year") %>%
  left_join(
    territorios,
    by = c(
      "codigo_ccaa", "codigo_provincia", "codigo_municipio",
      "codigo_distrito", "codigo_seccion"
    )
  ) %>%
  arrange(eleccion_id, territorio_id) %>%
  select(-c(year, starts_with("codigo_"))) %>%
  relocate(eleccion_id, territorio_id) %>%
  mutate(
    abstenciones = ifelse(abstenciones < 0, NA_integer_, abstenciones)
  )

# VOTOS
# CCAA
votos_ccaa <-
  votos_cer %>%
  filter(codigo_municipio == "999") %>%
  select(-c(codigo_provincia, codigo_circunscripcion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = T)), .groups = "drop") %>%
  ungroup() %>%
  arrange(year, -votos)

votos_cer <-
  bind_rows(votos_ccaa, votos_cer) %>%
  mutate(
    across(c(codigo_provincia, codigo_circunscripcion, codigo_distrito), ~ ifelse(is.na(.), "99", .)),
    codigo_municipio = ifelse(is.na(codigo_municipio), "999", codigo_municipio),
    codigo_seccion = ifelse(is.na(codigo_seccion), "9999", codigo_seccion)
  ) %>%
  left_join(fechas, by = "year") %>%
  left_join(
    territorios,
    by = c(
      "codigo_ccaa", "codigo_provincia", "codigo_municipio",
      "codigo_distrito", "codigo_seccion"
    )
  ) %>%
  arrange(eleccion_id, territorio_id) %>%
  select(-c(year, starts_with("codigo_"))) %>%
  relocate(eleccion_id, territorio_id)

# Descartar filas de info sin votos asociados (secciones recodificadas/sin escrutinio)
info_cer <- info_cer %>%
  semi_join(votos_cer, by = c("eleccion_id", "territorio_id"))

# CHECKS
validate_info(info_cer, label = "01-andalucia/info_cer")
validate_votos(votos_cer, label = "01-andalucia/votos_cer")
validate_info_votos_consistency(info_cer, votos_cer, label = "01-andalucia/cer")
validate_votos_partido_match(votos_cer, label = "01-andalucia/votos_cer")

# WRITE DATA
dir.create(OUTPUT_DIR, showWarnings = FALSE)

saveRDS(info_cer, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos_cer, paste0(OUTPUT_DIR, "votos.rds"))
