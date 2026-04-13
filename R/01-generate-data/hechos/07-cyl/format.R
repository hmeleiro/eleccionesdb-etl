library(dplyr)
library(readxl)
library(stringr)
library(purrr)
library(tidyr)
library(readr)

source("R/tests/validate_data_processed.R")

INPUT_DIR <- "data-raw/hechos/07-cyl/"
OUTPUT_DIR <- "data-processed/hechos/07-cyl/"

fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = F) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == "07") %>%
  transmute(eleccion_id = id, year = as.character(year))

territorios <- read_csv("tablas-finales/dimensiones/territorios", show_col_types = F) %>%
  select(territorio_id = id, codigo_ccaa, codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion)

files <- list.files(INPUT_DIR, full.names = T)
data <-
  map_df(files, function(file) {
    rename_cols <- c(
      "codigo_mesa_completo" = "codigo_de_mesa",
      # "codigo_mesa_completo" = "mesa",
      "codigo_seccion" = "seccion",
      "codigo_distrito" = "distrito",
      "codigo_seccion" = "seccion",
      "participacion_1" = "primer_avance",
      "participacion_2" = "segundo_avance",
      "votos_blancos" = "blanco",
      "votos_blancos" = "votos_blanco",
      "votos_nulos" = "nulos",
      "censo_ine" = "censo",
      "votos" = "no_votos",
      "total_votantes" = "votantes"
    )

    year <- str_remove(file, INPUT_DIR)
    year <- str_remove(year, ".csv")

    tmp <-
      read_delim(file, delim = ";", locale = locale(encoding = "latin1"), show_col_types = F) %>%
      janitor::clean_names() %>%
      rename(any_of(rename_cols)) %>%
      mutate(
        year,
        abstenciones = censo_ine - total_votantes,
        votos_validos = total_votantes - votos_nulos,
        codigo_provincia = substr(codigo_mesa_completo, 1, 2),
        codigo_distrito = str_pad(codigo_distrito, width = 2, pad = "0"),
        codigo_municipio = str_pad(codigo_municipio, width = 3, pad = "0"),
        codigo_seccion = str_pad(codigo_seccion, width = 4, pad = "0"),
        codigo_mesa = substr(codigo_mesa_completo, nchar(codigo_mesa_completo), nchar(codigo_mesa_completo))
      ) %>%
      select(-any_of("codigo_mesa_completo"))


    return(tmp)
  }) %>%
  left_join(fechas, by = "year") %>%
  ungroup() %>%
  transmute(
    year,
    codigo_ccaa = "07",
    across(starts_with("codigo_")),
    censo_ine, participacion_1, participacion_2, abstenciones, votos_validos, votos_blancos, votos_nulos,
    siglas = partido, denominacion = partido, votos
  ) %>%
  filter(!is.na(votos))

data_cer <- data %>% filter(codigo_municipio != "999")

# INFO
# MESAS
info_mesas <-
  data_cer %>%
  select(-c(siglas, denominacion, votos)) %>%
  distinct()

# SECCIONES
info_seccion <-
  info_mesas %>%
  select(-c(codigo_mesa)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# MUNICIPIOS
info_muni <-
  info_seccion %>%
  select(-c(codigo_distrito, codigo_seccion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# PROVINCIA
info_prov <-
  info_muni %>%
  select(-c(codigo_municipio)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# CCAA
info_ccaa <-
  info_prov %>%
  select(-c(codigo_provincia)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = T))) %>%
  ungroup() %>%
  arrange(year)

info_cer <-
  bind_rows(info_ccaa, info_prov, info_muni, info_seccion) %>%
  mutate(
    across(c(codigo_provincia, codigo_distrito), ~ ifelse(is.na(.), "99", .)),
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
  mutate(
    abstenciones = ifelse(abstenciones < 0, NA_integer_, abstenciones)
  )

# VOTOS
# MESAS
votos_mesas <-
  data_cer %>%
  select(-c(starts_with("votos_"), votos_validos, censo_ine, abstenciones)) %>%
  arrange(year, codigo_provincia, codigo_municipio, codigo_seccion, codigo_mesa, -votos)

# SECCIONES
votos_seccion <-
  votos_mesas %>%
  select(-c(codigo_mesa, participacion_1, participacion_2)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup() %>%
  arrange(year, codigo_provincia, codigo_municipio, codigo_seccion, -votos)

# MUNICIPIOS
votos_muni <-
  votos_seccion %>%
  select(-c(codigo_distrito, codigo_seccion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup() %>%
  arrange(year, codigo_provincia, codigo_municipio, -votos)

# PROVINCIA
votos_prov <-
  votos_muni %>%
  select(-c(codigo_municipio)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup() %>%
  arrange(year, codigo_provincia, -votos)

# CCAA
votos_ccaa <-
  votos_prov %>%
  select(-c(codigo_provincia)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = T))) %>%
  ungroup() %>%
  arrange(year, -votos)

votos_cer <-
  bind_rows(votos_ccaa, votos_prov, votos_muni, votos_seccion) %>%
  mutate(
    across(c(codigo_provincia, codigo_distrito), ~ ifelse(is.na(.), "99", .)),
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

info_mesas <- info_mesas %>% filter(!is.na(codigo_mesa))
votos_mesas <- votos_mesas %>% filter(!is.na(codigo_mesa))
info_seccion <- info_seccion %>% filter(!is.na(codigo_seccion))
votos_seccion <- votos_seccion %>% filter(!is.na(codigo_seccion))


# CHECKS
validate_info(info_cer, label = "07-cyl/info_cer")
validate_votos(votos_cer, label = "07-cyl/votos_cer")

# optional: cross-check that both cover the same territory/election keys
validate_info_votos_consistency(info_cer, votos_cer, label = "07-cyl/cer")
validate_votos_partido_match(votos_cer, label = "07-cyl/votos_cer")

# WRITE DATA
dir.create(OUTPUT_DIR, showWarnings = F)

saveRDS(info_cer, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos_cer, paste0(OUTPUT_DIR, "votos.rds"))
