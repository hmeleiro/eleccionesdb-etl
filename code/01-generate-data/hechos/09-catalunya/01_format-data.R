library(readr)
library(dplyr)
library(purrr)
library(stringr)

source("code/tests/validate_data_processed.R")

INPUT_DIR <- "data-raw/hechos/09-catalunya/"
OUTPUT_DIR <- "data-processed/hechos/09-catalunya/"

fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = F) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == "09") %>%
  transmute(eleccion_id = id, year = as.character(year))

territorios <- read_csv("tablas-finales/dimensiones/territorios", show_col_types = F) %>%
  select(territorio_id = id, codigo_ccaa, codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion)

files_info <- list.files(INPUT_DIR, pattern = "Participació", full.names = T)
files_votos <- list.files(INPUT_DIR, pattern = "Vots", full.names = T)

info <-
  map_df(files_info, function(file) {
    rename_cols <- c(
      "codigo_provincia" = "codi_provincia",
      "codigo_municipio" = "codi_municipi",
      "codigo_distrito" = "districte",
      "codigo_seccion" = "seccio",
      "codigo_mesa" = "mesa",
      "censo_ine" = "cens",
      "votos_blancos" = "vots_en_blanc",
      "votos_nulos" = "vots_nuls",
      "votos_validos" = "vots_valids",
      "abstenciones" = "abstencio",
      "participacion_1" = "participacio_13_00",
      "participacion_2" = "participacio_18_00"
    )

    lcl <- locale(decimal_mark = ",", grouping_mark = ".")

    year <- str_extract(file, "[0-9]{4}")
    read_delim(file, show_col_types = F, locale = lcl) %>%
      janitor::clean_names() %>%
      rename(any_of(rename_cols)) %>%
      mutate(
        across(starts_with("participacio"), as.numeric),
        codigo_municipio = str_pad(codigo_municipio, 3, "left", "0"),
        codigo_seccion = str_pad(codigo_seccion, 4, "left", "0"),
        across(c(codigo_provincia, codigo_distrito), ~str_pad(., 2, "left", "0")),
        codigo_mesa = as.character(codigo_mesa),
        year, .before = 1
      )
  }) %>%
  transmute(
    year,
    codigo_ccaa = "09",
    codigo_provincia,
    codigo_circunscripcion = codigo_provincia,
    codigo_municipio = substr(codigo_municipio, 3, 5),
    codigo_distrito,
    codigo_seccion,
    codigo_mesa,
    censo_ine,
    votos_blancos,
    votos_nulos,
    abstenciones,
    participacion_1,
    participacion_2
  )

votos <-
  map_df(files_votos, function(file) {
    rename_cols <- c(
      "codigo_provincia" = "codi_provincia",
      "codigo_municipio" = "codi_municipi",
      "codigo_distrito" = "districte",
      "codigo_seccion" = "seccio",
      "codigo_mesa" = "mesa",
      "denominacion" = "nom_candidatura",
      "siglas" = "sigles_candidatura",
      "votos" = "vots"
    )

    lcl <- locale(decimal_mark = ",", grouping_mark = ".")

    year <- str_extract(file, "[0-9]{4}")

    read_delim(file, show_col_types = F, locale = lcl) %>%
      janitor::clean_names() %>%
      rename(any_of(rename_cols)) %>%
      mutate(
        across(c(siglas, denominacion), ~str_squish(.)),
        codigo_municipio = str_pad(codigo_municipio, 3, "left", "0"),
        codigo_seccion = str_pad(codigo_seccion, 4, "left", "0"),
        across(c(codigo_provincia, codigo_distrito), ~str_pad(., 2, "left", "0")),
        codigo_mesa = as.character(codigo_mesa),
        year, .before = 1
      )
  }) %>%
  transmute(
    year,
    codigo_ccaa = "09",
    codigo_provincia,
    codigo_circunscripcion = codigo_provincia,
    codigo_municipio = substr(codigo_municipio, 3, 5),
    codigo_distrito,
    codigo_seccion,
    codigo_mesa,
    siglas, denominacion, votos
  )


info_cer <- info %>% filter(codigo_municipio != "998")
info_cera <- info %>% filter(codigo_municipio == "998")
votos_cer <- votos %>% filter(codigo_municipio != "998")
votos_cera <- votos %>% filter(codigo_municipio == "998")


# INFO
# MESAS
info_mesas <-
  info_cer %>%
  arrange(year, codigo_circunscripcion, codigo_municipio,
          codigo_distrito, codigo_seccion, codigo_mesa)

# SECCIONES
info_seccion <-
  info_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = T))) %>%
  ungroup() %>%
  arrange(year, codigo_municipio)

# MUNICIPIOS
info_muni <-
  info_seccion %>%
  select(-c(codigo_seccion, codigo_distrito)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = T))) %>%
  ungroup() %>%
  arrange(year, codigo_municipio)


# PROVINCIA
info_prov <-
  info_muni %>%
  mutate(codigo_provincia = ifelse(codigo_circunscripcion == "99", "99", codigo_provincia)) %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = T))) %>%
  ungroup() %>%
  arrange(year)

info_ccaa <-
  info_prov %>%
  select(-c(codigo_provincia, codigo_circunscripcion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = T))) %>%
  ungroup() %>%
  arrange(year)

info_cer <-
  bind_rows(info_ccaa, info_prov, info_muni, info_seccion) %>%
  mutate(
    across(c(codigo_provincia, codigo_circunscripcion, codigo_distrito), ~ifelse(is.na(.), "99", .)),
    codigo_municipio = ifelse(is.na(codigo_municipio), "999", codigo_municipio),
    codigo_seccion = ifelse(is.na(codigo_seccion), "9999", codigo_seccion)
  ) %>%
  left_join(fechas, by = "year") %>%
  left_join(
    territorios,
    by = c("codigo_ccaa", "codigo_provincia", "codigo_municipio",
           "codigo_distrito", "codigo_seccion")) %>%
  arrange(eleccion_id, territorio_id) %>%
  select(-c(year, starts_with("codigo_"))) %>%
  relocate(eleccion_id, territorio_id)

# INFO CERA
# info_cera <-
#   info_cera %>%
#   arrange(year, codigo_circunscripcion)

# VOTOS
# MESAS
votos_mesa <-
  votos_cer %>%
  arrange(year, codigo_circunscripcion, codigo_municipio,
          codigo_distrito, codigo_seccion, -votos)

# SECCIONES
votos_seccion <-
  votos_mesa %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = T))) %>%
  ungroup() %>%
  arrange(year, codigo_municipio, -votos)


# MUNICIPIOS
votos_muni <-
  votos_seccion %>%
  select(-c(codigo_seccion, codigo_distrito)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = T))) %>%
  ungroup() %>%
  arrange(year, codigo_municipio, -votos)

# PROVINCIA
votos_prov <-
  votos_muni  %>%
  mutate(codigo_provincia = ifelse(codigo_circunscripcion == "99", "99", codigo_provincia)) %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = T))) %>%
  ungroup() %>%
  arrange(year, -votos)

# CCAA
votos_ccaa <-
  votos_prov  %>%
  select(-c(codigo_provincia, codigo_circunscripcion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = T))) %>%
  ungroup() %>%
  arrange(year, -votos)


votos_cer <-
  bind_rows(votos_ccaa, votos_prov, votos_muni, votos_seccion) %>%
  mutate(
    across(c(codigo_provincia, codigo_circunscripcion, codigo_distrito), ~ifelse(is.na(.), "99", .)),
    codigo_municipio = ifelse(is.na(codigo_municipio), "999", codigo_municipio),
    codigo_seccion = ifelse(is.na(codigo_seccion), "9999", codigo_seccion)
  ) %>%
  left_join(fechas, by = "year") %>%
  left_join(
    territorios,
    by = c("codigo_ccaa", "codigo_provincia", "codigo_municipio",
           "codigo_distrito", "codigo_seccion")) %>%
  arrange(eleccion_id, territorio_id) %>%
  select(-c(year, starts_with("codigo_"))) %>%
  relocate(eleccion_id, territorio_id)

# CERA
# votos_cera <-
#   votos_cera %>%
#   arrange(year, codigo_circunscripcion, -votos)


# CHECKS
validate_info(info_cer, label = "09-catalunya/info_cer")
validate_votos(votos_cer, label = "09-catalunya/votos_cer")

# optional: cross-check that both cover the same territory/election keys
validate_info_votos_consistency(info_cer, votos_cer, label = "01-andalucia/cer")

# WRITE DATA
dir.create(OUTPUT_DIR, showWarnings = F)

saveRDS(info_cer, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos_cer, paste0(OUTPUT_DIR, "votos.rds"))

