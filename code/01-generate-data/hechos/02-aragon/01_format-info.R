library(dplyr)
library(purrr)
library(readr)
library(stringr)

source("code/tests/validate_data_processed.R")

INPUT_DIR <- "data-raw/hechos/02-aragon/"
OUTPUT_DIR <- "data-processed/hechos/02-aragon/"

fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = F) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == "02") %>%
  transmute(eleccion_id = id, year = as.character(year))

territorios <- read_csv("tablas-finales/dimensiones/territorios", show_col_types = F) %>%
  select(territorio_id = id, codigo_ccaa, codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion)

read_file <- function(file) {
  rename_cols <- c(
    "nombre_m" = "municipio",
    "codigo_provincia" = "cod_provincia",
    "codigo_municipio" = "cod_municipio",
    "codigo_distrito" = "cod_distrito",
    "codigo_mesa" = "cod_mesa",
    "votos_blancos" = "blancos_e",
    "votos_blancos" = "blancos_m",
    "votos_nulos" = "nulos_e",
    "votos_nulos" = "nulos_m",
    "votos_validos" = "validos_e",
    "votos_validos" = "validos_m",
    "abstenciones" = "abstencion_e",
    "abstenciones" = "abstencion_m",
    "censo_ine" = "electores_e",
    "censo_ine" = "electores_m"
  )

  tmp <-
    read_csv(file, show_col_types = F) %>%
    rename(any_of(rename_cols)) %>%
    mutate(across(any_of(c("codigo_municipio")), as.character))

  if(str_detect(file, "escala municipal")) {
    tmp <-
      tmp %>%
      filter(cod_elec < "CA2011")
  }

  return(tmp)

}

files <- list.files(INPUT_DIR, pattern = "partici|escala municipal", full.names = T, recursive = T)

data <- map_df(files, read_file)%>%
  mutate(codigo_mesa = gsub(" ", "", codigo_mesa))

# Separo CERA (codigo_municipio == 000) de CER
info_cera <- data %>%
  filter(substr(codigo_municipio, 3, 5) == "000")

info_cer <- data %>%
  filter(substr(codigo_municipio, 3, 5) != "000")

# MESAS
info_mesas <-
  info_cer %>%
  transmute(
    year = substr(cod_elec, 3, 6),
    codigo_ccaa = "02",
    codigo_provincia = ifelse(is.na(codigo_provincia), substr(codigo_municipio, 1, 2), as.character(codigo_provincia)),
    codigo_circunscripcion = codigo_provincia,
    codigo_municipio = ifelse(is.na(codigo_mesa), substr(codigo_municipio, 3, 5), substr(codigo_mesa, 3, 5)),
    codigo_distrito = substr(codigo_mesa, 6, 7),
    codigo_distrito = ifelse(is.na(codigo_distrito), "99", codigo_distrito),
    codigo_seccion = substr(codigo_mesa, 8, 10),
    codigo_seccion = ifelse(is.na(codigo_seccion), "9999", str_pad(codigo_seccion, 4, "left", "0")),
    codigo_mesa = substr(codigo_mesa, 11, 11),
    censo_ine,
    abstenciones,
    votos_validos,
    votos_blancos,
    votos_nulos) %>%
  arrange(year, codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion, codigo_mesa)

# SECCIONES
info_seccion <-
  info_mesas %>%
  select(-codigo_mesa) %>%
  filter(!is.na(codigo_seccion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = T))) %>%
  ungroup() %>%
  arrange(year, codigo_municipio, codigo_distrito, codigo_seccion) %>%
  filter(codigo_seccion != "9999")

# MUNICIPIOS
info_muni <-
  info_mesas %>%
  select(-c(codigo_seccion, codigo_distrito, codigo_mesa)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = T))) %>%
  ungroup() %>%
  arrange(year, codigo_municipio)

# PROVINCIA
info_prov <-
  info_muni %>%
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
  relocate(eleccion_id, territorio_id) %>%
  mutate(
    abstenciones = ifelse(abstenciones < 0, NA_integer_, abstenciones),
    abstenciones = ifelse(abstenciones + votos_validos > censo_ine, NA_integer_, abstenciones)
  )

# CHECKS
validate_info(info_cer, label = "02-aragon/info_cer")

# WRITE DATA
dir.create(OUTPUT_DIR, showWarnings = F)

saveRDS(info_cer, paste0(OUTPUT_DIR, "info.rds"))
