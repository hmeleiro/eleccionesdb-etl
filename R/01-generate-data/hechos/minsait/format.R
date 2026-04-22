library(readr)
library(dplyr)
library(purrr)
library(stringr)

source("R/tests/validate_data_processed.R")

INPUT_DIR  <- "data-raw/hechos/minsait/"
OUTPUT_DIR <- "data-processed/hechos/minsait/"

# Para cada CCAA se coge la elección autonómica más reciente del dimensión.
# Los ficheros minsait contienen exactamente una elección por CCAA (la última
# convocatoria provisional), por lo que slice_max(id) es una correspondencia 1:1.
fechas <- read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE) %>%
  filter(tipo_eleccion == "A") %>%
  group_by(codigo_ccaa) %>%
  slice_max(order_by = id, n = 1) %>%
  ungroup() %>%
  transmute(eleccion_id = id, codigo_ccaa)

territorios <- read_csv("tablas-finales/dimensiones/territorios", show_col_types = FALSE) %>%
  select(territorio_id = id, codigo_ccaa, codigo_provincia, codigo_municipio,
         codigo_distrito, codigo_seccion)

# Tabla de correspondencia entre códigos de CCAA del Ministerio del Interior (MIR)
# y los códigos INE. Los CSV de Minsait usan la codificación MIR.
codigos_ccaa <- readRDS("data-raw/hechos/00-congreso/codigos_ccaa.rds") %>%
  select(-ccaa)

remap_ccaa <- function(df) {
  df %>%
    left_join(codigos_ccaa, by = "codigo_ccaa") %>%
    select(-codigo_ccaa) %>%
    rename(codigo_ccaa = codigo_ccaa_ine)
}

# =============================================================================
# LECTURA
# =============================================================================

read_minsait_file <- function(file) {
  read_csv(file, show_col_types = FALSE) %>%
    filter(!is.na(recode)) %>%
    transmute(
      codigo_ccaa      = str_pad(as.character(codigo_ccaa), 2, "left", "0"),
      codigo_provincia = str_pad(as.character(codigo_provincia), 2, "left", "0"),
      codigo_municipio = str_pad(as.character(codigo_municipio), 3, "left", "0"),
      codigo_distrito  = str_pad(as.character(codigo_distrito),  2, "left", "0"),
      codigo_seccion   = str_pad(as.character(codigo_seccion),   4, "left", "0"),
      codigo_mesa      = as.character(codigo_mesa),
      censo_ine        = as.integer(censo),
      # votos_validos = votantes totales - votos nulos (España: válidos = candidaturas + blancos)
      votos_validos    = as.integer(total_votantes - votos_nulos),
      abstenciones     = as.integer(abstencion),
      votos_blancos    = as.integer(votos_blanco),
      votos_nulos      = as.integer(votos_nulos),
      siglas           = siglas,
      denominacion     = denominacion,
      votos            = as.integer(votos)
    )
}

files    <- sort(list.files(INPUT_DIR, pattern = "\\.csv$", full.names = TRUE))
data_raw <- map_df(files, read_minsait_file) %>%
  remap_ccaa()

# =============================================================================
# INFO (participación)
# =============================================================================

INFO_COLS <- c(
  "codigo_ccaa", "codigo_provincia", "codigo_municipio",
  "codigo_distrito", "codigo_seccion", "codigo_mesa",
  "censo_ine", "votos_validos", "abstenciones", "votos_blancos", "votos_nulos"
)

# Un registro por mesa (los datos de participación se repiten para cada partido)
info_mesas <- data_raw %>%
  select(all_of(INFO_COLS)) %>%
  distinct() %>%
  arrange(codigo_ccaa, codigo_provincia, codigo_municipio,
          codigo_distrito, codigo_seccion, codigo_mesa)

info_seccion <- info_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(codigo_ccaa, codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion)

info_muni <- info_seccion %>%
  select(-c(codigo_seccion, codigo_distrito)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(codigo_ccaa, codigo_provincia, codigo_municipio)

info_prov <- info_muni %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(codigo_ccaa, codigo_provincia)

info_ccaa <- info_prov %>%
  select(-codigo_provincia) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(codigo_ccaa)

info <- bind_rows(info_ccaa, info_prov, info_muni, info_seccion) %>%
  mutate(
    across(c(codigo_provincia), ~ ifelse(is.na(.), "99",   .)),
    codigo_municipio = ifelse(is.na(codigo_municipio), "999",  codigo_municipio),
    codigo_distrito  = ifelse(is.na(codigo_distrito),  "99",   codigo_distrito),
    codigo_seccion   = ifelse(is.na(codigo_seccion),   "9999", codigo_seccion)
  ) %>%
  left_join(fechas,      by = "codigo_ccaa") %>%
  left_join(territorios, by = c("codigo_ccaa", "codigo_provincia", "codigo_municipio",
                                "codigo_distrito", "codigo_seccion")) %>%
  arrange(eleccion_id, territorio_id) %>%
  select(-starts_with("codigo_")) %>%
  relocate(eleccion_id, territorio_id)

# =============================================================================
# VOTOS (por partido)
# =============================================================================

VOTOS_COLS <- c(
  "codigo_ccaa", "codigo_provincia", "codigo_municipio",
  "codigo_distrito", "codigo_seccion", "codigo_mesa",
  "siglas", "denominacion", "votos"
)


data_raw %>%
  count(siglas, denominacion)

votos_mesas <- data_raw %>%
  select(all_of(VOTOS_COLS)) %>%
  filter(!is.na(votos)) %>%
  arrange(codigo_ccaa, codigo_provincia, codigo_municipio,
          codigo_distrito, codigo_seccion, codigo_mesa, -votos)

votos_seccion <- votos_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = TRUE)), .groups = "drop")

votos_muni <- votos_seccion %>%
  select(-c(codigo_seccion, codigo_distrito)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = TRUE)), .groups = "drop")

votos_prov <- votos_muni %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = TRUE)), .groups = "drop")

votos_ccaa <- votos_prov %>%
  select(-codigo_provincia) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~sum(.x, na.rm = TRUE)), .groups = "drop")

votos <- bind_rows(votos_ccaa, votos_prov, votos_muni, votos_seccion) %>%
  mutate(
    across(c(codigo_provincia), ~ ifelse(is.na(.), "99",   .)),
    codigo_municipio = ifelse(is.na(codigo_municipio), "999",  codigo_municipio),
    codigo_distrito  = ifelse(is.na(codigo_distrito),  "99",   codigo_distrito),
    codigo_seccion   = ifelse(is.na(codigo_seccion),   "9999", codigo_seccion)
  ) %>%
  left_join(fechas,      by = "codigo_ccaa") %>%
  left_join(territorios, by = c("codigo_ccaa", "codigo_provincia", "codigo_municipio",
                                "codigo_distrito", "codigo_seccion")) %>%
  arrange(eleccion_id, territorio_id, -votos) %>%
  select(-starts_with("codigo_")) %>%
  relocate(eleccion_id, territorio_id)

# =============================================================================
# CHECKS
# =============================================================================

validate_info(info,   label = "minsait/info")
validate_votos(votos, label = "minsait/votos")
validate_info_votos_consistency(info, votos, label = "minsait")
validate_votos_partido_match(votos, label = "minsait/votos")

# =============================================================================
# WRITE
# =============================================================================

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

saveRDS(info,  paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos, paste0(OUTPUT_DIR, "votos.rds"))
