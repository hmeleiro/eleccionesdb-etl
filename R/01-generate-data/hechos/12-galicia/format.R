library(dplyr)
library(readxl)
library(purrr)
library(tidyr)
library(stringr)
library(readr)

source("R/tests/validate_data_processed.R")
source("R/01-generate-data/hechos/12-galicia/00_functions.R", encoding = "UTF-8")

INPUT_DIR <- "data-raw/hechos/12-galicia/"
OUTPUT_DIR <- "data-processed/hechos/12-galicia/"

fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == "12") %>%
  transmute(eleccion_id = id, year = as.character(year)) %>%
  group_by(year) %>%
  slice_max(eleccion_id, n = 1, with_ties = FALSE) %>%
  ungroup()

territorios <-
  read_csv("tablas-finales/dimensiones/territorios", show_col_types = FALSE) %>%
  select(
    territorio_id = id,
    codigo_ccaa, codigo_provincia, codigo_municipio,
    codigo_distrito, codigo_seccion
  )

# ===========================================================================
# POST-2001: mesas (2005+) y municipios (2001)
# ===========================================================================

# AVISO: 2001 solo tiene datos a nivel municipio (sin seccion/distrito/mesa)
files <- list.files(INPUT_DIR, pattern = "\\.xls", recursive = TRUE, full.names = TRUE)
files <- files[str_detect(tolower(basename(files)), "mesas|municipio")]

data_post01 <-
  map_df(files, read_file) %>%
  mutate(
    # 2001 has total_votos; 2005+ has votos_totais
    votos_total = coalesce(as.numeric(votos_totais), as.numeric(total_votos)),
    codigo_ccaa = "12",
    codigo_provincia = str_pad(as.character(codigo_provincia), 2, "left", "0"),
    codigo_municipio = str_pad(as.character(codigo_municipio), 3, "left", "0"),
    codigo_distrito = str_pad(as.character(codigo_distrito), 2, "left", "0"),
    codigo_seccion = str_pad(as.character(codigo_seccion), 4, "left", "0"),
    codigo_mesa = as.character(codigo_mesa),
    censo_ine = as.integer(censo),
    abstenciones = as.integer(coalesce(as.numeric(abstencion), censo_ine - votos_total)),
    votos_validos = as.integer(coalesce(as.numeric(validos), votos_total - as.numeric(votos_nulos))),
    votos_blancos = as.integer(votos_blancos),
    votos_nulos = as.integer(votos_nulos),
    siglas = as.character(partido),
    denominacion = siglas,
    votos = as.integer(votos)
  ) %>%
  select(
    year, codigo_ccaa, codigo_provincia,
    codigo_municipio, codigo_distrito, codigo_seccion, codigo_mesa,
    censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos,
    siglas, denominacion, votos
  )

# Excluir CERA y filas de totales (código municipio >= 900)
data_post01_cer <- data_post01 %>% filter(as.numeric(codigo_municipio) < 900)

# ===========================================================================
# PRE-2001: solo nivel provincia (1985-1997)
# ===========================================================================

files_prov <- list.files(INPUT_DIR, pattern = "provincias\\.xlsx$", full.names = TRUE)

data_pre01 <-
  map_df(files_prov, read_file_provincias) %>%
  mutate(codigo_provincia = as.character(codigo_provincia)) %>%
  group_by(year, codigo_provincia) %>%
  mutate(
    votos_candidaturas = sum(votos, na.rm = TRUE),
    votos_blancos      = as.integer(validos - votos_candidaturas),
    votos_nulos        = as.integer(total_votos - validos),
    abstenciones       = as.integer(censo - total_votos)
  ) %>%
  ungroup() %>%
  transmute(
    year,
    codigo_ccaa      = "12",
    codigo_provincia = str_pad(codigo_provincia, 2, "left", "0"),
    censo_ine        = as.integer(censo),
    abstenciones,
    votos_validos    = as.integer(validos),
    votos_blancos,
    votos_nulos,
    siglas           = as.character(partido),
    denominacion     = siglas,
    votos            = as.integer(votos)
  )

# ===========================================================================
# Separar info / votos
# ===========================================================================

info_post01_cer <- data_post01_cer %>%
  select(-c(siglas, denominacion, votos)) %>%
  distinct()

votos_post01_cer <- data_post01_cer %>%
  select(-c(censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos))

info_pre01 <- data_pre01 %>%
  select(-c(siglas, denominacion, votos)) %>%
  distinct()

votos_pre01 <- data_pre01 %>%
  select(-c(censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos))

# ===========================================================================
# INFO — Agregación jerárquica
# ===========================================================================

# Mesas con jerarquía completa (distrito + sección disponibles): 2005-2016
info_mesas <-
  info_post01_cer %>%
  filter(!is.na(codigo_mesa) & !is.na(codigo_seccion)) %>%
  arrange(
    year, codigo_provincia, codigo_municipio,
    codigo_distrito, codigo_seccion, codigo_mesa
  )

# Secciones (2005-2016): agregar mesas
info_seccion <-
  info_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year, codigo_provincia, codigo_municipio)

# Municipios: agregar secciones (2005-2016) + municipios directos (2001, 2020, 2024)
# Los datos sin sección (2001, 2020, 2024) van directo al nivel municipio
info_muni <-
  bind_rows(
    info_seccion %>% select(-c(codigo_seccion, codigo_distrito)),
    info_post01_cer %>%
      filter(is.na(codigo_seccion)) %>%
      select(-c(codigo_distrito, codigo_seccion, codigo_mesa))
  ) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year, codigo_provincia, codigo_municipio)

# Provincias: agregar municipios post-2001 + provincias pre-2001
info_prov <-
  bind_rows(
    info_muni %>% select(-codigo_municipio),
    info_pre01
  ) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year, codigo_provincia)

# CCAA
info_ccaa <-
  info_prov %>%
  select(-codigo_provincia) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year)

# ===========================================================================
# VOTOS — Agregación jerárquica
# ===========================================================================

votos_mesas <-
  votos_post01_cer %>%
  filter(!is.na(codigo_mesa) & !is.na(codigo_seccion)) %>%
  arrange(
    year, codigo_provincia, codigo_municipio,
    codigo_distrito, codigo_seccion, codigo_mesa, -votos
  )

votos_seccion <-
  votos_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year, codigo_municipio, -votos)

votos_muni <-
  bind_rows(
    votos_seccion %>% select(-c(codigo_seccion, codigo_distrito)),
    votos_post01_cer %>%
      filter(is.na(codigo_seccion)) %>%
      select(-c(codigo_distrito, codigo_seccion, codigo_mesa))
  ) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year, codigo_municipio, -votos)

votos_prov <-
  bind_rows(
    votos_muni %>% select(-codigo_municipio),
    votos_pre01
  ) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year, codigo_provincia, -votos)

votos_ccaa <-
  votos_prov %>%
  select(-codigo_provincia) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year, -votos)

# ===========================================================================
# BIND + JOIN IDs
# ===========================================================================

info_cer <-
  bind_rows(info_ccaa, info_prov, info_muni, info_seccion) %>%
  mutate(
    across(c(codigo_provincia, codigo_distrito), ~ ifelse(is.na(.), "99", .)),
    codigo_municipio = ifelse(is.na(codigo_municipio), "999", codigo_municipio),
    codigo_seccion   = ifelse(is.na(codigo_seccion), "9999", codigo_seccion)
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

votos_cer <-
  bind_rows(votos_ccaa, votos_prov, votos_muni, votos_seccion) %>%
  mutate(
    across(c(codigo_provincia, codigo_distrito), ~ ifelse(is.na(.), "99", .)),
    codigo_municipio = ifelse(is.na(codigo_municipio), "999", codigo_municipio),
    codigo_seccion   = ifelse(is.na(codigo_seccion), "9999", codigo_seccion)
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

# ===========================================================================
# CHECKS
# ===========================================================================

validate_info(info_cer, label = "12-galicia/info_cer")
validate_votos(votos_cer, label = "12-galicia/votos_cer")
validate_info_votos_consistency(info_cer, votos_cer, label = "12-galicia/cer")

# ===========================================================================
# WRITE DATA
# ===========================================================================

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

saveRDS(info_cer, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos_cer, paste0(OUTPUT_DIR, "votos.rds"))
