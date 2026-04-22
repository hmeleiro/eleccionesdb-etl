library(dplyr)
library(readr)
library(stringr)
library(tidyr)
library(readxl)
library(purrr)

CODIGOS_DIR <- "data-raw/codigos_territorios"

source("R/tests/validate_data_processed.R")

INPUT_DIR <- "data-raw/hechos/03-asturias/"
OUTPUT_DIR <- "data-processed/hechos/03-asturias/"

# --- Dimensiones ---
fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = F) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == "03") %>%
  transmute(eleccion_id = id, year = as.character(year))

territorios <-
  read_csv("tablas-finales/dimensiones/territorios", show_col_types = F) %>%
  select(
    territorio_id = id, codigo_ccaa, codigo_provincia,
    codigo_municipio, codigo_distrito, codigo_seccion,
    codigo_circunscripcion
  )

# Correspondencia municipio-circunscripción (CSV centralizado)
correspondencia <- read_csv(
  file.path(CODIGOS_DIR, "correspondencia_municipio_circunscripcion.csv"),
  col_types = cols(.default = "c")
) %>%
  filter(codigo_provincia == "33")


# ==========================================================================
# 2015 EN ADELANTE (GIPEYOP — nivel mesa)
# ==========================================================================

files_gipeyop <- list.files(INPUT_DIR, full.names = T, recursive = T)
files_gipeyop <- files_gipeyop[str_detect(files_gipeyop, "gipeyop")]

data_15 <- map_df(files_gipeyop, function(file) {
  rename_cols <- c(
    "year" = "anyo",
    "codigo_provincia" = "cod_prov",
    "codigo_municipio" = "cod_mun",
    "codigo_distrito" = "distrito",
    "codigo_seccion" = "seccion",
    "codigo_mesa" = "mesa",
    "censo_ine" = "censo_total",
    "votos_blancos" = "blancos",
    "votos_nulos" = "nulos",
    "votos_totales" = "votos"
  )

  tmp <- read_xlsx(file, sheet = "MESAS")
  c <- max(which(str_detect(colnames(tmp), "VOTOS|BLANCOS|NULOS")))
  colnames(tmp)[1:c] <- janitor::make_clean_names(colnames(tmp)[1:c])

  tmp %>%
    rename(any_of(rename_cols)) %>%
    pivot_longer((c + 1):ncol(.), names_to = "partido", values_to = "votos") %>%
    mutate(
      year = as.character(year),
      abstenciones = censo_ine - votos_totales,
      votos_validos = votos_totales - votos_nulos
    )
}) %>%
  transmute(
    year,
    codigo_ccaa = "03",
    codigo_provincia = str_pad(as.character(codigo_provincia), 2, "left", "0"),
    codigo_municipio = str_pad(as.character(codigo_municipio), 3, "left", "0"),
    codigo_distrito = str_pad(as.character(codigo_distrito), 2, "left", "0"),
    codigo_seccion = str_pad(as.character(codigo_seccion), 4, "left", "0"),
    codigo_mesa = as.character(codigo_mesa),
    censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos,
    siglas = partido,
    denominacion = partido,
    votos
  ) %>%
  left_join(correspondencia, by = c("codigo_provincia", "codigo_municipio"))

# CER (sin CERA: 991/992/993 son residentes ausentes por circunscripción)
data_15_cer <- data_15 %>% filter(!codigo_municipio %in% c("991", "992", "993"))


# ==========================================================================
# PREVIO A 2015 — INFO (SADEI .px, nivel municipio)
# ==========================================================================

nomenclator <- readRDS(file.path(INPUT_DIR, "SADEI", "nomenclator.rds")) %>%
  select(ambito_territorial = value, codigo_municipio = code)

info_px <- pxR::read.px(file.path(INPUT_DIR, "SADEI", "190402_20240307-113728.px"))

rename_cols_px <- c(
  "censo_ine" = "censo_electoral",
  "votos_blancos" = "votos_en_blanco"
)

info_mun_pre15 <-
  info_px$DATA$value %>%
  janitor::clean_names() %>%
  left_join(nomenclator, by = "ambito_territorial") %>%
  filter(!codigo_municipio %in% c("33000", "33501", "33502", "33503")) %>%
  select(-ambito_territorial) %>%
  pivot_wider(names_from = "censo_votos_abstencion", values_from = "value") %>%
  janitor::clean_names() %>%
  rename(any_of(rename_cols_px)) %>%
  separate(codigo_municipio, into = c("codigo_provincia", "codigo_municipio"), sep = 2) %>%
  separate(convocatoria_electoral, into = c("day", "mes", "year"), sep = " de ") %>%
  transmute(
    year,
    codigo_ccaa = "03",
    codigo_provincia,
    codigo_municipio,
    censo_ine, votos_blancos, votos_nulos,
    abstenciones = abstencion,
    votos_validos = votos_emitidos - votos_nulos
  ) %>%
  filter(year < 2015, codigo_municipio != "099") %>%
  left_join(correspondencia, by = c("codigo_provincia", "codigo_municipio"))


# ==========================================================================
# PREVIO A 2015 — VOTOS (SADEI Excel, nivel municipio)
# ==========================================================================

files_sadei <- list.files(INPUT_DIR, full.names = T, recursive = T)
files_sadei <- files_sadei[str_detect(files_sadei, "data_")]

votos_mun_pre15 <-
  map_df(files_sadei, function(file) {
    year <- str_remove_all(file, ".+data_|\\.xlsx")

    x <- read_xlsx(file)[, 3] %>% pull()
    sk <- which(x == "Votos") + 1

    tmp <- read_xlsx(file, skip = sk)
    c <- which(colnames(tmp) == "TOTAL")

    tmp %>%
      mutate(across(any_of(c):ncol(.), as.numeric)) %>%
      rename("codigo_municipio" = 1, "municipio" = 2) %>%
      filter(
        !is.na(TOTAL),
        !codigo_municipio %in% c("33000", "33501", "33502", "33503")
      ) %>%
      pivot_longer((c + 1):ncol(.), names_to = "partido", values_to = "votos") %>%
      separate(partido, into = c("siglas", "denominacion"), sep = " - ", extra = "merge") %>%
      separate(codigo_municipio, into = c("codigo_provincia", "codigo_municipio"), sep = 2) %>%
      select(-c(municipio, TOTAL)) %>%
      mutate(year, codigo_ccaa = "03", .before = 1)
  }) %>%
  filter(year < "2015", !codigo_municipio %in% c("079", "080", "081")) %>%
  left_join(correspondencia, by = c("codigo_provincia", "codigo_municipio"))


# ==========================================================================
# INFO — Agregación jerárquica
# ==========================================================================

# MESAS (solo 2015+)
info_mesas <-
  data_15_cer %>%
  select(-c(siglas, denominacion, votos)) %>%
  distinct() %>%
  arrange(year, codigo_municipio, codigo_distrito, codigo_seccion, codigo_mesa)

# SECCIONES (solo 2015+)
info_seccion <-
  info_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# MUNICIPIOS (2015+ desde secciones + pre-2015 desde SADEI)
info_muni <-
  info_seccion %>%
  select(-c(codigo_distrito, codigo_seccion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup() %>%
  bind_rows(info_mun_pre15)

# CIRCUNSCRIPCION
info_circ <-
  info_muni %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# PROVINCIA
info_prov <-
  info_circ %>%
  select(-codigo_circunscripcion) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# CCAA
info_ccaa <-
  info_prov %>%
  select(-codigo_provincia) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = T))) %>%
  ungroup() %>%
  arrange(year)

# COMBINAR NIVELES
info_cer <-
  bind_rows(info_ccaa, info_prov, info_circ, info_muni, info_seccion) %>%
  mutate(
    across(
      c(codigo_provincia, codigo_distrito),
      ~ ifelse(is.na(.), "99", .)
    ),
    codigo_municipio = ifelse(is.na(codigo_municipio), "999", codigo_municipio),
    codigo_seccion = ifelse(is.na(codigo_seccion), "9999", codigo_seccion)
  ) %>%
  left_join(fechas, by = "year") %>%
  left_join(
    territorios,
    by = c(
      "codigo_ccaa", "codigo_provincia", "codigo_municipio",
      "codigo_distrito", "codigo_seccion", "codigo_circunscripcion"
    )
  ) %>%
  arrange(eleccion_id, territorio_id) %>%
  select(-c(year, starts_with("codigo_"))) %>%
  relocate(eleccion_id, territorio_id) %>%
  mutate(
    abstenciones = ifelse(abstenciones < 0, NA_integer_, abstenciones),
    abstenciones = ifelse(!is.na(votos_validos) & !is.na(abstenciones) &
      abstenciones + votos_validos > censo_ine,
    NA_integer_, abstenciones
    )
  )

# Eliminar secciones que no existen en la dimensión territorios
# (p.ej. secciones alfanuméricas de 2019 en Oviedo: 001A, 002B…)
n_missing_info <- sum(is.na(info_cer$territorio_id))
if (n_missing_info > 0) {
  message(sprintf(
    "AVISO: %d filas info sin territorio_id (secciones ausentes en dimensión). Se eliminan.",
    n_missing_info
  ))
  info_cer <- info_cer %>% filter(!is.na(territorio_id))
}


# ==========================================================================
# VOTOS — Agregación jerárquica
# ==========================================================================

# MESAS (solo 2015+)
votos_mesas <-
  data_15_cer %>%
  select(-c(votos_nulos, votos_blancos, votos_validos, abstenciones, censo_ine))

# SECCIONES (solo 2015+)
votos_seccion <-
  votos_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# MUNICIPIOS (2015+ desde secciones + pre-2015 desde SADEI)
votos_muni <-
  votos_seccion %>%
  select(-c(codigo_distrito, codigo_seccion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup() %>%
  bind_rows(votos_mun_pre15)

# CIRCUNSCRIPCION
votos_circ <-
  votos_muni %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# PROVINCIA
votos_prov <-
  votos_circ %>%
  select(-codigo_circunscripcion) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# CCAA
votos_ccaa <-
  votos_prov %>%
  select(-codigo_provincia) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = T))) %>%
  ungroup() %>%
  arrange(year, -votos)

# COMBINAR NIVELES
votos_cer <-
  bind_rows(votos_ccaa, votos_prov, votos_circ, votos_muni, votos_seccion) %>%
  mutate(
    across(
      c(codigo_provincia, codigo_distrito),
      ~ ifelse(is.na(.), "99", .)
    ),
    codigo_municipio = ifelse(is.na(codigo_municipio), "999", codigo_municipio),
    codigo_seccion = ifelse(is.na(codigo_seccion), "9999", codigo_seccion)
  ) %>%
  left_join(fechas, by = "year") %>%
  left_join(
    territorios,
    by = c(
      "codigo_ccaa", "codigo_provincia", "codigo_municipio",
      "codigo_distrito", "codigo_seccion", "codigo_circunscripcion"
    )
  ) %>%
  arrange(eleccion_id, territorio_id) %>%
  select(-c(year, starts_with("codigo_"))) %>%
  relocate(eleccion_id, territorio_id)


# Mesas y secciones solo existen para 2015+
info_mesas <- info_mesas %>% filter(!is.na(codigo_mesa))
votos_mesas <- votos_mesas %>% filter(!is.na(codigo_mesa))

# ==========================================================================
# CHECKS
# ==========================================================================

validate_info(info_cer, label = "03-asturias/info_cer")
validate_votos(votos_cer, label = "03-asturias/votos_cer")
validate_info_votos_consistency(info_cer, votos_cer, label = "03-asturias/cer")
validate_votos_partido_match(votos_cer, label = "03-asturias/votos_cer")


# ==========================================================================
# WRITE DATA
# ==========================================================================

dir.create(OUTPUT_DIR, showWarnings = FALSE)

saveRDS(info_cer, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos_cer, paste0(OUTPUT_DIR, "votos.rds"))
