library(dplyr)
library(readr)
library(purrr)
library(stringr)
library(tidyr)
library(readxl)

source("R/tests/validate_data_processed.R")

INPUT_DIR <- "data-raw/hechos/10-comunidad-valenciana/"
OUTPUT_DIR <- "data-processed/hechos/10-comunidad-valenciana/"

# --- Dimensiones ---
fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = F) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == "10") %>%
  transmute(eleccion_id = id, year = as.character(year))

territorios <-
  read_csv("tablas-finales/dimensiones/territorios", show_col_types = F) %>%
  select(
    territorio_id = id, codigo_ccaa, codigo_provincia,
    codigo_municipio, codigo_distrito, codigo_seccion
  )

# --- Función auxiliar para leer ficheros CSV ---
read_file <- function(file) {
  rename_cols <- c(
    "year" = "anyo",
    "year" = "any",
    "codigo_provincia" = "cod_prov",
    "codigo_provincia" = "c_prov",
    "codigo_municipio" = "cod_municipio",
    "codigo_municipio" = "c_mun",
    "codigo_distrito" = "distrito",
    "codigo_distrito" = "districte",
    "codigo_seccion" = "seccion",
    "codigo_seccion" = "seccio",
    "codigo_seccion" = "secci",
    "codigo_mesa" = "mesa",
    "censo_ine" = "censo",
    "censo_ine" = "cens",
    "votos_blancos" = "blancos",
    "votos_blancos" = "v_blanc",
    "votos_nulos" = "nulos",
    "votos_nulos" = "v_nuls",
    "siglas" = "candidato_siglas",
    "siglas" = "sigles_candidatura",
    "votantes" = "votants",
    "denominacion" = "candidatos_desc",
    "denominacion" = "candidato_desc",
    "denominacion" = "desc_candidatura",
    "votos_validos" = "validos",
    "votos" = "v_cand"
  )
  lcle <- locale(
    encoding = (guess_encoding(file) %>% pull(1))[1],
    decimal_mark = ",", grouping_mark = "."
  )

  tmp <- read_delim(file, show_col_types = F, delim = ";", locale = lcle)

  if (ncol(tmp) == 1) {
    tmp <- read_delim(file, show_col_types = F, delim = ",", locale = lcle)
  }

  tmp %>%
    janitor::clean_names() %>%
    rename(any_of(rename_cols)) %>%
    mutate(across(c(year, starts_with("codigo_")), as.character))
}

# ==========================================================================
# 1983 (SOLO MUNICIPIOS)
# ==========================================================================

rename_cols_83 <- c(
  "year" = "anyo",
  "codigo_provincia" = "cod_prov",
  "codigo_municipio" = "cod_mun_2",
  "censo_ine" = "censo",
  "votos_blancos" = "blancos",
  "votos_nulos" = "nulos",
  "votos_validos" = "validos"
)

data83 <-
  read_xlsx(paste0(INPUT_DIR, "Comunitat Valenciana1983_municipios.xlsx"), sheet = "MUNICIPIOS") %>%
  pivot_longer(17:ncol(.), names_to = "siglas", values_to = "votos") %>%
  janitor::clean_names() %>%
  rename(any_of(rename_cols_83)) %>%
  mutate(
    year = as.character(year),
    siglas = gsub("\\.", "_", siglas),
    denominacion = siglas
  ) %>%
  select(-any_of("mes")) %>%
  filter(!is.na(votos))

# ==========================================================================
# 1987 EN ADELANTE (MESAS)
# ==========================================================================

files <- list.files(INPUT_DIR, pattern = "[0-9]{4}", full.names = T)
files <- files[grepl("csv", files)]

data <-
  map_df(files, read_file) %>%
  bind_rows(data83) %>%
  transmute(
    year,
    codigo_ccaa = "10",
    codigo_provincia = case_when(
      is.na(codigo_provincia) & nchar(codigo_municipio) == 5 ~ substr(codigo_municipio, 1, 2),
      is.na(codigo_provincia) & nchar(codigo_municipio) == 4 ~ substr(codigo_municipio, 1, 1),
      TRUE ~ codigo_provincia
    ),
    codigo_municipio = case_when(
      nchar(codigo_municipio) < 3 ~ str_pad(codigo_municipio, 3, "left", "0"),
      nchar(codigo_municipio) == 4 ~ substr(codigo_municipio, 2, 4),
      nchar(codigo_municipio) == 5 ~ substr(codigo_municipio, 3, 5),
      TRUE ~ codigo_municipio
    ),
    across(c(codigo_provincia, codigo_distrito), ~ str_pad(., 2, "left", "0")),
    codigo_circunscripcion = codigo_provincia,
    codigo_seccion = str_pad(codigo_seccion, 4, "left", "0"),
    codigo_mesa,
    censo_ine,
    # En origen, hay territorios donde los votos (válidos + nulos) superan al censo.
    # En esos casos, asigno el censo como la suma de votos emitidos + abstenciones (?)
    # censo_ine = ifelse(abstenciones + votos_validos + votos_nulos > censo_ine, abstenciones + votos_validos + votos_nulos, censo_ine),
    abstenciones = ifelse(is.na(abstenciones), censo_ine - votantes, abstenciones),
    votos_validos = ifelse(is.na(votos_validos), votantes - votos_nulos, votos_validos),
    votos_blancos, votos_nulos,
    siglas, denominacion, votos
  ) %>%
  arrange(year, across(starts_with("codigo_")))

data_cer <- data %>% filter(codigo_municipio != "990")
data_cera <- data %>% filter(codigo_municipio == "990")


# ==========================================================================
# INFO — Agregación jerárquica
# ==========================================================================

# MESAS
info_mesas <-
  data_cer %>%
  select(-c(siglas, denominacion, votos)) %>%
  distinct()

# SECCIONES
info_seccion <-
  info_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup() %>%
  filter(!is.na(codigo_seccion))

# MUNICIPIOS
info_muni <-
  info_seccion %>%
  select(-c(codigo_distrito, codigo_seccion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# PROVINCIAS
info_prov <-
  info_muni %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# CCAA
info_ccaa <-
  info_prov %>%
  select(-c(codigo_provincia, codigo_circunscripcion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = T))) %>%
  ungroup() %>%
  arrange(year)

# COMBINAR NIVELES
info_cer <-
  bind_rows(info_ccaa, info_prov, info_muni, info_seccion) %>%
  mutate(
    across(
      c(codigo_provincia, codigo_circunscripcion, codigo_distrito),
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
      "codigo_distrito", "codigo_seccion"
    )
  ) %>%
  arrange(eleccion_id, territorio_id) %>%
  select(-c(year, starts_with("codigo_"))) %>%
  relocate(eleccion_id, territorio_id) %>%
  mutate(
    abstenciones = ifelse(abstenciones < 0, NA_integer_, abstenciones)
  )


# ==========================================================================
# VOTOS — Agregación jerárquica
# ==========================================================================

# MESAS
votos_mesas <-
  data_cer %>%
  select(-c(censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos))

# SECCIONES
votos_seccion <-
  votos_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup() %>%
  filter(!is.na(codigo_seccion))

# MUNICIPIOS
votos_muni <-
  votos_seccion %>%
  select(-c(codigo_distrito, codigo_seccion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# PROVINCIAS
votos_prov <-
  votos_muni %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# CCAA
votos_ccaa <-
  votos_prov %>%
  select(-c(codigo_provincia, codigo_circunscripcion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = T))) %>%
  ungroup() %>%
  arrange(year, -votos)

# COMBINAR NIVELES
votos_cer <-
  bind_rows(votos_ccaa, votos_prov, votos_muni, votos_seccion) %>%
  mutate(
    across(
      c(codigo_provincia, codigo_circunscripcion, codigo_distrito),
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
      "codigo_distrito", "codigo_seccion"
    )
  ) %>%
  arrange(eleccion_id, territorio_id) %>%
  select(-c(year, starts_with("codigo_"))) %>%
  relocate(eleccion_id, territorio_id)

# Elimino los datos de 1983 de mesas/secciones (no tienen ese nivel)
info_mesas <- info_mesas %>% filter(!is.na(codigo_mesa))
votos_mesas <- votos_mesas %>% filter(!is.na(codigo_mesa))
info_seccion <- info_seccion %>% filter(!is.na(codigo_seccion))
votos_seccion <- votos_seccion %>% filter(!is.na(codigo_seccion))


# ==========================================================================
# CHECKS
# ==========================================================================

validate_info(info_cer, label = "10-comunidad-valenciana/info_cer")
validate_votos(votos_cer, label = "10-comunidad-valenciana/votos_cer")
validate_info_votos_consistency(info_cer, votos_cer, label = "10-comunidad-valenciana/cer")
validate_votos_partido_match(votos_cer, label = "10-comunidad-valenciana/votos_cer")

# ==========================================================================
# WRITE DATA
# ==========================================================================

dir.create(OUTPUT_DIR, showWarnings = FALSE)

saveRDS(info_cer, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos_cer, paste0(OUTPUT_DIR, "votos.rds"))
