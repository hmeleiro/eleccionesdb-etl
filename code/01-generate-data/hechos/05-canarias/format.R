library(dplyr)
library(readr)
library(stringr)
library(tidyr)
library(readxl)
library(purrr)

source("code/tests/validate_data_processed.R")

INPUT_DIR <- "data-raw/hechos/05-canarias/"
OUTPUT_DIR <- "data-processed/hechos/05-canarias/"

# --- Dimensiones ---
fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = F) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == "05") %>%
  transmute(eleccion_id = id, year = as.character(year))

territorios <-
  read_csv("tablas-finales/dimensiones/territorios", show_col_types = F) %>%
  select(
    territorio_id = id, codigo_ccaa, codigo_provincia,
    codigo_municipio, codigo_distrito, codigo_seccion,
    codigo_circunscripcion
  )


# ==========================================================================
# PARCAN (1999+) — nivel mesa
# ==========================================================================

files_parcan <- list.files(INPUT_DIR, full.names = T, recursive = T)
files_parcan <- files_parcan[str_detect(files_parcan, "/parcan/")]

data_parcan <-
  map_df(files_parcan, function(file) {
    rename_cols <- c(
      "year" = "eleccion",
      "codigo_circunscripcion" = "id_circunscripcion",
      "codigo_municipio" = "id_municipio",
      "codigo_distrito" = "distrito",
      "codigo_seccion" = "seccion",
      "codigo_mesa" = "mesa",
      "censo_ine" = "censados",
      "votos_blancos" = "votos_blancos",
      "votos_nulos" = "papeletas_nulas"
    )

    read_csv(file, show_col_types = F) %>%
      janitor::clean_names() %>%
      rename(any_of(rename_cols)) %>%
      mutate(
        year = as.character(year),
        across(starts_with("codigo_"), as.character),
        codigo_ccaa = "05",
        codigo_circunscripcion = case_when(
          codigo_circunscripcion == "1" ~ "382",
          codigo_circunscripcion == "2" ~ "351",
          codigo_circunscripcion == "3" ~ "352",
          codigo_circunscripcion == "4" ~ "381",
          codigo_circunscripcion == "5" ~ "353",
          codigo_circunscripcion == "6" ~ "383",
          codigo_circunscripcion == "7" ~ "384",
          codigo_circunscripcion == "8" ~ "050",
          TRUE ~ NA_character_
        ),
        # 2019 autonomous ballot uses fictitious provinces 95/98 → normalize
        codigo_provincia = case_when(
          substr(codigo_municipio, 1, 2) == "95" ~ "35",
          substr(codigo_municipio, 1, 2) == "98" ~ "38",
          TRUE ~ substr(codigo_municipio, 1, 2)
        ),
        codigo_municipio = substr(codigo_municipio, 3, 5),
        codigo_distrito = str_pad(codigo_distrito, 2, "left", "0"),
        codigo_municipio = str_pad(codigo_municipio, 3, "left", "0"),
        codigo_seccion = str_pad(codigo_seccion, 4, "left", "0")
      )
  }) %>%
  group_by(year, across(starts_with("codigo_"))) %>%
  mutate(
    votos_validos = votos_blancos + sum(votos),
    abstenciones = censo_ine - votos_validos - votos_nulos,
    abstenciones = pmax(abstenciones, 0L)
  ) %>%
  ungroup() %>%
  transmute(
    year,
    codigo_ccaa, codigo_provincia, codigo_municipio,
    codigo_distrito, codigo_seccion,
    codigo_mesa = as.character(codigo_mesa),
    codigo_circunscripcion,
    censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos,
    siglas, denominacion = partido,
    votos
  )

# Separar dato insular (circ 1-7) del autonómico (circ 8 = 050)
data_isla <- data_parcan %>% filter(codigo_circunscripcion != "050")
data_auto <- data_parcan %>% filter(codigo_circunscripcion == "050")

# CER: excluir CERA (municipios con código >= 990)
data_isla_cer <- data_isla %>%
  filter(
    as.numeric(codigo_municipio) < 990,
    codigo_provincia %in% c("35", "38")
  )

data_auto_cer <- data_auto %>%
  filter(
    as.numeric(codigo_municipio) < 990,
    codigo_provincia %in% c("35", "38")
  )


# ==========================================================================
# GIPEYOP (1991-1995) — nivel sección
# ==========================================================================

files_gipeyop <- list.files(INPUT_DIR, full.names = T, recursive = T)
files_gipeyop <- files_gipeyop[str_detect(files_gipeyop, "/gipeyop/") &
  str_detect(files_gipeyop, "1991|1995")]

data_gipeyop <- map_df(files_gipeyop, function(file) {
  rename_cols <- c(
    "year" = "anyo",
    "codigo_provincia" = "cod_prov",
    "codigo_circunscripcion" = "cod_isla",
    "codigo_municipio" = "cod_mun",
    "codigo_distrito" = "distrito",
    "codigo_seccion" = "seccion",
    "censo_ine" = "censo",
    "votos_blancos" = "blancos",
    "votos_totales" = "total_votos",
    "votos_nulos" = "nulos",
    "votos_validos" = "validos"
  )

  tmp <- read_xlsx(file, sheet = "SECCIONES")
  c <- max(which(str_detect(colnames(tmp), "VOTOS|BLANCOS|NULOS")))
  colnames(tmp)[1:c] <- janitor::make_clean_names(colnames(tmp)[1:c])

  tmp %>%
    rename(any_of(rename_cols)) %>%
    pivot_longer((c + 1):ncol(.), names_to = "partido", values_to = "votos") %>%
    mutate(
      year = as.character(year),
      across(starts_with("codigo_"), as.character),
      abstenciones = censo_ine - votos_totales
    ) %>%
    select(!any_of(c("mes", "votos_totales", "votos_candidaturas")))
}) %>%
  transmute(
    year,
    codigo_ccaa = "05",
    codigo_provincia = str_pad(codigo_provincia, 2, "left", "0"),
    codigo_municipio = str_pad(codigo_municipio, 3, "left", "0"),
    codigo_distrito = str_pad(codigo_distrito, 2, "left", "0"),
    codigo_seccion = str_pad(codigo_seccion, 4, "left", "0"),
    codigo_circunscripcion,
    censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos,
    siglas = partido,
    denominacion = partido,
    votos
  )


# ==========================================================================
# 1983-1987 — solo nivel provincial (info + votos agrupados por provincia)
# ==========================================================================

info_prov_83_87 <- read_xlsx(paste0(INPUT_DIR, "info_prov_canarias_83_87.xlsx")) %>%
  mutate(
    across(c(starts_with("codigo_"), year), as.character),
    votos_validos = validos,
    abstenciones = abstencion,
    codigo_circunscripcion = NA_character_
  ) %>%
  select(
    year, codigo_ccaa, codigo_provincia, codigo_circunscripcion,
    censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos
  )

votos_prov_83_87 <-
  read_xlsx(paste0(INPUT_DIR, "voto_circunscripciones_83_87.xlsx")) %>%
  mutate(across(c(starts_with("codigo_"), year), as.character)) %>%
  group_by(year, codigo_ccaa, codigo_provincia, siglas = partido, denominacion = partido) %>%
  summarise(votos = sum(votos, na.rm = T), .groups = "drop")


# ==========================================================================
# INFO — Agregación jerárquica (dato insular: 1991+ ; 83-87 solo provincial)
# ==========================================================================

# MESAS (1999+ parcan)
info_mesas <-
  data_isla_cer %>%
  select(-c(siglas, denominacion, votos)) %>%
  distinct()

# SECCIONES (1999+ desde mesas + 1991-1995 GIPEYOP)
info_seccion <-
  info_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T)), .groups = "drop") %>%
  bind_rows(
    data_gipeyop %>%
      select(-c(siglas, denominacion, votos)) %>%
      distinct()
  )

# MUNICIPIOS
info_muni <-
  info_seccion %>%
  select(-c(codigo_distrito, codigo_seccion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T)), .groups = "drop")

# CIRCUNSCRIPCION (isla)
info_circ <-
  info_muni %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T)), .groups = "drop")

# PROVINCIA (isla 1991+ agregado + 83-87)
info_prov <-
  info_circ %>%
  select(-codigo_circunscripcion) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T)), .groups = "drop") %>%
  bind_rows(info_prov_83_87)

# CCAA
info_ccaa <-
  info_prov %>%
  select(-codigo_provincia) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T)), .groups = "drop")

# CIRC AUTONOMICA (050): agregar todo el dato autonómico a nivel circunscripción
info_auto_circ <-
  data_auto_cer %>%
  select(-c(siglas, denominacion, votos)) %>%
  distinct() %>%
  group_by(year, codigo_ccaa, codigo_circunscripcion) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T)), .groups = "drop") %>%
  mutate(codigo_provincia = "99")

# COMBINAR NIVELES
info_cer <-
  bind_rows(info_ccaa, info_prov, info_circ, info_auto_circ, info_muni, info_seccion) %>%
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
      "codigo_distrito", "codigo_seccion", "codigo_circunscripcion"
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

# MESAS (1999+ parcan)
votos_mesas <-
  data_isla_cer %>%
  select(-c(votos_nulos, votos_blancos, votos_validos, abstenciones, censo_ine))

# SECCIONES (1999+ desde mesas + 1991-1995 GIPEYOP)
votos_seccion <-
  votos_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T)), .groups = "drop") %>%
  bind_rows(
    data_gipeyop %>%
      select(-c(votos_nulos, votos_blancos, votos_validos, abstenciones, censo_ine))
  )

# MUNICIPIOS
votos_muni <-
  votos_seccion %>%
  select(-c(codigo_distrito, codigo_seccion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T)), .groups = "drop")

# CIRCUNSCRIPCION (isla)
votos_circ <-
  votos_muni %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T)), .groups = "drop")

# PROVINCIA (isla 1991+ agregado + 83-87)
votos_prov <-
  votos_circ %>%
  select(-codigo_circunscripcion) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T)), .groups = "drop") %>%
  bind_rows(votos_prov_83_87)

# CCAA
votos_ccaa <-
  votos_prov %>%
  select(-codigo_provincia) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T)), .groups = "drop")

# CIRC AUTONOMICA (050): agregar votos autonómicos a nivel circunscripción
votos_auto_circ <-
  data_auto_cer %>%
  select(-c(votos_nulos, votos_blancos, votos_validos, abstenciones, censo_ine, codigo_mesa)) %>%
  group_by(year, codigo_ccaa, codigo_circunscripcion, siglas, denominacion) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T)), .groups = "drop") %>%
  mutate(codigo_provincia = "99")

# COMBINAR NIVELES
votos_cer <-
  bind_rows(votos_ccaa, votos_prov, votos_circ, votos_auto_circ, votos_muni, votos_seccion) %>%
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
      "codigo_distrito", "codigo_seccion", "codigo_circunscripcion"
    )
  ) %>%
  arrange(eleccion_id, territorio_id) %>%
  select(-c(year, starts_with("codigo_"))) %>%
  relocate(eleccion_id, territorio_id)

# Mesas solo para parcan (1999+)
info_mesas <- info_mesas %>% filter(!is.na(codigo_mesa))
votos_mesas <- votos_mesas %>% filter(!is.na(codigo_mesa))

# ==========================================================================
# CHECKS
# ==========================================================================

validate_info(info_cer, label = "05-canarias/info_cer")
validate_votos(votos_cer, label = "05-canarias/votos_cer")
validate_info_votos_consistency(info_cer, votos_cer, label = "05-canarias/cer")


# ==========================================================================
# WRITE DATA
# ==========================================================================

dir.create(OUTPUT_DIR, showWarnings = FALSE)

saveRDS(info_cer, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos_cer, paste0(OUTPUT_DIR, "votos.rds"))
