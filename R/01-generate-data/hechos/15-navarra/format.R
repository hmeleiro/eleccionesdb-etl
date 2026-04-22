library(dplyr)
library(readr)
library(purrr)
library(stringr)
library(readxl)
library(tidyr)

source("R/tests/validate_data_processed.R")

INPUT_DIR <- "data-raw/hechos/15-navarra/"
OUTPUT_DIR <- "data-processed/hechos/15-navarra/"

fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == "15") %>%
  transmute(eleccion_id = id, year = as.character(year))

territorios <-
  read_csv("tablas-finales/dimensiones/territorios", show_col_types = FALSE) %>%
  select(
    territorio_id = id,
    codigo_ccaa, codigo_provincia, codigo_municipio,
    codigo_distrito, codigo_seccion
  )

rename_cols <- c(
  "codigo_circunscripcion" = "codcir",
  "codigo_municipio" = "codmun",
  "codigo_municipio" = "codigo",
  "codigo_municipio" = "cod",
  "codigo_municipio" = "codigo_de_municipio",
  "codigo_distrito" = "distrito",
  "codigo_distrito" = "distr",
  "codigo_seccion" = "seccion",
  "codigo_seccion" = "secc",
  "codigo_mesa" = "mesa",
  "censo_ine" = "censo_electoral",
  "censo_ine" = "censo",
  "votos_nulos" = "nulos",
  "votos_nulos" = "votos_nulo",
  "abstenciones" = "abstenciones",
  "votos_validos" = "v_validos",
  "votos_validos" = "validos",
  "votos_validos" = "votos_validos",
  "votos_blancos" = "blancos",
  "votos_blancos" = "votos_blanco",
  "votos_blancos" = "blanco",
  "votos_blancos" = "votos_en_blanco",
  "votantes" = "votos_totales"
)

# ===========================================================================
# POST 1999 (POR MESAS — CSV)
# ===========================================================================

files <- list.files(INPUT_DIR, pattern = "csv", full.names = TRUE)

data <-
  map_df(files, function(file) {
    year <- str_extract(file, "[0-9]{4}")

    tmp <- read_csv(file, show_col_types = FALSE, locale = locale(grouping_mark = ".", decimal_mark = ","))

    c <- max(which(str_detect(tolower(colnames(tmp)), "nulo|nulos|blanco|blancos|válidos|validos|cand"))) + 1

    tmp <-
      tmp %>%
      select(!ends_with("_P")) %>%
      pivot_longer(any_of(c):ncol(.), names_to = "partido", values_to = "votos") %>%
      janitor::clean_names() %>%
      rename(any_of(rename_cols))

    if (!"codigo_municipio" %in% colnames(tmp)) {
      tmp <-
        tmp %>%
        rename(any_of(c("codigo_municipio" = "municipio")))
    }

    if (!"codigo_seccion" %in% colnames(tmp)) {
      tmp <-
        tmp %>%
        separate(codigo_mesa, into = c("codigo_distrito", "codigo_seccion", "codigo_mesa"), sep = "-")
    }

    tmp %>%
      mutate(
        year,
        across(starts_with("codigo_"), as.character),
        across(starts_with("codigo_"), str_trim),
        codigo_municipio = str_pad(codigo_municipio, 3, "left", "0"),
        codigo_seccion = str_pad(codigo_seccion, 4, "left", "0"),
        codigo_distrito = str_pad(codigo_distrito, 2, "left", "0")
      )
  }) %>%
  mutate(
    codigo_municipio = ifelse(is.na(codigo_municipio) & municipio == "C. E. R. A.", "999", codigo_municipio),
    abstenciones = ifelse(is.na(abstenciones), censo_ine - votantes, abstenciones),
    votos_validos = ifelse(is.na(votos_validos), votantes - votos_nulos, votos_validos)
  ) %>%
  transmute(
    year,
    codigo_ccaa = "15",
    codigo_provincia = "31",
    codigo_circunscripcion = "31",
    across(starts_with("codigo_")),
    censo_ine, abstenciones, votos_validos, votos_nulos, votos_blancos,
    siglas = as.character(partido),
    denominacion = as.character(partido),
    votos
  )


# ===========================================================================
# PRE 1999 (POR MUNICIPIOS — XLS)
# ===========================================================================

files <- list.files(INPUT_DIR, pattern = "xls", full.names = TRUE)

data_municipios <-
  map_df(files, function(file) {
    year <- str_extract(file, "[0-9]{4}")

    tmp <- read_excel(file, skip = 2, sheet = "Municipios")

    c <- max(which(str_detect(tolower(colnames(tmp)), "candidaturas|nulo|nulos|blanco|blancos|válidos|validos|cand"))) + 1

    tmp %>%
      pivot_longer(any_of(c):ncol(.), names_to = "partido", values_to = "votos") %>%
      janitor::clean_names() %>%
      rename(any_of(rename_cols)) %>%
      mutate(
        year,
        across(starts_with("codigo_"), as.character),
        codigo_municipio = str_pad(codigo_municipio, 3, "left", "0"),
      )
  }) %>%
  filter(!is.na(codigo_municipio)) %>%
  mutate(votos_validos = ifelse(is.na(votos_validos), censo_ine - votos_nulos - abstenciones, votos_validos)) %>%
  transmute(
    year,
    codigo_ccaa = "15",
    codigo_provincia = "31",
    codigo_circunscripcion = "31",
    across(starts_with("codigo_")),
    censo_ine, abstenciones,
    votos_validos, votos_nulos, votos_blancos,
    siglas = as.character(partido),
    denominacion = as.character(partido),
    votos
  ) %>%
  filter(year < 1999)

# ===========================================================================
# Combinar datos
# ===========================================================================

data <- data %>%
  filter(!is.na(codigo_municipio)) %>%
  bind_rows(data_municipios) %>%
  filter(censo_ine > 0) %>%
  mutate(codigo_municipio = ifelse(codigo_municipio == "9999", "999", codigo_municipio))

data_cer <- data %>% filter(!codigo_municipio %in% c("990", "899", "999"))

# ===========================================================================
# INFO — separar y agregar jerárquicamente
# ===========================================================================

info_mesas <-
  data_cer %>%
  select(-c(siglas, denominacion, votos)) %>%
  distinct() %>%
  filter(!is.na(codigo_mesa)) %>%
  arrange(year, codigo_municipio, codigo_seccion, codigo_distrito, codigo_mesa)

info_seccion <-
  info_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(year, codigo_municipio, codigo_seccion)

# Para municipios: combinar secciones (post-1999) + datos pre-1999 (sin sección)
info_pre99 <-
  data_cer %>%
  filter(is.na(codigo_mesa)) %>%
  select(-c(siglas, denominacion, votos)) %>%
  distinct()

info_muni <-
  bind_rows(
    info_seccion %>% select(-c(codigo_distrito, codigo_seccion)),
    info_pre99 %>% select(-c(codigo_distrito, codigo_seccion, codigo_mesa))
  ) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(year, codigo_municipio)

info_prov <-
  info_muni %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(year)

info_ccaa <-
  info_prov %>%
  select(-c(codigo_provincia, codigo_circunscripcion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(year)

# ===========================================================================
# VOTOS — separar y agregar jerárquicamente
# ===========================================================================

votos_mesas <-
  data_cer %>%
  filter(!is.na(codigo_mesa)) %>%
  select(-c(censo_ine, abstenciones, votos_validos, votos_nulos, votos_blancos)) %>%
  arrange(year, codigo_municipio, codigo_seccion, codigo_distrito, codigo_mesa, -votos)

votos_seccion <-
  votos_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(year, codigo_municipio, codigo_seccion, -votos)

votos_pre99 <-
  data_cer %>%
  filter(is.na(codigo_mesa)) %>%
  select(-c(censo_ine, abstenciones, votos_validos, votos_nulos, votos_blancos))

votos_muni <-
  bind_rows(
    votos_seccion %>% select(-c(codigo_distrito, codigo_seccion)),
    votos_pre99 %>% select(-c(codigo_distrito, codigo_seccion, codigo_mesa))
  ) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(year, codigo_municipio, -votos)

votos_prov <-
  votos_muni %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(year, -votos)

votos_ccaa <-
  votos_prov %>%
  select(-c(codigo_provincia, codigo_circunscripcion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(year, -votos)

# ===========================================================================
# BIND + JOIN IDs
# ===========================================================================

info_cer <-
  bind_rows(info_ccaa, info_prov, info_muni, info_seccion) %>%
  mutate(
    across(c(codigo_provincia, codigo_circunscripcion, codigo_distrito), ~ ifelse(is.na(.), "99", .)),
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
  relocate(eleccion_id, territorio_id) %>%
  mutate(
    abstenciones = ifelse(abstenciones < 0, NA_integer_, abstenciones),
    abstenciones = ifelse(
      !is.na(votos_validos) & !is.na(abstenciones) & abstenciones + votos_validos > censo_ine,
      NA_integer_,
      abstenciones
    )
  )

votos_cer <-
  bind_rows(votos_ccaa, votos_prov, votos_muni, votos_seccion) %>%
  mutate(
    across(c(codigo_provincia, codigo_circunscripcion, codigo_distrito), ~ ifelse(is.na(.), "99", .)),
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

validate_info(info_cer, label = "15-navarra/info_cer")
validate_votos(votos_cer, label = "15-navarra/votos_cer")
validate_info_votos_consistency(info_cer, votos_cer, label = "15-navarra/cer")
validate_votos_partido_match(votos_cer, label = "15-navarra/votos_cer")

# ===========================================================================
# WRITE DATA
# ===========================================================================

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

saveRDS(info_cer, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos_cer, paste0(OUTPUT_DIR, "votos.rds"))

