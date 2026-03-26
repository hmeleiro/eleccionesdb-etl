library(dplyr)
library(readr)
library(stringr)
library(tidyr)
library(purrr)

source("code/tests/validate_data_processed.R")

INPUT_DIR <- "data-raw/hechos/04-baleares/"
OUTPUT_DIR <- "data-processed/hechos/04-baleares/"

# --- Dimensiones ---
fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == "04") %>%
  transmute(eleccion_id = id, year = as.character(year))

territorios <-
  read_csv("tablas-finales/dimensiones/territorios", show_col_types = FALSE) %>%
  select(
    territorio_id = id, codigo_ccaa, codigo_provincia,
    codigo_municipio, codigo_distrito, codigo_seccion,
    codigo_circunscripcion
  )

correspondencia <-
  read_csv(
    "data-raw/codigos_territorios/correspondencia_municipio_circunscripcion.csv",
    col_types = cols(.default = "c")
  ) %>%
  filter(codigo_provincia == "07")


# ==========================================================================
# INFO — Lectura de datos básicos (.px)
# ==========================================================================

files_info <- list.files(INPUT_DIR,
  recursive = TRUE, full.names = TRUE,
  pattern = "basicos"
)

info_raw <- map_df(files_info, function(file) {
  rename_cols <- c(
    "variable"          = "datos",
    "area"              = "municipio_mesa_y_voto_cera",
    "area"              = "municipio_y_mesa",
    "votos_validos"     = "voto_valido",
    "votos_candidatura" = "voto_a_candidatura",
    "votos_blancos"     = "voto_en_blanco",
    "votos_nulos"       = "voto_nulo",
    "censo_ine"         = "censo_electoral",
    "abstenciones"      = "abstencion"
  )

  year <- str_extract(file, "[0-9]{4}")

  pxR::read.px(file)$DATA$value %>%
    janitor::clean_names() %>%
    rename(any_of(rename_cols)) %>%
    separate(area,
      into = c("codigo", "nombre", "nombre2"), sep = " ",
      extra = "drop", fill = "right"
    ) %>%
    mutate(year = year) %>%
    filter(nchar(codigo) > 5) %>%
    pivot_wider(names_from = "variable", values_from = "value") %>%
    janitor::clean_names() %>%
    select(!starts_with("percent")) %>%
    rename(any_of(rename_cols)) %>%
    mutate(
      codigo_ccaa = "04",
      codigo_provincia = substr(codigo, 1, 2),
      codigo_municipio = substr(codigo, 3, 5),
      codigo_distrito = substr(codigo, 6, 7),
      # Pre-1995: 12-char code (sec 4-dig pos 8-11, mesa pos 12)
      # 1995+:    10-char code (sec 3-dig pos 8-10, mesa = separate space token)
      codigo_seccion = case_when(
        nchar(codigo) == 12 ~ substr(codigo, 8, 11),
        TRUE ~ str_pad(substr(codigo, 8, 10), 4, "left", "0")
      ),
      codigo_mesa = case_when(
        nchar(codigo) == 12 ~ substr(codigo, 12, 12),
        TRUE ~ nombre
      )
    )
}) %>%
  left_join(correspondencia, by = c("codigo_provincia", "codigo_municipio")) %>%
  transmute(
    year,
    codigo_ccaa, codigo_provincia, codigo_municipio,
    codigo_distrito, codigo_seccion, codigo_mesa,
    codigo_circunscripcion,
    censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos
  )


# ==========================================================================
# VOTOS — Lectura de datos de votos (.px por isla)
# ==========================================================================

files_votos <- list.files(INPUT_DIR,
  recursive = TRUE, full.names = TRUE,
  pattern = "\\.px$"
)
files_votos <- files_votos[!str_detect(files_votos, "basicos")]

votos_raw <- map_df(files_votos, function(file) {
  rename_cols <- c(
    "variable" = "datos",
    "area"     = "municipio_mesa_y_voto_cera",
    "area"     = "municipio_y_mesa",
    "partido"  = "siglas_y_nombre_de_candidatura",
    "votos"    = "num_votos"
  )

  year <- str_extract(file, "[0-9]{4}")

  pxR::read.px(file)$DATA$value %>%
    janitor::clean_names() %>%
    rename(any_of(rename_cols)) %>%
    filter(variable != "% votos") %>%
    separate(area,
      into = c("codigo", "nombre", "nombre2"), sep = " ",
      extra = "drop", fill = "right"
    ) %>%
    mutate(year = year) %>%
    filter(nchar(codigo) > 5) %>%
    pivot_wider(names_from = "variable", values_from = "value") %>%
    janitor::clean_names() %>%
    select(!starts_with("percent")) %>%
    rename(any_of(rename_cols)) %>%
    filter(partido != "TOTAL CANDIDATURAS") %>%
    separate(partido, into = c("siglas", "denominacion"), sep = " - ") %>%
    mutate(
      codigo_ccaa = "04",
      codigo_provincia = substr(codigo, 1, 2),
      codigo_municipio = substr(codigo, 3, 5),
      codigo_distrito = substr(codigo, 6, 7),
      codigo_seccion = case_when(
        nchar(codigo) == 12 ~ substr(codigo, 8, 11),
        TRUE ~ str_pad(substr(codigo, 8, 10), 4, "left", "0")
      ),
      codigo_mesa = case_when(
        nchar(codigo) == 12 ~ substr(codigo, 12, 12),
        TRUE ~ nombre
      )
    )
}) %>%
  left_join(correspondencia, by = c("codigo_provincia", "codigo_municipio")) %>%
  transmute(
    year,
    codigo_ccaa, codigo_provincia, codigo_municipio,
    codigo_distrito, codigo_seccion, codigo_mesa,
    codigo_circunscripcion,
    siglas, denominacion, votos
  )


# ==========================================================================
# INFO — Agregación jerárquica
# ==========================================================================

# MESAS
info_mesas <- info_raw

# SECCIONES
info_seccion <-
  info_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")

# MUNICIPIOS
info_muni <-
  info_seccion %>%
  select(-c(codigo_distrito, codigo_seccion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")

# CIRCUNSCRIPCION (isla)
info_circ <-
  info_muni %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")

# PROVINCIA
info_prov <-
  info_circ %>%
  select(-codigo_circunscripcion) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")

# CCAA
info_ccaa <-
  info_prov %>%
  select(-codigo_provincia) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")

# COMBINAR NIVELES
info <-
  bind_rows(info_ccaa, info_prov, info_circ, info_muni, info_seccion) %>%
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
      "codigo_distrito", "codigo_seccion", "codigo_circunscripcion"
    )
  ) %>%
  arrange(eleccion_id, territorio_id) %>%
  filter(!is.na(territorio_id)) %>%
  select(-c(year, starts_with("codigo_"))) %>%
  relocate(eleccion_id, territorio_id) %>%
  mutate(abstenciones = ifelse(abstenciones < 0, NA_integer_, abstenciones))


# ==========================================================================
# VOTOS — Agregación jerárquica
# ==========================================================================

# MESAS
votos_mesas <- votos_raw

# SECCIONES
votos_seccion <-
  votos_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")

# MUNICIPIOS
votos_muni <-
  votos_seccion %>%
  select(-c(codigo_distrito, codigo_seccion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")

# CIRCUNSCRIPCION (isla)
votos_circ <-
  votos_muni %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")

# PROVINCIA
votos_prov <-
  votos_circ %>%
  select(-codigo_circunscripcion) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")

# CCAA
votos_ccaa <-
  votos_prov %>%
  select(-codigo_provincia) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")

# COMBINAR NIVELES
votos <-
  bind_rows(votos_ccaa, votos_prov, votos_circ, votos_muni, votos_seccion) %>%
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
      "codigo_distrito", "codigo_seccion", "codigo_circunscripcion"
    )
  ) %>%
  arrange(eleccion_id, territorio_id) %>%
  filter(!is.na(territorio_id)) %>%
  select(-c(year, starts_with("codigo_"))) %>%
  relocate(eleccion_id, territorio_id)


# ==========================================================================
# CHECKS
# ==========================================================================

validate_info(info, label = "04-baleares/info")
validate_votos(votos, label = "04-baleares/votos")
validate_info_votos_consistency(info, votos, label = "04-baleares")


# ==========================================================================
# WRITE DATA
# ==========================================================================

dir.create(OUTPUT_DIR, showWarnings = FALSE)

saveRDS(info, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos, paste0(OUTPUT_DIR, "votos.rds"))
