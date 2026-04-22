library(readxl)
library(dplyr)
library(stringr)
library(tidyr)
library(purrr)
library(readr)

source("R/tests/validate_data_processed.R")

INPUT_DIR <- "data-raw/hechos/16-pais-vasco/"
OUTPUT_DIR <- "data-processed/hechos/16-pais-vasco/"

fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == "16") %>%
  transmute(eleccion_id = id, year = as.character(year))

territorios <-
  read_csv("tablas-finales/dimensiones/territorios", show_col_types = FALSE) %>%
  select(
    territorio_id = id,
    codigo_ccaa, codigo_provincia, codigo_municipio,
    codigo_distrito, codigo_seccion
  )

# ===========================================================================
# Lectura de datos brutos
# ===========================================================================

files <- list.files(INPUT_DIR, full.names = TRUE)

data <- map_df(files, function(file) {
  rename_cols <- c(
    "codigo_municipio" = "Cod.Municipio",
    "codigo_municipio" = "Cod. Municipio",
    "codigo_distrito" = "Distrito",
    "codigo_seccion" = "Sección",
    "codigo_seccion" = "Seccion",
    "codigo_mesa" = "Mesa",
    "censo_ine" = "Censo",
    "votos_validos" = "Válidos",
    "votos_validos" = "Validos",
    "votos_blancos" = "Blancos",
    "votos_candidatura" = "Votos Cand.",
    "votos_candidatura" = "Votos Candidatura",
    "votos_nulos" = "Nulos",
    "abstenciones" = "Abstenciones",
    "abstenciones" = "Abstención",
    "votantes" = "Votantes"
  )

  year <- str_remove_all(file, ".+MesP|_.+")
  year <- ifelse(year > 78, paste0("19", year), paste0("20", year))

  sk <- suppressMessages(read_excel(file))
  sk <- max(which(is.na(pull(sk[, 2])))) + 1

  sheets <- suppressMessages(excel_sheets(file))
  tmp <-
    map_df(sheets, function(sh) {
      read_excel(file, skip = sk, sheet = sh) %>%
        mutate(year, provincia = sh, .before = 1)
    })

  c <- max(which(tolower(colnames(tmp)) %in% c("votos candidatura", "abstenciones", "abstención", "blancos", "nulos")))

  tmp %>%
    rename(any_of(rename_cols)) %>%
    pivot_longer(any_of(c + 1):ncol(.), names_to = "partido", values_to = "votos") %>%
    mutate(across(c(year, starts_with("codigo_")), as.character))
})

data <-
  data %>%
  mutate(
    codigo_ccaa = "16",
    codigo_provincia = case_when(
      provincia %in% c("Araba-Alava", "ARABA-ÁLAVA") ~ "01",
      provincia %in% c("Gipuzkoa", "GIPUZKOA") ~ "20",
      provincia %in% c("Bizkaia", "BIZKAIA") ~ "48"
    ),
    codigo_circunscripcion = codigo_provincia,
    codigo_municipio = str_pad(codigo_municipio, 3, pad = "0"),
    codigo_seccion = str_pad(codigo_seccion, 4, pad = "0"),
    codigo_distrito = str_pad(codigo_distrito, 2, pad = "0"),
    siglas = as.character(partido),
    denominacion = as.character(partido)
  ) %>%
  select(
    year, codigo_ccaa, codigo_provincia, codigo_circunscripcion,
    codigo_municipio, codigo_distrito, codigo_seccion, codigo_mesa,
    censo_ine, votos_blancos, votos_nulos, votos_validos, abstenciones,
    siglas, denominacion, votos
  )

# ===========================================================================
# Separar CER / CERA
# ===========================================================================

data_cer <- data %>% filter(codigo_municipio != "999")

# ===========================================================================
# INFO — nivel mesa + agregación jerárquica
# ===========================================================================

info_mesas <- data_cer %>%
  select(-c(siglas, denominacion, votos)) %>%
  distinct()

info_seccion <-
  info_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(year, codigo_municipio, codigo_distrito, codigo_seccion)

info_muni <-
  info_seccion %>%
  select(-c(codigo_seccion, codigo_distrito)) %>%
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
# VOTOS — nivel mesa + agregación jerárquica
# ===========================================================================

votos_mesas <- data_cer %>%
  select(-c(censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos))

votos_seccion <-
  votos_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(year, codigo_municipio, codigo_distrito, codigo_seccion, -votos)

votos_muni <-
  votos_seccion %>%
  select(-c(codigo_seccion, codigo_distrito)) %>%
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

validate_info(info_cer, label = "16-pais-vasco/info_cer")
validate_votos(votos_cer, label = "16-pais-vasco/votos_cer")
validate_info_votos_consistency(info_cer, votos_cer, label = "16-pais-vasco/cer")
validate_votos_partido_match(votos_cer, label = "16-pais-vasco/votos_cer")

# ===========================================================================
# WRITE DATA
# ===========================================================================

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

saveRDS(info_cer, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos_cer, paste0(OUTPUT_DIR, "votos.rds"))

