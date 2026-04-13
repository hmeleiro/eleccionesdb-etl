library(dplyr)
library(readxl)
library(stringr)
library(purrr)
library(tidyr)
library(readr)

source("R/tests/validate_data_processed.R")

INPUT_DIR <- "data-raw/hechos/11-extremadura/"
OUTPUT_DIR <- "data-processed/hechos/11-extremadura/"

fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == "11") %>%
  transmute(eleccion_id = id, year = as.character(year))

territorios <-
  read_csv("tablas-finales/dimensiones/territorios", show_col_types = FALSE) %>%
  select(
    territorio_id = id,
    codigo_ccaa, codigo_provincia, codigo_municipio,
    codigo_distrito, codigo_seccion
  )

files <- sort(list.files(INPUT_DIR, full.names = TRUE, pattern = "\\.xlsx$"))

if (length(files) == 0) {
  stop("No se han encontrado ficheros Excel de Extremadura en data-raw/hechos/11-extremadura/")
}

# ==========================================================================
# LECTURA — dos formatos: mesas (2015+) y municipios (histórico)
# ==========================================================================

read_file_ext <- function(file) {
  is_muni <- str_detect(basename(file), "municipios")
  sh <- if (is_muni) "MUNICIPIOS" else "MESAS"
  tmp <- read_xlsx(file, sheet = sh)

  c_idx <- max(which(
    toupper(colnames(tmp)) %in%
      c(
        "VOTOS", "VOTOS.TOTALES", "BLANCOS", "NULOS", "CENSO", "CENSO.TOTAL",
        "ABSTENCIONES", "ABSTENCION", "VALIDOS", "VOTOS.CANDIDATURAS"
      )
  ))
  party_cols <- names(tmp)[(c_idx + 1):ncol(tmp)]

  tmp <- tmp %>%
    pivot_longer(any_of(party_cols), names_to = "siglas", values_to = "votos_partido") %>%
    janitor::clean_names()

  # Los ficheros de municipios tienen dos columnas cod_mun (concatenado y código).
  # Tras janitor: cod_mun (concat) y cod_mun_2 (código INE real).
  # Los ficheros de mesas solo tienen cod_mun.
  if (is_muni) {
    mun_col <- if ("cod_mun_2" %in% names(tmp)) "cod_mun_2" else "cod_mun"
    tmp <- tmp %>% rename(codigo_municipio = !!mun_col)
    # Los históricos no tienen distrito / sección / mesa
    tmp$codigo_distrito <- NA_character_
    tmp$codigo_seccion <- NA_character_
    tmp$codigo_mesa <- NA_character_
  } else {
    tmp <- tmp %>% rename(codigo_municipio = cod_mun)
  }

  # Renames comunes: helper que toma la primera alternativa que exista
  rename_first <- function(df, target, candidates) {
    found <- intersect(candidates, names(df))[1]
    if (!is.na(found) && found != target) rename(df, !!target := !!found) else df
  }
  tmp <- tmp %>%
    rename_first("year", "anyo") %>%
    rename_first("codigo_provincia", "cod_prov") %>%
    rename_first("codigo_distrito", "distrito") %>%
    rename_first("codigo_seccion", "seccion") %>%
    rename_first("codigo_mesa", "mesa") %>%
    rename_first("votos_blancos", "blancos") %>%
    rename_first("votos_nulos", "nulos") %>%
    rename_first("censo_raw", c("censo_total", "censo")) %>%
    rename_first("vtotales_raw", c("votos_totales", "votantes")) %>%
    rename_first("abstenciones_raw", c("abstenciones", "abstencion")) %>%
    rename_first("validos_raw", "validos")

  # Asegurar columnas opcionales ausentes
  for (col in c(
    "censo_raw", "vtotales_raw", "abstenciones_raw", "validos_raw",
    "votos_blancos", "votos_nulos"
  )) {
    if (!col %in% names(tmp)) tmp[[col]] <- NA_real_
  }

  tmp %>%
    mutate(
      year                   = as.character(year),
      codigo_ccaa            = "11",
      codigo_provincia       = str_pad(as.character(codigo_provincia), 2, "left", "0"),
      codigo_circunscripcion = codigo_provincia,
      codigo_municipio       = str_pad(as.character(codigo_municipio), 3, "left", "0"),
      codigo_distrito        = str_pad(as.character(codigo_distrito), 2, "left", "0"),
      codigo_seccion         = str_pad(as.character(codigo_seccion), 4, "left", "0"),
      codigo_mesa            = as.character(codigo_mesa),
      censo_ine              = as.numeric(censo_raw),
      votos_totales          = as.numeric(vtotales_raw),
      votos_blancos          = as.numeric(votos_blancos),
      votos_nulos            = as.numeric(votos_nulos),
      votos_validos          = coalesce(as.numeric(validos_raw), votos_totales - votos_nulos),
      abstenciones           = coalesce(as.numeric(abstenciones_raw), censo_ine - votos_totales),
      siglas                 = str_squish(as.character(siglas)),
      denominacion           = siglas,
      votos                  = as.numeric(votos_partido)
    ) %>%
    filter(!is.na(votos)) %>%
    select(
      year, codigo_ccaa, codigo_provincia, codigo_circunscripcion,
      codigo_municipio, codigo_distrito, codigo_seccion, codigo_mesa,
      censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos,
      siglas, denominacion, votos
    )
}

data <- map_df(files, read_file_ext)

info_raw <-
  data %>%
  filter(codigo_municipio != "999") %>%
  select(-c(siglas, denominacion, votos)) %>%
  distinct()

votos_raw <-
  data %>%
  filter(codigo_municipio != "999") %>%
  select(-c(censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos))

# ==========================================================================
# INFO — Agregación jerárquica
# ==========================================================================

# MESAS (solo 2015+; histórico no tiene nivel mesa)
info_mesas <-
  info_raw %>%
  filter(!is.na(codigo_mesa)) %>%
  arrange(
    year, codigo_circunscripcion, codigo_municipio,
    codigo_distrito, codigo_seccion, codigo_mesa
  )

# SECCIONES (solo 2015+)
info_seccion <-
  info_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year, codigo_municipio)

# MUNICIPIOS (2015+ desde secciones + histórico directo)
info_muni_hist <-
  info_raw %>%
  filter(is.na(codigo_mesa)) %>%
  select(-c(codigo_mesa, codigo_distrito, codigo_seccion)) %>%
  distinct()

info_muni <-
  info_seccion %>%
  select(-c(codigo_seccion, codigo_distrito)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  bind_rows(info_muni_hist) %>%
  arrange(year, codigo_municipio)

# PROVINCIA
info_prov <-
  info_muni %>%
  mutate(codigo_provincia = ifelse(codigo_circunscripcion == "99", "99", codigo_provincia)) %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year)

# CCAA
info_ccaa <-
  info_prov %>%
  select(-c(codigo_provincia, codigo_circunscripcion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year)

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
    abstenciones = ifelse(abstenciones < 0, NA_integer_, abstenciones),
    abstenciones = ifelse(
      !is.na(votos_validos) & !is.na(abstenciones) & abstenciones + votos_validos > censo_ine,
      NA_integer_,
      abstenciones
    )
  )

# ==========================================================================
# VOTOS — Agregación jerárquica
# ==========================================================================

# MESAS (solo 2015+)
votos_mesas <-
  votos_raw %>%
  filter(!is.na(codigo_mesa)) %>%
  arrange(
    year, codigo_circunscripcion, codigo_municipio,
    codigo_distrito, codigo_seccion, -votos
  )

# SECCIONES (solo 2015+)
votos_seccion <-
  votos_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year, codigo_municipio, -votos)

# MUNICIPIOS
votos_muni_hist <-
  votos_raw %>%
  filter(is.na(codigo_mesa)) %>%
  select(-c(codigo_mesa, codigo_distrito, codigo_seccion))

votos_muni <-
  votos_seccion %>%
  select(-c(codigo_seccion, codigo_distrito)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  bind_rows(votos_muni_hist) %>%
  arrange(year, codigo_municipio, -votos)

# PROVINCIA
votos_prov <-
  votos_muni %>%
  mutate(codigo_provincia = ifelse(codigo_circunscripcion == "99", "99", codigo_provincia)) %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year, -votos)

# CCAA
votos_ccaa <-
  votos_prov %>%
  select(-c(codigo_provincia, codigo_circunscripcion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year, -votos)

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

# ==========================================================================
# CHECKS
# ==========================================================================

validate_info(info_cer, label = "11-extremadura/info_cer")
validate_votos(votos_cer, label = "11-extremadura/votos_cer")
validate_info_votos_consistency(info_cer, votos_cer, label = "11-extremadura/cer")

# ==========================================================================
# WRITE DATA
# ==========================================================================

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

saveRDS(info_cer, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos_cer, paste0(OUTPUT_DIR, "votos.rds"))
