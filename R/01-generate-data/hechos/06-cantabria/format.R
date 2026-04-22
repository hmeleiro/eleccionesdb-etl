library(dplyr)
library(readxl)
library(purrr)
library(tidyr)
library(stringr)
library(readr)

source("R/tests/validate_data_processed.R")

INPUT_DIR <- "data-raw/hechos/06-cantabria/"
OUTPUT_DIR <- "data-processed/hechos/06-cantabria/"

fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == "06") %>%
  transmute(eleccion_id = id, year = as.character(year))

territorios <-
  read_csv("tablas-finales/dimensiones/territorios", show_col_types = FALSE) %>%
  select(
    territorio_id = id,
    codigo_ccaa, codigo_provincia, codigo_municipio,
    codigo_distrito, codigo_seccion
  )

files <- list.files(INPUT_DIR, full.names = TRUE, recursive = TRUE, pattern = "\\.xlsx$")
files <- sort(files[str_detect(basename(files), "Cantabria[0-9]{4}")])

if (length(files) == 0) {
  stop("No se han encontrado ficheros Excel de Cantabria en data-raw/hechos/06-cantabria/")
}

data <-
  map_df(files, function(file) {
    tmp <- read_xlsx(file, sheet = "MESAS")

    c <- max(which(str_detect(tolower(colnames(tmp)), "votos|nulos|blancos")))
    colnames(tmp)[1:c] <- janitor::make_clean_names(colnames(tmp)[1:c])
    party_cols <- names(tmp)[(c + 1):ncol(tmp)]

    for (col in setdiff(c("censo", "censo_total", "votantes", "votos", "validos", "blancos", "nulos"), names(tmp))) {
      tmp[[col]] <- NA
    }

    tmp %>%
      rename(
        year = any_of("anyo"),
        codigo_provincia = any_of("cod_prov"),
        codigo_municipio = any_of("cod_mun"),
        codigo_distrito = any_of("distrito"),
        codigo_seccion = any_of("seccion"),
        codigo_mesa = any_of("mesa"),
        votos_blancos = any_of("blancos"),
        votos_nulos = any_of("nulos")
      ) %>%
      pivot_longer(any_of(party_cols), names_to = "siglas", values_to = "votos_partido") %>%
      mutate(
        year = as.character(year),
        codigo_ccaa = "06",
        codigo_provincia = str_pad(as.character(codigo_provincia), 2, "left", "0"),
        codigo_circunscripcion = codigo_provincia,
        codigo_municipio = str_pad(as.character(codigo_municipio), 3, "left", "0"),
        codigo_distrito = str_pad(as.character(codigo_distrito), 2, "left", "0"),
        codigo_seccion = str_pad(as.character(codigo_seccion), 4, "left", "0"),
        codigo_mesa = as.character(codigo_mesa),
        censo_ine = coalesce(as.numeric(censo_total), as.numeric(censo)),
        votos_totales = coalesce(as.numeric(votos), as.numeric(votantes)),
        votos_blancos = as.numeric(votos_blancos),
        votos_nulos = as.numeric(votos_nulos),
        votos_validos = coalesce(as.numeric(validos), votos_totales - votos_nulos),
        abstenciones = censo_ine - votos_totales,
        siglas = str_squish(as.character(siglas)),
        denominacion = siglas,
        votos = as.numeric(votos_partido)
      ) %>%
      select(
        year, codigo_ccaa, codigo_provincia, codigo_circunscripcion,
        codigo_municipio, codigo_distrito, codigo_seccion, codigo_mesa,
        censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos,
        siglas, denominacion, votos
      )
  })

info_raw <-
  data %>%
  filter(codigo_municipio != "999") %>%
  select(-c(siglas, denominacion, votos)) %>%
  distinct()

votos_raw <-
  data %>%
  filter(codigo_municipio != "999") %>%
  select(-c(censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos))

# ===========================================================================
# INFO — Agregación jerárquica
# ===========================================================================

info_mesas <-
  info_raw %>%
  arrange(
    year, codigo_circunscripcion, codigo_municipio,
    codigo_distrito, codigo_seccion, codigo_mesa
  )

info_seccion <-
  info_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year, codigo_municipio)

info_muni <-
  info_seccion %>%
  select(-c(codigo_seccion, codigo_distrito)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year, codigo_municipio)

info_prov <-
  info_muni %>%
  mutate(codigo_provincia = ifelse(codigo_circunscripcion == "99", "99", codigo_provincia)) %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year)

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
    across(c(codigo_provincia, codigo_circunscripcion, codigo_distrito), ~ ifelse(is.na(.), "99", .)),
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

# ===========================================================================
# VOTOS — Agregación jerárquica
# ===========================================================================

votos_mesa <-
  votos_raw %>%
  arrange(
    year, codigo_circunscripcion, codigo_municipio,
    codigo_distrito, codigo_seccion, -votos
  )

votos_seccion <-
  votos_mesa %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year, codigo_municipio, -votos)

votos_muni <-
  votos_seccion %>%
  select(-c(codigo_seccion, codigo_distrito)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year, codigo_municipio, -votos)

votos_prov <-
  votos_muni %>%
  mutate(codigo_provincia = ifelse(codigo_circunscripcion == "99", "99", codigo_provincia)) %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE))) %>%
  ungroup() %>%
  arrange(year, -votos)

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
    across(c(codigo_provincia, codigo_circunscripcion, codigo_distrito), ~ ifelse(is.na(.), "99", .)),
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

# ===========================================================================
# CHECKS
# ===========================================================================

validate_info(info_cer, label = "06-cantabria/info_cer")
validate_votos(votos_cer, label = "06-cantabria/votos_cer")
validate_info_votos_consistency(info_cer, votos_cer, label = "06-cantabria/cer")
validate_votos_partido_match(votos_cer, label = "06-cantabria/votos_cer")

# ===========================================================================
# WRITE DATA
# ===========================================================================

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

saveRDS(info_cer, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos_cer, paste0(OUTPUT_DIR, "votos.rds"))
