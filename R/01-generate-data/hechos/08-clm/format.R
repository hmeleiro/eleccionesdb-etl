# ===========================================================================
# 08-clm/format.R
#
# Genera las tablas de hechos de las elecciones autonómicas de
# Castilla-La Mancha.
#
# PARTICULARIDADES:
# - Los datos anteriores a 2019 solo proporcionan datos a nivel de MUNICIPIO.
#   NO incluyen votos en blanco ni votos nulos, por lo que votos_validos se
#   calcula únicamente como la suma de votos a candidatura (infraestimación
#   del valor real, ya que no incluye los votos en blanco). votos_blancos y
#   votos_nulos son NA para estas elecciones.
# - Cuando el dato de abstenciones ("ABSTEN") no está disponible en los CSV
#   originales, se calcula como censo_ine - votos_validos.
# - Para 2019 y 2023 sí hay datos a nivel de MESA electoral.
# - Para 2023, los datos no incluyen código de municipio sino solo el nombre,
#   por lo que se hace un join con la tabla de códigos del paquete infoelectoral.
#
# Elecciones cubiertas: 1983, 1987, 1991, 1995, 1999, 2003, 2007, 2011, 2015,
#                        2019, 2023.
# ===========================================================================

library(readr)
library(dplyr)
library(purrr)
library(stringr)
library(tidyr)

source("R/tests/validate_data_processed.R")

options(readr.show_col_types = FALSE)

INPUT_DIR  <- "data-raw/hechos/08-clm/"
OUTPUT_DIR <- "data-processed/hechos/08-clm/"

CODIGO_CCAA <- "08"

# Suma que preserva NA cuando TODOS los valores son NA
sum_na <- function(x) if (all(is.na(x))) NA_real_ else sum(x, na.rm = TRUE)

# ===========================================================================
# DIMENSIONES
# ===========================================================================

fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == CODIGO_CCAA) %>%
  transmute(eleccion_id = id, year = as.character(year))

territorios <-
  read_csv("tablas-finales/dimensiones/territorios", show_col_types = FALSE) %>%
  select(
    territorio_id = id,
    codigo_ccaa, codigo_provincia, codigo_municipio,
    codigo_distrito, codigo_seccion
  )

# ===========================================================================
# LECTURA DE FICHEROS
# ===========================================================================

files <- list.files(INPUT_DIR, full.names = TRUE)

files_pre2015 <- files[!grepl("2019|202[0-9]", files)]
file_2019     <- files[grepl("2019", files) & grepl("\\.xlsx$", files)]
file_2023     <- files[grepl("2023", files) & grepl("\\.xlsx$", files)]

# ---------------------------------------------------------------------------
# Pre-2015: CSV, nivel municipio, SIN votos en blanco NI votos nulos.
# Los datos incluyen CENSO y ABSTEN como entradas pseudo-partido.
# ---------------------------------------------------------------------------

read_pre2015 <- function(file) {
  read_delim(
    file, delim = ";",
    locale = locale(encoding = "ISO-8859-1", decimal_mark = ","),
    col_types = cols(`Código INE` = "c")
  ) %>%
    janitor::clean_names() %>%
    rename(any_of(c("year" = "ano"))) %>%
    mutate(
      year = as.character(year),
      # Pad codigo_ine a 9 dígitos: la provincia de Albacete (02) se almacena
      # sin cero inicial (ej: 20010000 en vez de 020010000).
      codigo_ine = str_pad(as.character(codigo_ine), 9, "left", "0"),
      codigo_provincia = substr(codigo_ine, 1, 2),
      codigo_municipio = substr(codigo_ine, 3, 5),
      denominacion = partido,
      siglas = partido,
      votos = as.numeric(votos)
    ) %>%
    select(year, codigo_provincia, codigo_municipio, siglas, denominacion, votos)
}

data_pre2015 <- map_df(files_pre2015, read_pre2015)

# --- Info pre-2015: extraer CENSO y ABSTEN ---
info_pre2015_raw <-
  data_pre2015 %>%
  filter(siglas %in% c("CENSO", "ABSTEN")) %>%
  select(-denominacion) %>%
  pivot_wider(names_from = siglas, values_from = votos)

# ABSTEN no está disponible en todos los años
if (!"ABSTEN" %in% names(info_pre2015_raw)) {
  info_pre2015_raw$ABSTEN <- NA_real_
}

info_pre2015_raw <- info_pre2015_raw %>%
  rename(censo_ine = CENSO, abstenciones = ABSTEN)

# votos_validos = suma de votos a candidatura (sin blancos ni nulos, que no
# están disponibles). Es una infraestimación del valor real.
votos_sum_pre2015 <-
  data_pre2015 %>%
  filter(!siglas %in% c("CENSO", "ABSTEN")) %>%
  group_by(year, codigo_provincia, codigo_municipio) %>%
  summarise(votos_validos = sum(votos, na.rm = TRUE), .groups = "drop")

info_pre2015 <-
  info_pre2015_raw %>%
  left_join(votos_sum_pre2015, by = c("year", "codigo_provincia", "codigo_municipio")) %>%
  mutate(
    codigo_ccaa = CODIGO_CCAA,
    codigo_distrito = "99",
    codigo_seccion = "9999",
    votos_blancos = NA_real_,
    votos_nulos = NA_real_,
    # Si ABSTEN no está disponible, calcular como censo - votos_validos
    abstenciones = ifelse(is.na(abstenciones), censo_ine - votos_validos, abstenciones)
  )

# --- Votos pre-2015: excluir CENSO y ABSTEN ---
votos_pre2015 <-
  data_pre2015 %>%
  filter(!siglas %in% c("CENSO", "ABSTEN")) %>%
  mutate(
    codigo_ccaa = CODIGO_CCAA,
    codigo_distrito = "99",
    codigo_seccion = "9999"
  )

# ---------------------------------------------------------------------------
# 2019: XLSX, nivel mesa, CON votos en blanco y nulos.
# El codigo_de_mesa tiene formato "prov-muni-distrito-seccion-mesa".
# ---------------------------------------------------------------------------

raw_2019 <-
  readxl::read_xlsx(file_2019) %>%
  janitor::clean_names() %>%
  rename(any_of(c(
    "codigo_mesa"   = "codigo_de_mesa",
    "censo_ine"     = "censo_total",
    "votos_blancos" = "votos_blanco",
    "votos_totales" = "total_votantes"
  )))

party_start_2019 <- which(names(raw_2019) == "psoe")
party_cols_2019  <- names(raw_2019)[party_start_2019:ncol(raw_2019)]

data_2019 <-
  raw_2019 %>%
  pivot_longer(all_of(party_cols_2019), names_to = "siglas", values_to = "votos") %>%
  mutate(
    year = "2019",
    codigo_ccaa = CODIGO_CCAA,
    codigo_provincia = str_pad(str_split(codigo_mesa, "-", simplify = TRUE)[, 1], 2, "left", "0"),
    codigo_municipio = str_pad(str_split(codigo_mesa, "-", simplify = TRUE)[, 2], 3, "left", "0"),
    codigo_distrito  = str_pad(str_split(codigo_mesa, "-", simplify = TRUE)[, 3], 2, "left", "0"),
    codigo_seccion   = str_pad(str_split(codigo_mesa, "-", simplify = TRUE)[, 4], 4, "left", "0"),
    censo_ine     = as.numeric(censo_ine),
    votos_blancos = as.numeric(votos_blancos),
    votos_nulos   = as.numeric(votos_nulos),
    votos_totales = as.numeric(votos_totales),
    votos_validos = votos_totales - votos_nulos,
    abstenciones  = censo_ine - votos_totales,
    denominacion  = siglas,
    votos = as.numeric(votos)
  ) %>%
  filter(
    !is.na(votos),
    codigo_municipio != "999" # CERA
  )

info_2019 <-
  data_2019 %>%
  select(
    year, codigo_ccaa, codigo_provincia, codigo_municipio,
    codigo_distrito, codigo_seccion, codigo_mesa,
    censo_ine, votos_blancos, votos_nulos, votos_validos, abstenciones
  ) %>%
  distinct()

votos_2019 <-
  data_2019 %>%
  select(
    year, codigo_ccaa, codigo_provincia, codigo_municipio,
    codigo_distrito, codigo_seccion, codigo_mesa,
    siglas, denominacion, votos
  )

# ---------------------------------------------------------------------------
# 2023: XLSX, nivel mesa, CON votos en blanco y nulos.
# No se proporciona código de municipio sino solo el nombre, por lo que se
# une con la tabla de códigos del paquete infoelectoral.
# El campo "mesa" tiene formato "distrito-seccion-mesa".
# Cada hoja del XLSX corresponde a una provincia con partidos distintos.
# ---------------------------------------------------------------------------

codigos_municipio <-
  infoelectoral::codigos_municipios %>%
  filter(codigo_provincia %in% c("02", "13", "16", "19", "45")) %>%
  mutate(
    municipio = paste0(
      str_split(municipio, ",", n = 2, simplify = TRUE)[, 2], " ",
      str_split(municipio, ",", n = 2, simplify = TRUE)[, 1]
    ),
    municipio = str_squish(municipio)
  )

sheets_2023 <- readxl::excel_sheets(file_2023)

raw_2023 <-
  map(sheets_2023, function(sheet) {
    readxl::read_xlsx(file_2023, sheet = sheet, col_types = "text") %>%
      janitor::clean_names()
  }) %>%
  list_rbind() %>%
  rename(any_of(c(
    "codigo_mesa"   = "mesa",
    "censo_ine"     = "censo_total",
    "votos_blancos" = "blanco",
    "votos_nulos"   = "nulos"
  )))

party_start_2023 <- which(names(raw_2023) == "psoe")
party_cols_2023  <- names(raw_2023)[party_start_2023:ncol(raw_2023)]

data_2023 <-
  raw_2023 %>%
  pivot_longer(all_of(party_cols_2023), names_to = "siglas", values_to = "votos") %>%
  mutate(
    year = "2023",
    across(any_of(c("censo", "censo_ine", "votos_totales", "votos_blancos",
                    "votos_nulos", "votos")), as.numeric),
    codigo_ccaa      = CODIGO_CCAA,
    codigo_distrito  = str_pad(str_split(codigo_mesa, "-", simplify = TRUE)[, 1], 2, "left", "0"),
    codigo_seccion   = str_pad(str_split(codigo_mesa, "-", simplify = TRUE)[, 2], 4, "left", "0"),
    votos_validos    = votos_totales - votos_nulos,
    abstenciones     = censo_ine - votos_totales,
    denominacion     = siglas
  ) %>%
  filter(!is.na(votos)) %>%
  left_join(codigos_municipio, by = join_by(municipio))

info_2023 <-
  data_2023 %>%
  select(
    year, codigo_ccaa, codigo_provincia, codigo_municipio,
    codigo_distrito, codigo_seccion, codigo_mesa,
    censo_ine, votos_blancos, votos_nulos, votos_validos, abstenciones
  ) %>%
  distinct()

votos_2023 <-
  data_2023 %>%
  select(
    year, codigo_ccaa, codigo_provincia, codigo_municipio,
    codigo_distrito, codigo_seccion, codigo_mesa,
    siglas, denominacion, votos
  )

# ===========================================================================
# INFO — Agregación jerárquica
# ===========================================================================

# 2019/2023: mesa → sección (eliminar codigo_mesa)
info_seccion <-
  bind_rows(info_2019, info_2023) %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum_na(.x)), .groups = "drop")

# 2019/2023: sección → municipio
info_muni_recent <-
  info_seccion %>%
  select(-c(codigo_seccion, codigo_distrito)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum_na(.x)), .groups = "drop")

# Pre-2015: ya está a nivel municipio, eliminar distrito/seccion placeholder
info_muni_pre2015 <-
  info_pre2015 %>%
  select(-c(codigo_distrito, codigo_seccion))

# Combinar todos los municipios
info_muni <- bind_rows(info_muni_pre2015, info_muni_recent)

# municipio → provincia
info_prov <-
  info_muni %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum_na(.x)), .groups = "drop")

# provincia → CCAA
info_ccaa <-
  info_prov %>%
  select(-codigo_provincia) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum_na(.x)), .groups = "drop")

# Combinar todos los niveles territoriales
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

# 2019/2023: mesa → sección
votos_seccion <-
  bind_rows(votos_2019, votos_2023) %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum_na(.x)), .groups = "drop")

# 2019/2023: sección → municipio
votos_muni_recent <-
  votos_seccion %>%
  select(-c(codigo_seccion, codigo_distrito)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum_na(.x)), .groups = "drop")

# Pre-2015: ya está a nivel municipio
votos_muni_pre2015 <-
  votos_pre2015 %>%
  select(-c(codigo_distrito, codigo_seccion))

# Combinar todos los municipios
votos_muni <- bind_rows(votos_muni_pre2015, votos_muni_recent)

# municipio → provincia
votos_prov <-
  votos_muni %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum_na(.x)), .groups = "drop")

# provincia → CCAA
votos_ccaa <-
  votos_prov %>%
  select(-codigo_provincia) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum_na(.x)), .groups = "drop")

# Combinar todos los niveles territoriales
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

# Para los municipios de Ledaña (Cuenca) y Bogarra (Albacete) no se proporciona
# información de resultados de candidaturas en las elecciones de 1987, por lo que
# se excluyen los registros.
info_cer <-
  info_cer %>% filter(
  !(eleccion_id == "38" & territorio_id == "4316"),
  !(eleccion_id == "38" & territorio_id == "4594")
  )

# ===========================================================================
# CHECKS
# ===========================================================================

validate_info(info_cer, label = "08-clm/info_cer")
validate_votos(votos_cer, label = "08-clm/votos_cer")
validate_info_votos_consistency(info_cer, votos_cer, label = "08-clm/cer")
validate_votos_partido_match(votos_cer, label = "08-clm/votos_cer")

# ===========================================================================
# WRITE DATA
# ===========================================================================

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

saveRDS(info_cer, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos_cer, paste0(OUTPUT_DIR, "votos.rds"))

