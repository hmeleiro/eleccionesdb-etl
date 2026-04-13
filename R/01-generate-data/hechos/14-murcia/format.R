library(dplyr)
library(readxl)
library(purrr)
library(tidyr)
library(stringr)
library(readr)

source("R/tests/validate_data_processed.R")

INPUT_DIR <- "data-raw/hechos/14-murcia/"
OUTPUT_DIR <- "data-processed/hechos/14-murcia/"

fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == "14") %>%
  transmute(eleccion_id = id, year = as.character(year))

territorios <-
  read_csv("tablas-finales/dimensiones/territorios", show_col_types = FALSE) %>%
  select(
    territorio_id = id,
    codigo_ccaa, codigo_provincia, codigo_municipio,
    codigo_distrito, codigo_seccion
  )

# ===========================================================================
# Función de lectura para los xls (post-2003 y pre-2003)
# ===========================================================================

leer_murcia <- function(x) {
  pr <- read_xls(x)
  fecha <- as.Date(pr[3, 1] %>% pull(), format = "%d de %B de %Y")
  names <- paste0(pr[4, ], "_", pr[5, ])
  temp <- pr[6:nrow(pr), ]
  colnames(temp) <- names
  temp <- temp %>%
    select(-c(ends_with("_Porcentajes"))) %>%
    mutate(fecha_elec = fecha)
  return(temp)
}

# Helper: limpia columnas y calcula otros_partidos
preparar_datos <- function(data, parties_idx) {
  otros <-
    data %>%
    pivot_longer(any_of(parties_idx):ncol(.), names_to = "partido", values_to = "votos") %>%
    filter(!is.na(votos)) %>%
    group_by(across(-c(partido, votos))) %>%
    mutate(
      votos = as.integer(votos),
      votos_total_previo = sum(votos, na.rm = TRUE)
    ) %>%
    ungroup() %>%
    mutate(otros = as.integer(votos_a_candidaturas) - votos_total_previo) %>%
    select(-c(partido, votos, votos_total_previo)) %>%
    mutate(partido = "otros_partidos") %>%
    rename(votos = otros) %>%
    distinct()

  result <-
    data %>%
    pivot_longer(any_of(parties_idx):ncol(.), names_to = "partido", values_to = "votos") %>%
    filter(!is.na(votos)) %>%
    mutate(votos = as.integer(votos)) %>%
    bind_rows(otros)

  return(result)
}

# ===========================================================================
# POST-2003: datos a nivel mesa (2007-2023)
# ===========================================================================

files_mesas <- list.files(file.path(INPUT_DIR, "mesas_post2003"),
  pattern = "\\.xls$", full.names = TRUE
)

data_raw_post <- map_df(files_mesas, leer_murcia)

clean_mesas <-
  data_raw_post %>%
  rename(info = 1) %>%
  relocate(fecha_elec) %>%
  janitor::clean_names() %>%
  filter(!is.na(censo_electores)) %>%
  filter(str_detect(info, "^[0-9]") & !str_detect(info, "C.E.R.A"))

colnames(clean_mesas) <- gsub("_electores", "", colnames(clean_mesas))

parties_idx <- which(colnames(clean_mesas) == "votos_a_candidaturas") + 1

data_post03 <-
  preparar_datos(clean_mesas, parties_idx) %>%
  separate(
    info,
    into = c("codigo_municipio", "codigo_distrito", "codigo_seccion", "codigo_mesa"),
    sep = "-"
  ) %>%
  separate(fecha_elec, into = c("year", "mes", "day"), sep = "-") %>%
  transmute(
    year,
    codigo_ccaa = "14",
    codigo_provincia = "30",
    codigo_municipio = str_pad(codigo_municipio, 3, "left", "0"),
    codigo_distrito = str_pad(codigo_distrito, 2, "left", "0"),
    codigo_seccion = str_pad(codigo_seccion, 4, "left", "0"),
    codigo_mesa = as.character(codigo_mesa),
    censo_ine = as.integer(censo),
    abstenciones = as.integer(abstencion),
    votos_validos = as.integer(votos_validos),
    votos_blancos = as.integer(votos_blancos),
    votos_nulos = as.integer(votos_nulos),
    siglas = as.character(partido),
    denominacion = siglas,
    votos = as.integer(votos)
  )

# ===========================================================================
# DATOS MESA 2003 (fichero con columnas explícitas)
# ===========================================================================

mesas03 <- suppressMessages(read_excel(file.path(INPUT_DIR, "Murcia2003_mesas.xlsx"), sheet = "MESAS"))

parties03_idx <- which(colnames(mesas03) == "VOTOS.CANDIDATURAS") + 1

data_2003 <-
  mesas03 %>%
  pivot_longer(any_of(parties03_idx):ncol(.), names_to = "partido", values_to = "votos") %>%
  janitor::clean_names() %>%
  transmute(
    year = as.character(anyo),
    codigo_ccaa = "14",
    codigo_provincia = str_pad(cod_prov, 2, "left", "0"),
    codigo_municipio = str_pad(cod_mun, 3, "left", "0"),
    codigo_distrito = str_pad(distrito, 2, "left", "0"),
    codigo_seccion = str_pad(seccion, 4, "left", "0"),
    codigo_mesa = as.character(mesa),
    censo_ine = as.integer(censo),
    abstenciones = as.integer(censo - nulos - votos_candidaturas - blancos),
    votos_validos = as.integer(votos_candidaturas + blancos),
    votos_blancos = as.integer(blancos),
    votos_nulos = as.integer(nulos),
    siglas = as.character(partido),
    denominacion = siglas,
    votos = as.integer(votos)
  )

# Combinar post-2003 y 2003
data_mesas_all <- bind_rows(data_post03, data_2003)

# ===========================================================================
# PRE-2003: datos a nivel municipio (1983-1999)
# ===========================================================================

files_pre <- list.files(file.path(INPUT_DIR, "municipios_pre2003"),
  pattern = "\\.xls$", full.names = TRUE
)

data_raw_pre <- map_df(files_pre, leer_murcia)

clean_muni_pre <-
  data_raw_pre %>%
  rename(info = 1) %>%
  relocate(fecha_elec) %>%
  janitor::clean_names() %>%
  filter(!is.na(censo_electores)) %>%
  filter(
    !str_detect(info, "C\\.E\\.R\\.A") &
      !str_detect(info, "CIRCUNSCRIPCI") &
      !str_detect(info, "^Regi")
  )

colnames(clean_muni_pre) <- gsub("_electores", "", colnames(clean_muni_pre))

parties_pre_idx <- which(colnames(clean_muni_pre) == "votos_a_candidaturas") + 1

# Mapeo nombre municipio → código municipio
aux_muni <- readRDS(file.path(INPUT_DIR, "aux_codigos_municipios.RDS")) %>%
  filter(codigo_provincia == "30" & municipio != "CERA") %>%
  distinct(codigo_municipio, municipio)

data_pre03 <-
  preparar_datos(clean_muni_pre, parties_pre_idx) %>%
  separate(fecha_elec, into = c("year", "mes", "day"), sep = "-") %>%
  # Limpieza de nombres para join con aux
  mutate(
    municipio = info %>%
      str_replace(" \\(Los\\)$", ", Los") %>%
      str_replace(" \\(Las\\)$", ", Las") %>%
      str_replace(" \\(La\\)$", ", La") %>%
      str_replace(" \\(El\\)$", ", El") %>%
      str_replace("^Fuente Álamo$", "Fuente Álamo de Murcia")
  ) %>%
  left_join(aux_muni, by = "municipio") %>%
  transmute(
    year,
    codigo_ccaa = "14",
    codigo_provincia = "30",
    codigo_municipio = str_pad(codigo_municipio, 3, "left", "0"),
    censo_ine = as.integer(censo),
    abstenciones = as.integer(abstencion),
    votos_validos = as.integer(votos_validos),
    votos_blancos = as.integer(votos_blancos),
    votos_nulos = as.integer(votos_nulos),
    siglas = as.character(partido),
    denominacion = siglas,
    votos = as.integer(votos)
  )

# ===========================================================================
# Separar info / votos
# ===========================================================================

info_mesas <- data_mesas_all %>%
  select(-c(siglas, denominacion, votos)) %>%
  distinct()

votos_mesas <- data_mesas_all %>%
  select(-c(censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos))

info_pre03 <- data_pre03 %>%
  select(-c(siglas, denominacion, votos)) %>%
  distinct()

votos_pre03 <- data_pre03 %>%
  select(-c(censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos))

# ===========================================================================
# INFO — Agregación jerárquica
# ===========================================================================

info_seccion <-
  info_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(year, codigo_municipio, codigo_distrito, codigo_seccion)

info_muni <-
  bind_rows(
    info_seccion %>% select(-c(codigo_seccion, codigo_distrito)),
    info_pre03
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
  select(-codigo_provincia) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(year)

# ===========================================================================
# VOTOS — Agregación jerárquica
# ===========================================================================

votos_seccion <-
  votos_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
  arrange(year, codigo_municipio, -votos)

votos_muni <-
  bind_rows(
    votos_seccion %>% select(-c(codigo_seccion, codigo_distrito)),
    votos_pre03
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
  select(-codigo_provincia) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
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

validate_info(info_cer, label = "14-murcia/info_cer")
validate_votos(votos_cer, label = "14-murcia/votos_cer")
validate_info_votos_consistency(info_cer, votos_cer, label = "14-murcia/cer")

# ===========================================================================
# WRITE DATA
# ===========================================================================

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

saveRDS(info_cer, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos_cer, paste0(OUTPUT_DIR, "votos.rds"))
