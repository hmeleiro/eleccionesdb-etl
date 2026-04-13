library(dplyr)
library(tidyr)
library(stringr)
library(purrr)
library(readxl)
library(readr)

source("R/tests/validate_data_processed.R")

INPUT_DIR <- "data-raw/hechos/13-comunidad-madrid/"
OUTPUT_DIR <- "data-processed/hechos/13-comunidad-madrid/"

# --- Dimensiones ---
# Madrid tiene 2 elecciones en 2003 (mayo + octubre) → join necesita year + mes
fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = F) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == "13") %>%
  transmute(
    eleccion_id = id, year = as.character(year),
    mes = sprintf("%02d", as.numeric(mes))
  )

territorios <-
  read_csv("tablas-finales/dimensiones/territorios", show_col_types = F) %>%
  select(
    territorio_id = id, codigo_ccaa, codigo_provincia,
    codigo_municipio, codigo_distrito, codigo_seccion
  )

# --- Función auxiliar para leer ficheros Excel ---
read <- function(fichero, clean_names = FALSE, ...) {
  year <- str_extract(fichero, "[0-9]{4}")
  year <- ifelse(str_detect(fichero, "octubre"), paste0(year, "_2"), year)

  out <-
    suppressMessages(read_excel(fichero)) %>%
    mutate(year, .before = 1) %>%
    rename(...)

  if (clean_names) {
    out <- janitor::clean_names(out)
  }

  cols <- colnames(out)
  if (!"VALI" %in% cols & "VOTO" %in% cols) {
    out <- out %>%
      mutate(VALI = VOTO - NULO)
  }
  out <- out %>%
    select(!any_of("censo_total"))
  return(out)
}

# ==========================================================================
# LECTURA Y PARSEO POR AÑO
# ==========================================================================

# 2023
ficheros <- list.files(INPUT_DIR, pattern = "2023", full.names = T)
df <- read_xlsx(ficheros)
i <- which(colnames(df) == "Votos_total")
colnames(df)[1:i] <- janitor::make_clean_names(colnames(df)[1:i])

data23 <-
  df %>%
  pivot_longer(any_of(i + 1):ncol(.), names_to = "partido", values_to = "votos") %>%
  transmute(
    year = "2023",
    mes = "05",
    codigo_ccaa = "13",
    codigo_provincia = "28",
    codigo_circunscripcion = "28",
    codigo_municipio = str_pad(cod_muni, 3, "left", "0"),
    codigo_distrito = str_pad(distrito, 2, "left", "0"),
    codigo_seccion = str_squish(seccion),
    codigo_seccion = str_pad(seccion, 4, "left", "0"),
    codigo_mesa = mesa,
    censo_ine = censo,
    votos_validos = votos_electores - votos_nulos,
    votos_nulos,
    votos_blancos,
    abstenciones = censo_ine - votos_electores,
    siglas = partido,
    denominacion = partido,
    votos
  )


# 95-2003
ficheros <- list.files(INPUT_DIR, pattern = "Mesas", full.names = T)
ficheros <- ficheros[1:4]

df <- map_df(ficheros, read)

data95_03 <-
  df %>%
  select(-VOTO) %>%
  relocate(year, .before = MUNI) %>%
  relocate(VALI, .before = MUNI) %>%
  mutate(
    mes = case_when(
      year == "1999" ~ "06",
      year == "2003_2" ~ "10",
      TRUE ~ "05"
    ),
    year = substr(year, 1, 4)
  ) %>%
  relocate(mes, .before = MUNI) %>%
  pivot_longer(cols = c(PP:ncol(.)), names_to = "partido", values_to = "votos") %>%
  select(-CERT) %>%
  filter(!is.na(votos)) %>%
  janitor::clean_names() %>%
  rename(
    "codigo_municipio" = muni,
    "codigo_distrito" = dist,
    "codigo_seccion" = secc,
    "codigo_mesa" = mesa,
    "censo_ine" = cens,
    "votos_validos" = vali,
    "votos_blancos" = blan,
    "votos_nulos" = nulo
  ) %>%
  mutate(
    codigo_municipio = str_pad(codigo_municipio, width = 3, side = "left", pad = "0"),
    codigo_distrito = str_pad(codigo_distrito, width = 2, side = "left", pad = "0"),
    codigo_seccion = str_squish(codigo_seccion),
    codigo_seccion = str_pad(codigo_seccion, width = 4, side = "left", pad = "0"),
    codigo_ccaa = "13",
    codigo_provincia = "28",
    codigo_circunscripcion = "28",
    abstenciones = censo_ine - (votos_validos + votos_nulos),
    siglas = partido,
    denominacion = partido
  ) %>%
  select(-inte, -partido)

# 2007
df <- suppressMessages(read_excel(paste0(INPUT_DIR, "2007_Mesas.xls")))

data07 <-
  df %>%
  select(-`...33`) %>%
  pivot_longer(cols = c("IU-CM":ncol(.)), names_to = "partido", values_to = "votos") %>%
  janitor::clean_names() %>%
  mutate(year = "2007", mes = "05") %>%
  group_by(municipio, distrito, seccion, mesa) %>%
  mutate(
    votos_validos = sum(votos, na.rm = T) + blancos
  ) %>%
  ungroup() %>%
  rename(
    "codigo_municipio" = municipio,
    "codigo_distrito" = distrito,
    "codigo_seccion" = seccion,
    "codigo_mesa" = mesa,
    "censo_ine" = electores,
    "votos_blancos" = blancos,
    "votos_nulos" = nulos
  ) %>%
  mutate(
    codigo_municipio = str_pad(codigo_municipio, width = 3, side = "left", pad = "0"),
    codigo_distrito = str_pad(codigo_distrito, width = 2, side = "left", pad = "0"),
    codigo_seccion = str_squish(codigo_seccion),
    codigo_seccion = str_pad(codigo_seccion, width = 4, side = "left", pad = "0"),
    codigo_ccaa = "13",
    codigo_provincia = "28",
    codigo_circunscripcion = "28",
    abstenciones = censo_ine - (votos_validos + votos_nulos),
    siglas = partido,
    denominacion = partido
  ) %>%
  select(-certif, -cert_corr, -votantes, -num_electores, -interv, -partido)

# 2011 EN ADELANTE
ficheros <- list.files(INPUT_DIR, pattern = "Mesas", full.names = T)
ficheros <- ficheros[6:9]

rename_cols <- c(
  "votos_electores" = "Votos Elect.",
  "votos_interventores" = "Votos Interv.",
  "votos_blancos" = "Votos Blancos",
  "votos_blancos" = "Votos Blanco",
  "votos_blancos" = "Votos_blanco"
)

df <- map_df(ficheros, read, clean_names = T, any_of(rename_cols))

remove_cols <- c(
  "censo_total", "votos_electores", "votos_interventores",
  "certif_alta", "certif_correc", "votos_totales", "municipio"
)

data11_22 <-
  df %>%
  select(!any_of(remove_cols)) %>%
  relocate(year, .after = codcir) %>%
  pivot_longer(cols = c("iu_lv":ncol(.)), names_to = "partido", values_to = "votos") %>%
  filter(!is.na(votos)) %>%
  separate(mesa, into = c("codigo_distrito", "codigo_seccion", "codigo_mesa"), sep = "-") %>%
  rename(
    "codigo_municipio" = codmun,
    "codigo_circunscripcion" = codcir,
    "censo_ine" = censo
  ) %>%
  mutate(
    codigo_municipio = str_pad(codigo_municipio, width = 3, side = "left", pad = "0"),
    codigo_distrito = str_pad(codigo_distrito, width = 2, side = "left", pad = "0"),
    codigo_seccion = str_squish(codigo_seccion),
    codigo_seccion = str_pad(codigo_seccion, width = 4, side = "left", pad = "0")
  ) %>%
  group_by(year, codigo_municipio, codigo_distrito, codigo_seccion, codigo_mesa) %>%
  mutate(
    votos_validos = sum(votos, na.rm = T) + votos_blancos,
    codigo_ccaa = "13",
    codigo_provincia = "28",
    codigo_circunscripcion = "28",
    abstenciones = censo_ine - (votos_validos + votos_nulos),
    siglas = partido,
    denominacion = partido
  ) %>%
  ungroup() %>%
  mutate(mes = "05") %>%
  select(-partido)


# ==========================================================================
# COMBINAR TODOS LOS AÑOS (NIVEL MESA)
# ==========================================================================

data <- bind_rows(data95_03, data07, data11_22, data23)

data_cera <- data %>%
  filter(codigo_distrito == "99")

data <- data %>%
  filter(!is.na(codigo_municipio)) %>%
  filter(!codigo_municipio %in% c("999", "990"))

# ==========================================================================
# DATOS ANTERIORES A 1995 (SOLO PROVINCIA)
# ==========================================================================

ficheros <- list.files(INPUT_DIR, pattern = "circunscripci", full.names = T)

df_pre95 <-
  map_df(ficheros, function(fichero) {
    year <- str_extract(fichero, "[0-9]{4}")

    suppressMessages(read_excel(fichero, sheet = "PROVINCIAS (OFICIALES)")) %>%
      janitor::clean_names() %>%
      select(-anyo) %>%
      mutate(year, mes = ifelse(year == "1987", "06", "05"))
  })

remove_cols <- c("votantes", "voto_candidaturas", "provincia")

df_pre95 <-
  df_pre95 %>%
  relocate(year, .before = cod_prov) %>%
  relocate(mes, .after = year) %>%
  select(!any_of(remove_cols)) %>%
  pivot_longer(cols = c("adei":ncol(.)), names_to = "partido", values_to = "votos") %>%
  filter(!is.na(votos)) %>%
  rename(
    "codigo_provincia" = cod_prov,
    "censo_ine" = censo,
    "votos_nulos" = nulos,
    "votos_blancos" = blancos,
    "votos_validos" = validos
  ) %>%
  mutate(
    codigo_ccaa = "13",
    codigo_provincia = "28",
    codigo_circunscripcion = "28",
    abstenciones = censo_ine - votos_validos - votos_nulos,
    siglas = partido,
    denominacion = partido
  ) %>%
  select(-partido)

info_prov_pre95 <-
  df_pre95 %>%
  select(-c(siglas, denominacion, votos)) %>%
  distinct()

votos_prov_pre95 <-
  df_pre95 %>%
  select(-c(censo_ine, votos_nulos, votos_blancos, votos_validos, abstenciones)) %>%
  distinct()

# ==========================================================================
# INFO — Agregación jerárquica
# ==========================================================================

# MESAS
info_mesas <- data %>%
  select(-c(siglas, denominacion, votos)) %>%
  distinct()

# SECCIONES
info_seccion <-
  info_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# MUNICIPIOS
info_muni <-
  info_seccion %>%
  select(-c(codigo_distrito, codigo_seccion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# PROVINCIA (+ datos pre-1995)
info_prov <-
  info_muni %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup() %>%
  bind_rows(info_prov_pre95)

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
  left_join(fechas, by = c("year", "mes")) %>%
  left_join(
    territorios,
    by = c(
      "codigo_ccaa", "codigo_provincia", "codigo_municipio",
      "codigo_distrito", "codigo_seccion"
    )
  ) %>%
  arrange(eleccion_id, territorio_id) %>%
  select(-c(year, mes, starts_with("codigo_"))) %>%
  relocate(eleccion_id, territorio_id) %>%
  mutate(
    abstenciones = ifelse(abstenciones < 0, NA_integer_, abstenciones)
  )

# ==========================================================================
# VOTOS — Agregación jerárquica
# ==========================================================================

# MESAS
votos_mesas <- data %>%
  select(-c(votos_nulos, votos_blancos, votos_validos, abstenciones, censo_ine))

# SECCIONES
votos_seccion <-
  votos_mesas %>%
  select(-codigo_mesa) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# MUNICIPIOS
votos_muni <-
  votos_seccion %>%
  select(-c(codigo_distrito, codigo_seccion)) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup()

# PROVINCIA (+ datos pre-1995)
votos_prov <-
  votos_muni %>%
  select(-codigo_municipio) %>%
  group_by(across(where(is.character))) %>%
  summarise(across(where(is.numeric), ~ sum(., na.rm = T))) %>%
  ungroup() %>%
  bind_rows(votos_prov_pre95)

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
  left_join(fechas, by = c("year", "mes")) %>%
  left_join(
    territorios,
    by = c(
      "codigo_ccaa", "codigo_provincia", "codigo_municipio",
      "codigo_distrito", "codigo_seccion"
    )
  ) %>%
  arrange(eleccion_id, territorio_id) %>%
  select(-c(year, mes, starts_with("codigo_"))) %>%
  relocate(eleccion_id, territorio_id)


# ==========================================================================
# CHECKS
# ==========================================================================

validate_info(info_cer, label = "13-comunidad-madrid/info_cer")
validate_votos(votos_cer, label = "13-comunidad-madrid/votos_cer")
validate_info_votos_consistency(info_cer, votos_cer, label = "13-comunidad-madrid/cer")
validate_votos_partido_match(votos_cer, label = "13-comunidad-madrid/votos_cer")

# ==========================================================================
# WRITE DATA
# ==========================================================================

dir.create(OUTPUT_DIR, showWarnings = FALSE)

saveRDS(info_cer, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos_cer, paste0(OUTPUT_DIR, "votos.rds"))
