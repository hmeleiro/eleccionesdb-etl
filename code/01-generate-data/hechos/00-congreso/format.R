library(dplyr)
library(readr)
library(stringr)
library(purrr)
library(infoelectoral)

source("code/tests/validate_data_processed.R")

OUTPUT_DIR <- "data-processed/hechos/00-congreso/"

# --- Dimensiones ---
fechas <-
    read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE) %>%
    filter(tipo_eleccion == "G") %>%
    transmute(eleccion_id = id, year = as.character(year), mes)

territorios_raw <-
    read_csv("tablas-finales/dimensiones/territorios", show_col_types = FALSE)

territorios_sec <- territorios_raw %>%
    filter(tipo == "seccion") %>%
    select(
        territorio_id = id, codigo_provincia, codigo_municipio,
        codigo_distrito, codigo_seccion
    )

territorios_mun <- territorios_raw %>%
    filter(tipo == "municipio") %>%
    select(territorio_id = id, codigo_provincia, codigo_municipio)

territorios_prov <- territorios_raw %>%
    filter(tipo == "provincia") %>%
    select(territorio_id = id, codigo_provincia)

territorios_ccaa <- territorios_raw %>%
    filter(tipo == "ccaa") %>%
    select(territorio_id = id, codigo_ccaa)

codigos_ccaa <- infoelectoral::codigos_ccaa %>% select(-ccaa)

remap_ccaa <- function(df) {
    df %>%
        left_join(codigos_ccaa, by = "codigo_ccaa") %>%
        select(-codigo_ccaa) %>%
        rename(codigo_ccaa = codigo_ccaa_ine)
}


# ==========================================================================
# DATA: lectura desde infoelectoral
# ==========================================================================

# Mesa level (finest granularity)
data_mesas <-
    map_df(
        split(fechas, seq_len(nrow(fechas))),
        ~ mesas(tipo_eleccion = "congreso", anno = .x$year, mes = .x$mes)
    ) %>%
    mutate(
        codigo_seccion = str_pad(codigo_seccion, 4, "left", "0"),
        year = as.character(anno)
    ) %>%
    remap_ccaa()

# Provincia level (enrichment: participacion, nrepresentantes)
data_prov_raw <-
    map_df(
        split(fechas, seq_len(nrow(fechas))),
        ~ provincias(tipo_eleccion = "congreso", anno = .x$year, mes = .x$mes)
    ) %>%
    filter(codigo_provincia != "99") %>%
    mutate(year = as.character(anno))

# Separar CER de CERA
data_cer <- data_mesas %>% filter(municipio != "CERA")


# ==========================================================================
# FALLBACK: elecciones sin datos de mesas (e.g. 1977, 1979)
# ==========================================================================

elections_in_mesas <- data_cer %>% distinct(year, mes)
missing_elections <- fechas %>% anti_join(elections_in_mesas, by = c("year", "mes"))

if (nrow(missing_elections) > 0) {
    cat(
        "Fallback: leyendo municipios() para elecciones sin mesas:",
        paste(missing_elections$year, collapse = ", "), "\n"
    )

    data_muni_fb <- map_df(
        split(missing_elections, seq_len(nrow(missing_elections))),
        ~ municipios(
            tipo_eleccion = "congreso", anno = .x$year, mes = .x$mes,
            distritos = FALSE
        )
    ) %>%
        mutate(year = as.character(anno)) %>%
        remap_ccaa()

    info_muni_fb <- data_muni_fb %>%
        mutate(
            abstenciones = censo_ine - votos_candidaturas - votos_blancos - votos_nulos,
            votos_validos = votos_candidaturas + votos_blancos
        ) %>%
        select(
            year, mes, codigo_ccaa, codigo_provincia, codigo_municipio,
            censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos
        ) %>%
        distinct() %>%
        group_by(year, mes, codigo_ccaa, codigo_provincia, codigo_municipio) %>%
        summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")

    votos_muni_fb <- data_muni_fb %>%
        select(
            year, mes, codigo_ccaa, codigo_provincia, codigo_municipio,
            siglas, denominacion, votos
        ) %>%
        group_by(
            year, mes, codigo_ccaa, codigo_provincia, codigo_municipio,
            siglas, denominacion
        ) %>%
        summarise(votos = sum(votos, na.rm = TRUE), .groups = "drop")
} else {
    info_muni_fb <- tibble()
    votos_muni_fb <- tibble()
}


# ==========================================================================
# INFO — Agregación jerárquica
# ==========================================================================

# SECCIONES (from mesas)
info_seccion <-
    data_cer %>%
    mutate(
        abstenciones = censo_ine - votos_candidaturas - votos_blancos - votos_nulos,
        votos_validos = votos_candidaturas + votos_blancos
    ) %>%
    select(
        year, mes, codigo_ccaa, codigo_provincia, codigo_municipio,
        codigo_distrito, codigo_seccion,
        censo_ine, abstenciones, votos_validos, votos_blancos, votos_nulos
    ) %>%
    distinct() %>%
    group_by(
        year, mes, codigo_ccaa, codigo_provincia, codigo_municipio,
        codigo_distrito, codigo_seccion
    ) %>%
    summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")

# MUNICIPIOS
info_muni <-
    info_seccion %>%
    group_by(year, mes, codigo_ccaa, codigo_provincia, codigo_municipio) %>%
    summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop") %>%
    bind_rows(info_muni_fb)

# PROVINCIA (+ enrichment from provincias())
info_prov_enrichment <-
    data_prov_raw %>%
    select(year, mes, codigo_provincia,
        participacion_1, participacion_2,
        nrepresentantes = n_diputados
    ) %>%
    distinct()

info_prov <-
    info_muni %>%
    group_by(year, mes, codigo_ccaa, codigo_provincia) %>%
    summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop") %>%
    left_join(info_prov_enrichment, by = c("year", "mes", "codigo_provincia"))

# CCAA
info_ccaa <-
    info_prov %>%
    select(-c(participacion_1, participacion_2, nrepresentantes)) %>%
    group_by(year, mes, codigo_ccaa) %>%
    summarise(across(where(is.numeric), ~ sum(., na.rm = TRUE)), .groups = "drop")


# ==========================================================================
# VOTOS — Agregación jerárquica
# ==========================================================================

# SECCIONES
votos_seccion <-
    data_cer %>%
    select(
        year, mes, codigo_ccaa, codigo_provincia, codigo_municipio,
        codigo_distrito, codigo_seccion, siglas, denominacion, votos
    ) %>%
    group_by(
        year, mes, codigo_ccaa, codigo_provincia, codigo_municipio,
        codigo_distrito, codigo_seccion, siglas, denominacion
    ) %>%
    summarise(votos = sum(votos, na.rm = TRUE), .groups = "drop")

# MUNICIPIOS
votos_muni <-
    votos_seccion %>%
    group_by(
        year, mes, codigo_ccaa, codigo_provincia, codigo_municipio,
        siglas, denominacion
    ) %>%
    summarise(votos = sum(votos, na.rm = TRUE), .groups = "drop") %>%
    bind_rows(votos_muni_fb)

# PROVINCIA
votos_prov <-
    votos_muni %>%
    group_by(year, mes, codigo_ccaa, codigo_provincia, siglas, denominacion) %>%
    summarise(votos = sum(votos, na.rm = TRUE), .groups = "drop")

# CCAA
votos_ccaa <-
    votos_prov %>%
    group_by(year, mes, codigo_ccaa, siglas, denominacion) %>%
    summarise(votos = sum(votos, na.rm = TRUE), .groups = "drop")


# ==========================================================================
# JOIN + COMBINE levels
# ==========================================================================

join_level <- function(df, terr, terr_keys) {
    df %>%
        left_join(fechas, by = c("year", "mes")) %>%
        left_join(terr, by = terr_keys) %>%
        select(-c(year, mes, starts_with("codigo_"))) %>%
        relocate(eleccion_id, territorio_id)
}

info <- bind_rows(
    join_level(info_ccaa, territorios_ccaa, "codigo_ccaa"),
    join_level(info_prov, territorios_prov, "codigo_provincia"),
    join_level(info_muni, territorios_mun, c("codigo_provincia", "codigo_municipio")),
    join_level(
        info_seccion, territorios_sec,
        c("codigo_provincia", "codigo_municipio", "codigo_distrito", "codigo_seccion")
    )
) %>%
    arrange(eleccion_id, territorio_id) %>%
    filter(!is.na(territorio_id)) %>%
    mutate(abstenciones = ifelse(abstenciones < 0, NA_integer_, abstenciones))

votos <- bind_rows(
    join_level(votos_ccaa, territorios_ccaa, "codigo_ccaa"),
    join_level(votos_prov, territorios_prov, "codigo_provincia"),
    join_level(votos_muni, territorios_mun, c("codigo_provincia", "codigo_municipio")),
    join_level(
        votos_seccion, territorios_sec,
        c("codigo_provincia", "codigo_municipio", "codigo_distrito", "codigo_seccion")
    )
) %>%
    arrange(eleccion_id, territorio_id) %>%
    filter(!is.na(territorio_id))


# ==========================================================================
# CHECKS
# ==========================================================================

validate_info(info, label = "00-congreso/info")
validate_votos(votos, label = "00-congreso/votos")
validate_info_votos_consistency(info, votos, label = "00-congreso")


# ==========================================================================
# WRITE DATA
# ==========================================================================

dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

saveRDS(info, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos, paste0(OUTPUT_DIR, "votos.rds"))
