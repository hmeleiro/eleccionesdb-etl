library(dplyr)
library(readxl)
library(tidyr)
library(stringr)
library(readr)

source("R/tests/validate_data_processed.R")

INPUT_DIR <- "data-raw/hechos/17-la-rioja/"
OUTPUT_DIR <- "data-processed/hechos/17-la-rioja/"

fechas <-
    read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE) %>%
    filter(tipo_eleccion == "A" & codigo_ccaa == "17") %>%
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

info_raw <- read_xls(file.path(INPUT_DIR, "resumen_mesa.xls")) %>%
    janitor::clean_names()

votos_raw <- read_xls(file.path(INPUT_DIR, "detalle_votos_mesa.xls")) %>%
    janitor::clean_names()

avances_raw <- read_xls(file.path(INPUT_DIR, "avances_mesa.xls")) %>%
    janitor::clean_names()

# ===========================================================================
# Avances de participación (pivot a columnas participacion_1, participacion_2)
# ===========================================================================

avances <-
    avances_raw %>%
    filter(as.numeric(codigo_municipio) < 200) %>%
    filter(!(distrito == "99" & seccion == "999")) %>%
    transmute(
        year = anio_electoral,
        codigo_municipio = str_pad(codigo_municipio, 3, "left", "0"),
        codigo_distrito = str_pad(distrito, 2, "left", "0"),
        codigo_seccion = str_pad(seccion, 4, "left", "0"),
        mesa,
        numero_avance = paste0("participacion_", numero_avance),
        votos_emitidos = as.integer(votos_emitidos)
    ) %>%
    pivot_wider(
        names_from  = numero_avance,
        values_from = votos_emitidos
    )

# ===========================================================================
# INFO — nivel mesa
# ===========================================================================

info_mesa <-
    info_raw %>%
    filter(as.numeric(codigo_municipio) < 200) %>%
    filter(!(distrito == "99" & seccion == "999")) %>%
    transmute(
        year = anio_electoral,
        codigo_ccaa = "17",
        codigo_provincia = "26",
        codigo_municipio = str_pad(codigo_municipio, 3, "left", "0"),
        codigo_distrito = str_pad(distrito, 2, "left", "0"),
        codigo_seccion = str_pad(seccion, 4, "left", "0"),
        codigo_mesa = mesa,
        censo_ine = as.integer(censo),
        abstenciones = as.integer(censo - votos_emitidos),
        votos_validos = as.integer(votos_validos),
        votos_blancos = as.integer(votos_en_blanco),
        votos_nulos = as.integer(votos_nulos)
    ) %>%
    left_join(
        avances,
        by = c("year", "codigo_municipio", "codigo_distrito",
            "codigo_seccion",
            "codigo_mesa" = "mesa"
        )
    )

# ===========================================================================
# VOTOS — nivel mesa
# ===========================================================================

votos_mesa <-
    votos_raw %>%
    filter(as.numeric(codigo_municipio) < 200) %>%
    filter(!(distrito == "99" & seccion == "999")) %>%
    transmute(
        year = anio_electoral,
        codigo_ccaa = "17",
        codigo_provincia = "26",
        codigo_municipio = str_pad(codigo_municipio, 3, "left", "0"),
        codigo_distrito = str_pad(distrito, 2, "left", "0"),
        codigo_seccion = str_pad(seccion, 4, "left", "0"),
        codigo_mesa = mesa,
        siglas = siglas_partido,
        denominacion = denominacion_partido,
        votos = as.integer(votos)
    )

# ===========================================================================
# INFO — Agregación jerárquica
# ===========================================================================

info_seccion <-
    info_mesa %>%
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
    select(-codigo_provincia) %>%
    group_by(across(where(is.character))) %>%
    summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
    arrange(year)

# ===========================================================================
# VOTOS — Agregación jerárquica
# ===========================================================================

votos_seccion <-
    votos_mesa %>%
    select(-codigo_mesa) %>%
    group_by(across(where(is.character))) %>%
    summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop") %>%
    arrange(year, codigo_municipio, -votos)

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

validate_info(info_cer, label = "17-la-rioja/info_cer")
validate_votos(votos_cer, label = "17-la-rioja/votos_cer")
validate_info_votos_consistency(info_cer, votos_cer, label = "17-la-rioja/cer")

# ===========================================================================
# WRITE DATA
# ===========================================================================

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

saveRDS(info_cer, paste0(OUTPUT_DIR, "info.rds"))
saveRDS(votos_cer, paste0(OUTPUT_DIR, "votos.rds"))
