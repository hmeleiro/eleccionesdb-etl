# generate_diagnosticos.R
#
# Detecta automáticamente incidencias de calidad de datos en tablas-finales/hechos/info.rds
# y las combina con las incidencias manuales persistentes (incidencias_manual.csv).
#
# Produce docs-site/data/diagnosticos/incidencias.csv con el esquema:
#
#   tipo, tipo_label, tipo_descripcion,
#   eleccion_id, eleccion, anio, tipo_eleccion,
#   n_casos, nivel_territorial,
#   peor_caso_territorio, peor_caso_valor, peor_caso_esperado,
#   fuente, resoluble, origen, comentario
#
# Una fila por (tipo × eleccion_id): agrupado, no desagregado a nivel de territorio.
#
# ----- Uso directo -----
#   source("R/tests/generate_diagnosticos.R")
#   generate_diagnosticos()
#
# ----- Uso en pipeline -----
#   Llamado desde R/pipeline_helpers.R -> run_gen_diagnosticos()

library(dplyr)
library(readr)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

MANUAL_CSV  <- "docs-site/data/diagnosticos/incidencias_manual.csv"
OUTPUT_CSV  <- "docs-site/data/diagnosticos/incidencias.csv"
INFO_PATH   <- "tablas-finales/hechos/info.rds"
ELEC_PATH   <- "tablas-finales/dimensiones/elecciones"
TERR_PATH   <- "tablas-finales/dimensiones/territorios"

# Etiquetas y descripciones de cada tipo de incidencia automática
TIPO_META <- tribble(
  ~tipo,                        ~tipo_label,                               ~tipo_descripcion,
  "votos_superan_censo",        "Votos > censo",                           "La suma de votos válidos y abstenciones supera el censo INE. Puede indicar un error en el censo publicado, en los totales de participación, o en la fuente original.",
  "nulos_superan_validos",      "Votos nulos > votos válidos",             "El número de votos nulos supera al total de votos válidos, lo cual es aritméticamente posible, pero probablemente indica un error en la fuente original o en el proceso de integración.",
  "blancos_superan_validos",    "Votos en blanco > votos válidos",         "El número de votos en blanco supera al total de votos válidos. Aritméticamente imposible; indica un error en el dato de origen o en la integración.",
  "nas_censo_ine",              "NAs en censo_ine",                        "Territorios con dato de censo ausente (NA). El censo es necesario para calcular participación y para validaciones de consistencia.",
  "nas_votos_validos",          "NAs en votos_validos",                    "Territorios con dato de votos válidos ausente (NA). Puede deberse a una cobertura parcial de la fuente para esa elección y nivel territorial."
)

# ---------------------------------------------------------------------------
# Función principal de detección
# ---------------------------------------------------------------------------

#' Detecta incidencias automáticas y devuelve un data.frame con el esquema
#' estándar de incidencias (origen = "automatico").
#'
#' @param info_path  Ruta al archivo info.rds.
#' @param elec_path  Ruta a la dimensión elecciones (CSV sin extensión).
#' @param terr_path  Ruta a la dimensión territorios (CSV sin extensión).
#' @return data.frame con todas las incidencias detectadas.
detect_diagnosticos <- function(
    info_path = INFO_PATH,
    elec_path = ELEC_PATH,
    terr_path = TERR_PATH
) {
  message("[diagnosticos] Leyendo datos...")
  info  <- readRDS(info_path)
  elec  <- read_csv(elec_path,  show_col_types = FALSE)
  terr  <- read_csv(terr_path,  show_col_types = FALSE)

  # Enriquecer info con metadatos de elección y territorio para identificar peor caso
  info_enr <- info %>%
    left_join(elec %>% select(id, tipo_eleccion, year, descripcion, slug),
              by = c("eleccion_id" = "id")) %>%
    left_join(terr %>% select(id, tipo, nombre),
              by = c("territorio_id" = "id"))

  results <- list()

  # ---- 1. votos_superan_censo -----------------------------------------------
  message("[diagnosticos] Comprobando votos > censo...")
  check1 <- info_enr %>%
    filter(!is.na(censo_ine), !is.na(abstenciones), !is.na(votos_validos)) %>%
    mutate(exceso = abstenciones + votos_validos - censo_ine) %>%
    filter(exceso > 0)

  if (nrow(check1) > 0) {
    peor1 <- check1 %>%
      group_by(eleccion_id) %>%
      slice_max(exceso, n = 1, with_ties = FALSE) %>%
      ungroup() %>%
      select(eleccion_id, peor_caso_territorio = nombre,
             peor_caso_valor = exceso, peor_nivel = tipo) %>%
      mutate(peor_caso_valor = as.character(peor_caso_valor))

    results[["votos_superan_censo"]] <- check1 %>%
      group_by(eleccion_id, tipo_eleccion, year, descripcion) %>%
      summarise(
        n_casos = n(),
        nivel_territorial = paste(sort(unique(tipo)), collapse = " / "),
        .groups = "drop"
      ) %>%
      left_join(peor1, by = "eleccion_id") %>%
      mutate(
        tipo               = "votos_superan_censo",
        eleccion           = descripcion,
        anio               = as.integer(year),
        peor_caso_esperado = NA_character_,
        fuente             = "fuente_original",
        resoluble          = FALSE,
        origen             = "automatico",
        comentario         = NA_character_
      )
  }

  # ---- 2. nulos_superan_validos ---------------------------------------------
  message("[diagnosticos] Comprobando nulos > válidos...")
  check2 <- info_enr %>%
    filter(!is.na(votos_nulos), !is.na(votos_validos)) %>%
    filter(votos_nulos > votos_validos)

  if (nrow(check2) > 0) {
    peor2 <- check2 %>%
      group_by(eleccion_id) %>%
      slice_max(votos_nulos - votos_validos, n = 1, with_ties = FALSE) %>%
      ungroup() %>%
      select(eleccion_id, peor_caso_territorio = nombre,
             peor_caso_valor = votos_nulos, peor_caso_ref = votos_validos,
             peor_nivel = tipo)

    results[["nulos_superan_validos"]] <- check2 %>%
      group_by(eleccion_id, tipo_eleccion, year, descripcion) %>%
      summarise(
        n_casos = n(),
        nivel_territorial = paste(sort(unique(tipo)), collapse = " / "),
        .groups = "drop"
      ) %>%
      left_join(peor2, by = "eleccion_id") %>%
      mutate(
        tipo               = "nulos_superan_validos",
        eleccion           = descripcion,
        anio               = as.integer(year),
        peor_caso_esperado = as.character(peor_caso_ref),
        peor_caso_valor    = as.character(peor_caso_valor),
        fuente             = "fuente_original",
        resoluble          = FALSE,
        origen             = "automatico",
        comentario         = NA_character_
      ) %>%
      select(-peor_caso_ref)
  }

  # ---- 3. blancos_superan_validos -------------------------------------------
  message("[diagnosticos] Comprobando blancos > válidos...")
  check3 <- info_enr %>%
    filter(!is.na(votos_blancos), !is.na(votos_validos)) %>%
    filter(votos_blancos > votos_validos)

  if (nrow(check3) > 0) {
    peor3 <- check3 %>%
      group_by(eleccion_id) %>%
      slice_max(votos_blancos - votos_validos, n = 1, with_ties = FALSE) %>%
      ungroup() %>%
      select(eleccion_id, peor_caso_territorio = nombre,
             peor_caso_valor = votos_blancos, peor_caso_ref = votos_validos,
             peor_nivel = tipo)

    results[["blancos_superan_validos"]] <- check3 %>%
      group_by(eleccion_id, tipo_eleccion, year, descripcion) %>%
      summarise(
        n_casos = n(),
        nivel_territorial = paste(sort(unique(tipo)), collapse = " / "),
        .groups = "drop"
      ) %>%
      left_join(peor3, by = "eleccion_id") %>%
      mutate(
        tipo               = "blancos_superan_validos",
        eleccion           = descripcion,
        anio               = as.integer(year),
        peor_caso_esperado = as.character(peor_caso_ref),
        peor_caso_valor    = as.character(peor_caso_valor),
        fuente             = "fuente_original",
        resoluble          = FALSE,
        origen             = "automatico",
        comentario         = NA_character_
      ) %>%
      select(-peor_caso_ref)
  }

  # ---- 4. NAs en censo_ine --------------------------------------------------
  message("[diagnosticos] Comprobando NAs en censo_ine...")
  check4 <- info_enr %>%
    filter(is.na(censo_ine))

  if (nrow(check4) > 0) {
    peor4 <- check4 %>%
      group_by(eleccion_id) %>%
      slice(1) %>%
      ungroup() %>%
      select(eleccion_id, peor_caso_territorio = nombre, peor_nivel = tipo)

    results[["nas_censo_ine"]] <- check4 %>%
      group_by(eleccion_id, tipo_eleccion, year, descripcion) %>%
      summarise(
        n_casos = n(),
        nivel_territorial = paste(sort(unique(tipo)), collapse = " / "),
        .groups = "drop"
      ) %>%
      left_join(peor4, by = "eleccion_id") %>%
      mutate(
        tipo               = "nas_censo_ine",
        eleccion           = descripcion,
        anio               = as.integer(year),
        peor_caso_valor    = NA_character_,
        peor_caso_esperado = NA_character_,
        fuente             = "fuente_original",
        resoluble          = FALSE,
        origen             = "automatico",
        comentario         = NA_character_
      )
  }

  # ---- 5. NAs en votos_validos ----------------------------------------------
  message("[diagnosticos] Comprobando NAs en votos_validos...")
  check5 <- info_enr %>%
    filter(is.na(votos_validos))

  if (nrow(check5) > 0) {
    peor5 <- check5 %>%
      group_by(eleccion_id) %>%
      slice(1) %>%
      ungroup() %>%
      select(eleccion_id, peor_caso_territorio = nombre, peor_nivel = tipo)

    results[["nas_votos_validos"]] <- check5 %>%
      group_by(eleccion_id, tipo_eleccion, year, descripcion) %>%
      summarise(
        n_casos = n(),
        nivel_territorial = paste(sort(unique(tipo)), collapse = " / "),
        .groups = "drop"
      ) %>%
      left_join(peor5, by = "eleccion_id") %>%
      mutate(
        tipo               = "nas_votos_validos",
        eleccion           = descripcion,
        anio               = as.integer(year),
        peor_caso_valor    = NA_character_,
        peor_caso_esperado = NA_character_,
        fuente             = "fuente_original",
        resoluble          = FALSE,
        origen             = "automatico",
        comentario         = NA_character_
      )
  }

  # ---- Consolidar -----------------------------------------------------------
  if (length(results) == 0) {
    message("[diagnosticos] No se detectaron incidencias automáticas.")
    return(tibble(
      tipo = character(), tipo_label = character(), tipo_descripcion = character(),
      eleccion_id = integer(), eleccion = character(), anio = integer(),
      tipo_eleccion = character(), n_casos = integer(),
      nivel_territorial = character(), peor_caso_territorio = character(),
      peor_caso_valor = character(), peor_caso_esperado = character(),
      fuente = character(), resoluble = logical(), origen = character(),
      comentario = character()
    ))
  }

  auto_df <- bind_rows(results) %>%
    left_join(TIPO_META, by = "tipo") %>%
    mutate(
      peor_caso_valor    = as.character(peor_caso_valor),
      peor_caso_esperado = as.character(peor_caso_esperado)
    ) %>%
    select(
      tipo, tipo_label, tipo_descripcion,
      eleccion_id, eleccion, anio, tipo_eleccion,
      n_casos, nivel_territorial,
      peor_caso_territorio, peor_caso_valor, peor_caso_esperado,
      fuente, resoluble, origen, comentario
    ) %>%
    arrange(tipo, anio, eleccion_id)

  message(sprintf("[diagnosticos] %d incidencias automáticas detectadas en %d elecciones.",
                  nrow(auto_df), n_distinct(auto_df$eleccion_id)))
  auto_df
}

# ---------------------------------------------------------------------------
# Merge con incidencias manuales y escritura del CSV final
# ---------------------------------------------------------------------------

#' Combina las incidencias automáticas con las manuales y escribe el CSV.
#'
#' @param auto_df    Data.frame devuelto por detect_diagnosticos().
#' @param manual_path Ruta al CSV de incidencias manuales persistentes.
#' @param output_path Ruta de escritura del CSV combinado.
merge_with_manual <- function(
    auto_df,
    manual_path = MANUAL_CSV,
    output_path = OUTPUT_CSV
) {
  manual_df <- if (file.exists(manual_path)) {
    read_csv(manual_path, show_col_types = FALSE) %>%
      mutate(
        across(c(eleccion_id, anio, n_casos), as.numeric),
        peor_caso_valor    = as.character(peor_caso_valor),
        peor_caso_esperado = as.character(peor_caso_esperado),
        resoluble          = as.logical(resoluble)
      )
  } else {
    message(sprintf("[diagnosticos] No se encontró %s; se usarán solo incidencias automáticas.", manual_path))
    tibble()
  }

  combined <- bind_rows(auto_df, manual_df) %>%
    arrange(tipo, anio, eleccion_id)

  dir.create(dirname(output_path), showWarnings = FALSE, recursive = TRUE)
  write_csv(combined, output_path)
  message(sprintf("[diagnosticos] CSV escrito: %s (%d filas)", output_path, nrow(combined)))
  invisible(output_path)
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

#' Ejecuta la detección completa y escribe el CSV de incidencias.
generate_diagnosticos <- function() {
  auto_df <- detect_diagnosticos()
  merge_with_manual(auto_df)
}

if (sys.nframe() == 0) {
  generate_diagnosticos()
}
