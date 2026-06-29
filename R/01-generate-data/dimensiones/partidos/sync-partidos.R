library(dplyr)
library(readr)

# Construye y guarda la dimension partidos a partir de partidos_recodes.xlsx
# sin tocar la base de datos. El id se genera localmente.
sync_partidos <- function(path = "data-raw/partidos_recodes.xlsx",
                          colores_path = "data-raw/partidos_colores.xlsx") {
  message("[PARTIDOS] Generando dimension partidos a partir de partidos_recodes.xlsx (solo ficheros, sin BD)...")

  partidos_recodes <- readxl::read_xlsx(path)
  partidos_colores <- readxl::read_xlsx(colores_path)

  required_colores_cols <- c("recode", "bloque", "color", "color_pastel", "color_oscuro")
  missing_colores_cols <- setdiff(required_colores_cols, names(partidos_colores))
  if (length(missing_colores_cols) > 0) {
    stop(
      "[PARTIDOS] Faltan columnas en partidos_colores.xlsx: ",
      paste(missing_colores_cols, collapse = ", "),
      call. = FALSE
    )
  }

  duplicated_colores <- partidos_colores %>%
    filter(!is.na(recode)) %>%
    count(recode) %>%
    filter(n > 1)
  if (nrow(duplicated_colores) > 0) {
    stop(
      "[PARTIDOS] partidos_colores.xlsx contiene recodes duplicados: ",
      paste(duplicated_colores$recode, collapse = ", "),
      call. = FALSE
    )
  }

  recodes_maestros <- partidos_recodes %>%
    filter(!is.na(recode)) %>%
    distinct(recode) %>%
    pull(recode)

  recodes_extra_colores <- setdiff(
    partidos_colores %>%
      filter(!is.na(recode)) %>%
      distinct(recode) %>%
      pull(recode),
    recodes_maestros
  )
  if (length(recodes_extra_colores) > 0) {
    warning(
      "[PARTIDOS] partidos_colores.xlsx contiene recodes no presentes en partidos_recodes.xlsx: ",
      paste(recodes_extra_colores, collapse = ", "),
      call. = FALSE
    )
  }

  partidos_recode <- partidos_recodes %>%
    select(recode, agrupacion) %>%
    filter(!is.na(recode)) %>%
    distinct(recode, .keep_all = TRUE) %>%
    left_join(
      partidos_colores %>%
        select(recode, bloque, color, color_pastel, color_oscuro) %>%
        distinct(recode, .keep_all = TRUE),
      by = "recode"
    ) %>%
    mutate(
      bloque = coalesce(bloque, "Otros"),
      color = coalesce(color, "#808080"),
      color_pastel = coalesce(color_pastel, "#D9D9D9"),
      color_oscuro = coalesce(color_oscuro, "#595959")
    ) %>%
    mutate(id = dplyr::row_number(), .before = 1) %>%
    rename(partido_recode = recode) %>%
    select(id, partido_recode, agrupacion, bloque, color, color_pastel, color_oscuro)

  partidos_final <- partidos_recodes %>%
    mutate(
      across(c(denominacion, siglas), tolower, .names = "{.col}_lower"),
      .dedup_key = paste(
        tolower(trimws(gsub("\\s+", " ", denominacion))),
        tolower(trimws(gsub("\\s+", " ", siglas)))
      )) %>%
    filter(
      !duplicated(.dedup_key),
      !is.na(siglas),
      !is.na(denominacion),
      !is.na(recode)
    ) %>%
    select(recode, denominacion, siglas, agrupacion) %>%
    arrange(recode, denominacion, siglas) %>%
    mutate(id = dplyr::row_number(), .before = 1)

  dir.create("data-processed", recursive = TRUE, showWarnings = FALSE)
  dir.create("tablas-finales/dimensiones", recursive = TRUE, showWarnings = FALSE)

  readr::write_csv(partidos_final, "data-processed/partidos", na = "NNNNAAAA")
  readr::write_csv(partidos_recode, "tablas-finales/dimensiones/partidos_recode", na = "NNNNAAAA")

  message("[PARTIDOS] Dimension partidos generada en data-processed/partidos.")
  message("[PARTIDOS] Dimension partidos_recode generada en tablas-finales/dimensiones/partidos_recode.")

  invisible(list(
    partidos = partidos_final,
    partidos_recode = partidos_recode
  ))
}

if (sys.nframe() == 0) {
  # Permite ejecutar: Rscript code/dimensiones/partidos/sync-partidos.R
  sync_partidos()
}
