library(dplyr)
library(readr)

# Construye y guarda la dimension partidos a partir de partidos_recodes.xlsx
# sin tocar la base de datos. El id se genera localmente.
sync_partidos <- function(path = "data-raw/partidos_recodes.xlsx",
                          colores_path = "data-raw/partidos_colores.xlsx") {
    message("[PARTIDOS] Generando dimension partidos a partir de partidos_recodes.xlsx (solo ficheros, sin BD)...")

    partidos_recodes <- readxl::read_xlsx(path)
    partidos_colores <- readxl::read_xlsx(colores_path)

    partidos_recode <- partidos_recodes %>%
        select(recode, agrupacion) %>%
        filter(!is.na(recode)) %>%
        distinct(recode, .keep_all = TRUE) %>%
        left_join(
            partidos_colores %>%
                select(recode, color) %>%
                distinct(recode, .keep_all = TRUE),
            by = "recode"
        ) %>%
        mutate(id = dplyr::row_number(), .before = 1) %>%
        rename(partido_recode = recode) %>%
        select(id, partido_recode, agrupacion, color)

    partidos_final <- partidos_recodes %>%
        mutate(across(c(denominacion, siglas), tolower, .names = "{.col}_lower")) %>%
        filter(
            !duplicated(paste(denominacion_lower, siglas_lower)),
            !is.na(siglas),
            !is.na(denominacion),
            !is.na(recode)
        ) %>%
        select(recode, denominacion, siglas, agrupacion) %>%
        arrange(recode, denominacion, siglas) %>%
        mutate(id = dplyr::row_number(), .before = 1)

    partidos_dimension <- partidos_final %>%
        left_join(
            partidos_recode %>%
                select(partido_recode_id = id, recode = partido_recode),
            by = "recode"
        ) %>%
        select(id, partido_recode_id, siglas, denominacion) %>%
        distinct()

    dir.create("data-processed", recursive = TRUE, showWarnings = FALSE)
    dir.create("tablas-finales/dimensiones", recursive = TRUE, showWarnings = FALSE)

    readr::write_csv(partidos_final, "data-processed/partidos", na = "NNNNAAAA")
    readr::write_csv(partidos_recode, "tablas-finales/dimensiones/partidos_recode", na = "NNNNAAAA")
    readr::write_csv(partidos_dimension, "tablas-finales/dimensiones/partidos", na = "NNNNAAAA")

    message("[PARTIDOS] Dimension partidos generada en data-processed/partidos.")
    message("[PARTIDOS] Dimension partidos_recode generada en tablas-finales/dimensiones/partidos_recode.")
    message("[PARTIDOS] Dimension partidos base generada en tablas-finales/dimensiones/partidos.")

    invisible(list(
        partidos = partidos_dimension,
        partidos_raw = partidos_final,
        partidos_recode = partidos_recode
    ))
}

if (sys.nframe() == 0) {
    # Permite ejecutar: Rscript code/dimensiones/partidos/sync-partidos.R
    sync_partidos()
}
