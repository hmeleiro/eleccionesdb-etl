library(dplyr)
library(readr)

# Construye y guarda la dimension partidos a partir de partidos_recodes.xlsx
# sin tocar la base de datos. El id se genera localmente.
sync_partidos <- function(path = "data-raw/partidos_recodes.xlsx") {
    message("[PARTIDOS] Generando dimension partidos a partir de partidos_recodes.xlsx (solo ficheros, sin BD)...")

    partidos_recodes <- readxl::read_xlsx(path)

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

    readr::write_csv(partidos_final, "data-processed/partidos", na = "NNNNAAAA")

    message("[PARTIDOS] Dimension partidos generada en data-processed/partidos.")

    invisible(partidos_final)
}

if (sys.nframe() == 0) {
    # Permite ejecutar: Rscript code/dimensiones/partidos/sync-partidos.R
    sync_partidos()
}
