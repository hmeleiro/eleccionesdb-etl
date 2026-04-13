library(readxl)
library(dplyr)
library(purrr)
library(stringr)

INPUT_DIR <- "data-raw/hechos/03-asturias/gipeyop/"
OUTPUT_DIR <- "data-raw/codigos_territorios/"

files_gipeyop <- list.files(INPUT_DIR, pattern = "\\.xlsx$", full.names = TRUE)

codigos <-
    map_df(files_gipeyop, function(file) {
        read_xlsx(file, sheet = "MESAS") %>%
            transmute(
                codigo_provincia = str_pad(as.character(COD.PROV), 2, "left", "0"),
                codigo_municipio = str_pad(as.character(COD.MUN), 3, "left", "0"),
                codigo_distrito  = str_pad(as.character(DISTRITO), 2, "left", "0"),
                codigo_seccion   = str_pad(as.character(SECCION), 4, "left", "0")
            )
    }) %>%
    mutate(
        codigo_ccaa = "03",
        codigo_circunscripcion = codigo_provincia
    ) %>%
    filter(
        !is.na(codigo_municipio),
        !is.na(codigo_seccion),
        !codigo_municipio %in% c("991", "992", "993")
    ) %>%
    distinct()

saveRDS(codigos, file.path(OUTPUT_DIR, "codigos_secciones_03.rds"))
