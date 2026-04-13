library(readr)
library(dplyr)
library(purrr)
library(stringr)

INPUT_DIR <- "data-raw/hechos/01-andalucia/"
OUTPUT_DIR <- "data-raw/codigos_territorios/"

files_info <- list.files(INPUT_DIR, pattern = "resumen", full.names = T)

codigos <-
  map_df(files_info, function(file) {
    rename_cols <- c(
      "codigo_provincia" = "provincia_clave",
      "codigo_municipio" = "municipio_clave",
      "codigo_distrito" = "distrito_clave",
      "codigo_seccion" = "seccion_clave"
    )

    read_csv(file, show_col_types = F) %>%
      janitor::clean_names() %>%
      rename(any_of(rename_cols))
  }) %>%
  select(any_of(
    c("year", "codigo_ccaa", "codigo_provincia", "codigo_municipio",
      "codigo_distrito", "codigo_seccion"))
  ) %>%
  mutate(
    codigo_ccaa = "01",
    codigo_provincia = str_pad(codigo_provincia, width = 2, side = "left", pad = "0"),
    codigo_municipio = str_pad(codigo_municipio, width = 3, side = "left", pad = "0"),
    codigo_distrito = str_pad(codigo_distrito, width = 2, side = "left", pad = "0"),
    codigo_seccion = str_pad(codigo_seccion, width = 3, side = "left", pad = "0"),
    codigo_circunscripcion = codigo_provincia
  ) %>%
  filter(!is.na(codigo_municipio), !is.na(codigo_seccion))



saveRDS(codigos, file.path(OUTPUT_DIR, "codigos_secciones_01.rds"))
