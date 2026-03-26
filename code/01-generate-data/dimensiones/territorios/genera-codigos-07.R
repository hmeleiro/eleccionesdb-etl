library(readr)
library(dplyr)
library(purrr)
library(stringr)

INPUT_DIR <- "data-raw/hechos/07-cyl"
OUTPUT_DIR <- "data-raw/codigos_territorios"

files_info <- list.files(INPUT_DIR, full.names = T)

codigos <-
  map_df(files_info, function(file) {

    rename_cols <- c(
      "codigo_mesa_completo" = "codigo_de_mesa",
      "codigo_seccion" = "seccion",
      "codigo_distrito" = "distrito",
      "codigo_seccion" = "seccion"
    )

    tmp <-
      read_delim(file, delim = ";", locale=locale(encoding="latin1"), show_col_types = F) %>%
      janitor::clean_names() %>%
      rename(any_of(rename_cols)) %>%
      mutate(
        codigo_provincia = substr(codigo_mesa_completo, 1, 2),
        codigo_distrito = str_pad(codigo_distrito, width = 2, pad = "0"),
        codigo_municipio = str_pad(codigo_municipio, width = 3, pad = "0"),
        codigo_seccion = str_pad(codigo_seccion, width = 4, pad = "0"),
      ) %>%
      select(codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion) %>%
      distinct()

  })  %>%
  mutate(
    codigo_ccaa = "07",
    codigo_circunscripcion = codigo_provincia
  ) %>%
  filter(!is.na(codigo_municipio), !is.na(codigo_seccion)) %>%
  distinct()

saveRDS(codigos, file.path(OUTPUT_DIR, "codigos_secciones_07.rds"))
