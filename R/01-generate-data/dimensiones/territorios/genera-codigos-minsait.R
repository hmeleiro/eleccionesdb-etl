library(readr)
library(dplyr)
library(purrr)
library(stringr)

INPUT_DIR <- "data-raw/hechos/minsait/"
OUTPUT_DIR <- "data-raw/codigos_territorios"

files_info <- list.files(INPUT_DIR, full.names = T)

# Tabla de correspondencia entre códigos de CCAA del Ministerio del Interior (MIR)
# y los códigos INE. Los CSV de Minsait usan la codificación MIR.
codigos_ccaa <- readRDS("data-raw/hechos/00-congreso/codigos_ccaa.rds") %>%
  select(-ccaa)

remap_ccaa <- function(df) {
  df %>%
    left_join(codigos_ccaa, by = "codigo_ccaa") %>%
    select(-codigo_ccaa) %>%
    rename(codigo_ccaa = codigo_ccaa_ine)
}

codigos <-
  map_df(files_info, function(file) {

    rename_cols <- c(
      "codigo_mesa_completo" = "codigo_de_mesa",
      "codigo_seccion" = "seccion",
      "codigo_distrito" = "distrito",
      "codigo_seccion" = "seccion"
    )

    tmp <-
      read_csv(file, show_col_types = F) %>%
      janitor::clean_names() %>%
      mutate(
        codigo_ccaa      = str_pad(as.character(codigo_ccaa), 2, "left", "0"),
        codigo_provincia = str_pad(as.character(codigo_provincia), 2, "left", "0"),
        codigo_municipio = str_pad(as.character(codigo_municipio), 3, "left", "0"),
        codigo_distrito  = str_pad(as.character(codigo_distrito),  2, "left", "0"),
        codigo_seccion   = str_pad(as.character(codigo_seccion),   4, "left", "0")
      ) %>%
      select(codigo_ccaa, codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion) %>%
      distinct()

  })  %>%
  mutate(
    codigo_circunscripcion = codigo_provincia
  ) %>%
  filter(!is.na(codigo_municipio), !is.na(codigo_seccion)) %>%
  distinct() %>%
  remap_ccaa()

saveRDS(codigos, file.path(OUTPUT_DIR, "codigos_secciones_minsait.rds"))
