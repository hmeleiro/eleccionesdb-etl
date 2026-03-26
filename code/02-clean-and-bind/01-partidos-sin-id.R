library(dplyr)
library(purrr)
library(DBI)
library(readr)

INPUT_DIR <- "data-processed/hechos/"

votos_files <- list.files(INPUT_DIR, recursive = T, full.names = T, pattern = "votos")

votos <- map(votos_files, readRDS) %>%
  list_rbind() %>%
  arrange(eleccion_id, territorio_id, -votos) %>%
  mutate(votos, representantes, across(c(denominacion, siglas), tolower, .names = "{.col}_lower"))

partidos <- read_csv("data-processed/partidos", show_col_types = F, na = "NNNNAAAA")

partidos_lower <-
  partidos %>%
  mutate(across(c(denominacion, siglas), tolower, .names = "{.col}_lower")) %>%
  select(partido_id = id, denominacion_lower, siglas_lower)

votos <-
  left_join(votos, partidos_lower, by = c("denominacion_lower", "siglas_lower"))

partidos_sin_id <- votos %>%
  filter(is.na(partido_id)) %>%
  group_by(denominacion, siglas) %>%
  summarise(
    votos = sum(votos, na.rm = T),
    representantes = sum(representantes, na.rm = T)
  ) %>%
  arrange(-votos, -representantes)

head(partidos_sin_id, n = 20)

partidos <-
  bind_rows(partidos, partidos_sin_id) %>%
  filter(!duplicated(paste(tolower(denominacion), tolower(siglas)))) %>%
  mutate(id = row_number()) %>%
  select(id, partido_recode_id, siglas, denominacion)

write_csv(partidos, "tablas-finales/dimensiones/partidos", na = "NNNNAAAA")
