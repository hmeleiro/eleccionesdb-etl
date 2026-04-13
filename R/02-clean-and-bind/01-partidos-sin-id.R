library(dplyr)
library(purrr)
library(readr)

INPUT_DIR <- "data-processed/hechos/"

votos_files <- list.files(INPUT_DIR, recursive = T, full.names = T, pattern = "votos")

normalize_lower <- function(x) tolower(trimws(gsub("\\s+", " ", x)))

votos <- map(votos_files, readRDS) %>%
  list_rbind() %>%
  arrange(eleccion_id, territorio_id, -votos) %>%
  mutate(across(c(denominacion, siglas), normalize_lower, .names = "{.col}_lower"))

# Read partidos from data-processed (has: id, recode, denominacion, siglas, agrupacion)
partidos_raw <- read_csv("data-processed/partidos", show_col_types = F, na = "NNNNAAAA")

# Map recode -> partido_recode_id using the partidos_recode dimension
partidos_recode <- read_csv("tablas-finales/dimensiones/partidos_recode", show_col_types = F, na = "NNNNAAAA") %>%
  select(partido_recode_id = id, recode = partido_recode)

partidos <- partidos_raw %>%
  left_join(partidos_recode, by = "recode") %>%
  select(id, partido_recode_id, siglas, denominacion)

partidos_lower <-
  partidos %>%
  mutate(across(c(denominacion, siglas), normalize_lower, .names = "{.col}_lower")) %>%
  select(partido_id = id, denominacion_lower, siglas_lower) %>%
  filter(!duplicated(paste(denominacion_lower, siglas_lower)))

votos <-
  left_join(votos, partidos_lower, by = c("denominacion_lower", "siglas_lower"))

partidos_sin_id <- votos %>%
  filter(is.na(partido_id)) %>%
  group_by(denominacion, siglas) %>%
  summarise(
    votos = sum(votos, na.rm = T)
  ) %>%
  arrange(-votos)

print(partidos_sin_id, n = 50)

partidos <-
  bind_rows(partidos, partidos_sin_id) %>%
  mutate(.dedup_key = paste(
    tolower(trimws(gsub("\\s+", " ", denominacion))),
    tolower(trimws(gsub("\\s+", " ", siglas)))
  )) %>%
  filter(!duplicated(.dedup_key)) %>%
  select(-.dedup_key) %>%
  mutate(id = row_number()) %>%
  select(id, partido_recode_id, siglas, denominacion)

write_csv(partidos, "tablas-finales/dimensiones/partidos", na = "NNNNAAAA")
