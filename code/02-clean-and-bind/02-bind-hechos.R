library(dplyr)
library(purrr)
library(DBI)
library(readr)

source("code/utils.R", encoding = "UTF-8")
source("code/tests/validate_tablas_finales.R")

info_files <- list.files("data-processed/", recursive = T, full.names = T, pattern = "info")
votos_files <- list.files("data-processed/", recursive = T, full.names = T, pattern = "votos")

nrepresentantes <- get_nrepresentantes()
representantes <- get_representantes()

votos <- map(votos_files, readRDS) %>%
  list_rbind() %>%
  arrange(eleccion_id, territorio_id, -votos) %>%
  mutate(votos, representantes, across(c(denominacion, siglas), tolower, .names = "{.col}_lower"))

info <- map(info_files, readRDS) %>%
  list_rbind() %>%
  arrange(eleccion_id, territorio_id)


# ASIGNAR ID DE PARTIDO A votos
partidos <- read_csv("tablas-finales/dimensiones/partidos", show_col_types = F, na = "NNNNAAAA") %>%
  mutate(across(c(denominacion, siglas), tolower, .names = "{.col}_lower")) %>%
  select(partido_id = id, denominacion_lower, siglas_lower)

votos <-
  left_join(votos, partidos, by = c("denominacion_lower", "siglas_lower"))


votos_sin_id <- votos %>%
  filter(is.na(partido_id)) %>%
  group_by(denominacion, siglas) %>%
  summarise(
    votos = sum(votos, na.rm = T),
    representantes = sum(representantes, na.rm = T)
  ) %>%
  arrange(-representantes, -votos) %>%
  select(denominacion, siglas, votos, representantes)


if (nrow(votos_sin_id) > 0) {
  stop("Hay partidos sin partido_id asignado en votos.")
  print(head(votos_sin_id))
}

# JOIN CON EL NÚMERO DE REPRESENTANTES (G, A, L)
info <-
  info %>%
  left_join(nrepresentantes, by = join_by(eleccion_id, territorio_id)) %>%
  mutate(
    votos_validos = ifelse(is.na(votos_validos), censo_ine - abstenciones - votos_nulos, votos_validos),
    nrepresentantes = coalesce(nrepresentantes.x, nrepresentantes.y),
  ) %>%
  select(-c(nrepresentantes.x, nrepresentantes.y))

votos <-
  votos %>%
  left_join(representantes, by = join_by(eleccion_id, territorio_id, partido_id)) %>%
  mutate(representantes = coalesce(representantes.x, representantes.y)) %>%
  select(-c(representantes.x, representantes.y)) %>%
  select(-c(denominacion_lower, siglas_lower, denominacion, siglas))

votos_grouped <-
  votos %>%
  dtplyr::lazy_dt() %>%
  group_by(eleccion_id, territorio_id, partido_id) %>%
  summarise(across(c(votos, representantes), sum, na.rm = T)) %>%
  as_tibble()

# Asegurar que columnas enteras no tengan decimales (sum() en R convierte integer a double)
int_cols <- c(
  "censo_ine", "participacion_1", "participacion_2", "participacion_3",
  "votos_validos", "abstenciones", "votos_blancos", "votos_nulos", "nrepresentantes"
)
info <- info %>%
  mutate(across(any_of(int_cols), ~ as.integer(round(.))))

# CHECKS
validate_hechos_info(info, label = "[HECHOS] info")
validate_hechos_votos(votos_grouped, label = "[HECHOS] votos")

saveRDS(info, "tablas-finales/hechos/info.rds")
saveRDS(votos_grouped, "tablas-finales/hechos/votos.rds")

message("[HECHOS] Hechos info y votos generados en tablas-finales/hechos/")
