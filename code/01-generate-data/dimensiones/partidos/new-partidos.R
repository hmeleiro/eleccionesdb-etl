library(dplyr)
library(purrr)
library(readr)
library(DBI)

source("code/utils.R", encoding = "UTF-8")

votos_files <- list.files("data-processed/hechos", recursive = T, full.names = T, pattern = "votos")

votos <- map(votos_files, readRDS) %>%
  list_rbind() %>%
  arrange(eleccion_id, territorio_id, -votos) %>%
  mutate(votos, across(c(denominacion, siglas), tolower, .names = "{.col}_lower"))

partidos_raw <- votos %>%
  mutate(votos, across(c(denominacion, siglas), tolower, .names = "{.col}_lower"),
         .keep = "used") %>%
  filter(!duplicated(paste(denominacion_lower, siglas_lower)))


# CONSULTA PARTIDOS EXISTENTES
con <- connect()
existing_partidos <-
  dbGetQuery(con, "SELECT id, partido_recode_id, denominacion, siglas FROM partidos") %>%
  mutate(across(c(denominacion, siglas), tolower, .names = "{.col}_lower"))
partidos_recode <-
  dbGetQuery(con, "SELECT id as partido_recode_id, partido_recode FROM partidos_recode")
existing_partidos <- full_join(existing_partidos, partidos_recode, by = "partido_recode_id") %>%
  as_tibble()
dbDisconnect(con)

new_partidos <-
  anti_join(
    partidos_raw, existing_partidos,
    by = c("denominacion_lower", "siglas_lower")
  ) %>%
  mutate(denominacion, siglas, recode = "Otros", .keep = "used")

# GUARDAR PARTIDOS NUEVOS PARA REVISAR
if(nrow(new_partidos) > 0) {
  new_partidos %>%
    mutate(recode = NA) %>%
    writexl::write_xlsx("data-processed/partidos_recodes_pending.xlsx")
}

# # UNA VEZ REVISADOS, AÑADIR A LA BASE DE DATOS
new_partidos_checked <- readxl::read_xlsx("data-processed/partidos_recodes_pending.xlsx")
if(nrow(new_partidos_checked) > 0) {
  con <- connect()

  tryCatch(
    {
      dbWriteTable(con, "partidos", new_partidos, append = T, row.names = F)
    },
    error = function(e) {
      message("Error al escribir en partidos_recode: ", e$message)
    }, finally = dbDisconnect(con)
  )
}

