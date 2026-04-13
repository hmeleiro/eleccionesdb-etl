library(dplyr)
library(readr)

partidos_recodes_raw <- readxl::read_xlsx("data-raw/partidos_recodes.xlsx")

partidos_colores_raw <- readxl::read_xlsx("data-raw/partidos_colores.xlsx")


partidos <- full_join(partidos_recodes_raw, partidos_colores_raw, by = "recode")


partidos_recode <-
  partidos %>%
  select(recode, agrupacion, color, color_pastel) %>%
  filter(!duplicated(recode)) %>%
  mutate(partido_recode_id = dplyr::row_number(), .before = 1)

partidos <-
  partidos_recodes_raw %>%
  mutate(across(c(denominacion, siglas), tolower, .names = "{.col}_lower")) %>%
  filter(
    !duplicated(paste(denominacion_lower, siglas_lower)),
    !is.na(siglas),
    !is.na(denominacion),
    !is.na(recode)
  ) %>%
  arrange(recode, denominacion, siglas) %>%
  mutate(id = dplyr::row_number(), .before = 1) %>%
  left_join(partidos_recode, by = "recode") %>%
  relocate(partido_recode_id, .after = 1) %>%
  select(id, partido_recode_id, siglas, denominacion) %>%
  distinct()


partidos_recode <-
  partidos_recode %>%
  rename(
    id = partido_recode_id,
    partido_recode = recode) %>%
  select(-color_pastel)

readr::write_csv(partidos, "data-processed/partidos", na = "NNNNAAAA")
readr::write_csv(partidos_recode, "tablas-finales/dimensiones/partidos_recode", na = "NNNNAAAA")

message("[PARTIDOS] Dimension partidos generada en data-processed/partidos.")
