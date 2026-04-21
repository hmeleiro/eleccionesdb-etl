library(dplyr)
library(readr)

info <- readRDS("tablas-finales/hechos/info.rds")
territorios <- read_csv("tablas-finales/dimensiones/territorios", show_col_types = F)
elecciones <- read_csv("tablas-finales/dimensiones/elecciones", show_col_types = F) %>%
  select(-codigo_ccaa)

secciones_raras_1 <-
  info %>%
  left_join(territorios, by = c("territorio_id" = "id")) %>%
  left_join(elecciones, by = c("eleccion_id" = "id")) %>%
  group_by(year, tipo_eleccion, codigo_ccaa, codigo_seccion) %>%
  count() %>%
  mutate(first_char = substr(codigo_seccion, 1, 1)) %>%
  filter(!first_char %in% c("0", "9")) %>%
  select(-first_char)

secciones_raras_2 <-
  info %>%
  left_join(territorios, by = c("territorio_id" = "id")) %>%
  left_join(elecciones, by = c("eleccion_id" = "id")) %>%
  group_by(year, tipo_eleccion, codigo_ccaa, codigo_seccion) %>%
  count() %>%
  mutate(last_char = substr(codigo_seccion, nchar(codigo_seccion), nchar(codigo_seccion))) %>%
  filter(!last_char %in% 0:9) %>%
  select(-last_char)

rbind(secciones_raras_1, secciones_raras_2) %>%
  arrange(year, tipo_eleccion, codigo_ccaa, codigo_seccion) %>%
  write_csv("docs-site/data/diagnosticos/secciones_4_digitos.csv")
