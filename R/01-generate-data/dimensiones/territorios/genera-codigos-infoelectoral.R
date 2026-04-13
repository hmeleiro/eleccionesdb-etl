library(dplyr, warn.conflicts = FALSE)
library(infoelectoral)
library(readr)
library(purrr)

fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = F) %>%
  filter(tipo_eleccion %in% c("G", "L", "E")) %>%
  transmute(tipo_eleccion, year = as.character(year), mes)

fechas_generales <-
  fechas %>%
  filter(tipo_eleccion == "G")

fechas_locales <-
  fechas %>%
  filter(tipo_eleccion == "L") %>%
  filter(year >= 1987)

fechas_europeas <-
  fechas %>%
  filter(tipo_eleccion == "E") %>%
  filter(year >= 1987)

data_generales <-
  map_df(
    split(fechas_generales, f = 1:nrow(fechas_generales)),
    ~mesas(tipo_eleccion = "congreso", anno = .x$year, mes = .x$mes) %>%
      select(codigo_ccaa, codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion) %>%
      distinct()
  )

data_locales <-
  map_df(
    split(fechas_locales, f = 1:nrow(fechas_locales)),
    ~mesas(tipo_eleccion = "municipales", anno = .x$year, mes = .x$mes) %>%
      select(codigo_ccaa, codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion) %>%
      distinct()
  )

data_europeas <-
  map_df(
    split(fechas_europeas, f = 1:nrow(fechas_europeas)),
    ~mesas(tipo_eleccion = "europeas", anno = .x$year, mes = .x$mes) %>%
      select(codigo_ccaa, codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion) %>%
      distinct()
  )

codigos_ccaa <-
  infoelectoral::codigos_ccaa %>%
  select(-ccaa)

codigos_secciones <-
  bind_rows(data_generales, data_locales, data_europeas)  %>%
  left_join(codigos_ccaa, by = "codigo_ccaa") %>%
  select(-codigo_ccaa) %>%
  rename(codigo_ccaa = codigo_ccaa_ine) %>%
  relocate(codigo_ccaa, 1) %>%
  mutate(codigo_circunscripcion = codigo_provincia)

# MUNICIPIOS
municipios <-
  map_df(
    split(fechas_generales, f = 1:nrow(fechas_generales)),
    ~municipios(tipo_eleccion = "congreso", anno = .x$year, mes = .x$mes)
  ) %>%
  select(codigo_provincia, codigo_municipio, nombre = municipio) %>%
  mutate(codigo_circunscripcion = codigo_provincia) %>%
  distinct()

write_csv(municipios, "data-raw/nombres_municipios.csv")
saveRDS(codigos_secciones, "data-raw/codigos_secciones_infoelectoral.rds")
