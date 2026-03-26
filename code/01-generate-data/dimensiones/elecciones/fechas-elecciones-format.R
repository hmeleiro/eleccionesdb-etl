library(dplyr)
library(readr)

Sys.setlocale(category = "LC_TIME", "es_ES.UTF-8")

fechas_elecciones <- read_csv("data-raw/fechas_elecciones.csv", show_col_types = F) %>%
  mutate(
    ccaa = stringr::str_to_title(ccaa),
    fecha = case_when(
      fecha == "10 y 16 de diciembre de 1985" ~"10 de diciembre de 1985",
      fecha == "12 marzo de 2000" ~ "12 de marzo de 2000",
      T ~ fecha),
    fecha = as.Date(fecha, format = "%d de %B de %Y"),
    codigo_ccaa = case_when(
      ccaa == "Andalucia" ~ "01",
      ccaa == "Aragon" ~ "02",
      ccaa == "Asturias" ~ "03",
      ccaa == "Baleares" ~ "04",
      ccaa == "Canarias" ~ "05",
      ccaa == "Cantabria" ~ "06",
      ccaa == "Castillayleon" ~ "07",
      ccaa == "Castilla-Lamancha" ~ "08",
      ccaa == "Catalunya" ~ "09",
      ccaa == "Valencia" ~ "10",
      ccaa == "Extremadura" ~ "11",
      ccaa == "Galicia" ~ "12",
      ccaa == "Madrid" ~ "13",
      ccaa == "Murcia" ~ "14",
      ccaa == "Navarra" ~ "15",
      ccaa == "Paisvasco" ~ "16",
      ccaa == "Rioja" ~ "17",
      ccaa == "Ceuta" ~ "18",
      ccaa == "Melilla" ~ "19",
      T ~ "99"
    )
  )

elecciones <-
  fechas_elecciones %>%
  mutate(
    tipo_eleccion = case_when(
      tipo_eleccion == "generales" ~ "G",
      tipo_eleccion == "autonomicas" ~ "A",
      tipo_eleccion == "locales" ~ "L",
      tipo_eleccion == "europeas" ~ "E"
    ),
    year = format(fecha, "%Y"),
    mes = format(fecha, "%m"),
    dia = format(fecha, "%d"),
    numero_vuelta = 1,
    descripcion = case_when(
      tipo_eleccion == "G" ~ sprintf("Elecciones Generales %s", year),
      tipo_eleccion == "A" ~ sprintf("Elecciones Autonómicas %s %s", ccaa, year),
      tipo_eleccion == "L" ~ sprintf("Elecciones Locales %s", year),
      tipo_eleccion == "E" ~ sprintf("Elecciones Europeas %s", year)
    ),
    ambito = case_when(
      tipo_eleccion == "G" ~ "Nacional",
      tipo_eleccion == "A" ~ "Autonómico",
      tipo_eleccion == "L" ~ "Nacional",
      tipo_eleccion == "E" ~ "Nacional"
    ),
    slug = case_when(
      tipo_eleccion == "G" ~ sprintf("elecciones-generales-%s", year),
      tipo_eleccion == "A" ~ sprintf("elecciones-autonomicas-%s", year),
      tipo_eleccion == "L" ~ sprintf("elecciones-locales-%s", year),
      tipo_eleccion == "E" ~ sprintf("elecciones-europeas-%s", year)
    )
  ) %>%
  select(-ccaa) %>%
  arrange(fecha) %>%
  mutate(id = row_number(), .before = 1)


write_csv(elecciones, "tablas-finales/dimensiones/elecciones")
