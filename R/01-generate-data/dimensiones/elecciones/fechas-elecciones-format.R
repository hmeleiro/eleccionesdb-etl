library(dplyr)
library(readr)

parse_spanish_date <- function(x) {
  month_map <- c(
    enero = "01",
    febrero = "02",
    marzo = "03",
    abril = "04",
    mayo = "05",
    junio = "06",
    julio = "07",
    agosto = "08",
    septiembre = "09",
    setiembre = "09",
    octubre = "10",
    noviembre = "11",
    diciembre = "12"
  )

  x <- trimws(tolower(x))
  x <- ifelse(x == "10 y 16 de diciembre de 1985", "10 de diciembre de 1985", x)
  x <- ifelse(x == "12 marzo de 2000", "12 de marzo de 2000", x)

  matches <- regexec("^([0-9]{1,2}) de ([[:alpha:]]+) de ([0-9]{4})$", x)
  parts <- regmatches(x, matches)

  parsed <- vapply(parts, function(part) {
    month <- if (length(part) == 4) unname(month_map[part[3]]) else NA_character_
    if (length(part) != 4 || is.na(month)) {
      return(NA_character_)
    }

    sprintf("%s-%s-%02d", part[4], month, as.integer(part[2]))
  }, character(1))

  as.Date(parsed)
}

fechas_elecciones <- read_csv("data-raw/fechas_elecciones.csv", show_col_types = F) %>%
  mutate(
    ccaa = stringr::str_to_title(ccaa),
    fecha = case_when(
      fecha == "10 y 16 de diciembre de 1985" ~ "10 de diciembre de 1985",
      fecha == "12 marzo de 2000" ~ "12 de marzo de 2000",
      T ~ fecha
    ),
    fecha = parse_spanish_date(fecha),
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

if (any(is.na(fechas_elecciones$fecha))) {
  stop(
    "No se pudieron parsear algunas fechas de data-raw/fechas_elecciones.csv.",
    call. = FALSE
  )
}

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
  filter(
    fecha != "2020-04-05" # elecciones gallegas y vascas que se suspendieron por la pandemia
  ) %>%
  mutate(id = row_number(), .before = 1)

write_csv(elecciones, "tablas-finales/dimensiones/elecciones")
