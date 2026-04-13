library(dplyr)
library(readr)
library(httr)
library(rvest)


parse_dates <- function(url) {
  resp <- GET(url)

  if(resp$status_code == 200) {
    html <- read_html(resp)
    fechas <-
      html %>%
      html_element(".lista-flecha-azul") %>%
      html_elements("a") %>%
      html_text()

    return(fechas)

  }

  return(NULL)
}


BASE_URL <- "https://www.juntaelectoralcentral.es"
url <- paste0(BASE_URL, "/cs/jec/elecciones")

resp <- GET(url)

if(resp$status_code == 200) {

  html <- read_html(resp)

  election_type_urls <-
    html %>%
    html_element(".submenu") %>%
    html_elements(".op1 a") %>%
    html_attr(name = "href") %>%
    paste0(BASE_URL, .)

  election_type_urls <- election_type_urls[1:4]

  fechas_elecciones <- NULL
  for(election_type_url in election_type_urls) {
    resp <- GET(election_type_url)
    tipo_eleccion <-
      gsub(paste0(BASE_URL, "/cs/jec/elecciones/"), "", election_type_url)

    if(tipo_eleccion == "autonomicas") {
      html <- read_html(resp)
      autonomicas_urls <-
        html %>%
        html_element(".lista-flecha-azul") %>%
        html_elements("a") %>%
        html_attr("href") %>%
        paste0(BASE_URL, .)

      for (url in autonomicas_urls) {

        ccaa <-
          gsub(paste0(BASE_URL, "/cs/jec/elecciones/autonomicas/"), "", url)

        fechas <- parse_dates(url)

        out <- tibble(
          tipo_eleccion = tipo_eleccion,
          ccaa,
          fecha = fechas
        )

        fechas_elecciones <- bind_rows(fechas_elecciones, out)

      }

    } else {
      fechas <- parse_dates(election_type_url)

      out <- tibble(
        tipo_eleccion = tipo_eleccion,
        fecha = fechas
      )

      fechas_elecciones <- bind_rows(fechas_elecciones, out)

    }
  }
}


write_csv(fechas_elecciones, "data-raw/fechas_elecciones.csv")
