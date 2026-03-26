library(dplyr)
library(httr)
library(rvest)
library(purrr)

url <- "https://www.ine.es/dyngs/INEbase/es/operacion.htm?c=Estadistica_C&cid=1254736177031&menu=ultiDatos&idp=1254734710990"

resp <- GET(url)

urls <-
  resp %>%
  read_html() %>%
  html_elements("option") %>%
  html_attr("value") %>%
  paste0("https://www.ine.es", .)

urls <- urls[!grepl("codmunmod", urls)]

download_codmun <- function(url) {
  filename <-gsub(".+/", "", url)
  filename <- paste0("data-raw/diccionarios-codigos-municipios/", filename)
  download.file(url, destfile = filename, mode = "wb")
}

map(urls, download_codmun)
