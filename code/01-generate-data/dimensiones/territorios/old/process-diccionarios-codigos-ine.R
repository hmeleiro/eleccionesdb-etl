library(dplyr)
library(readxl)
library(purrr)
library(readr)

files <- list.files("data-raw/diccionarios-codigos-municipios/", full.names = T)

read_dic <- function(file) {
  rn_cols <-
    c("cpro" = "provincia",
      "cmun" = "municipio")
  x <- read_excel(file, col_types = "text") %>%
    janitor::clean_names() %>%
    rename(any_of(rn_cols))

  x %>%
    mutate(
      cpro = stringr::str_pad(cpro, 2, "left", "0"),
      file
    )

}

municipios <-
  map(files, read_dic) %>%
  list_rbind() %>%
  mutate(
    cmun = case_when(
      is.na(dc) ~ substr(cmun, 1, nchar(cmun) - 1),
      T ~ cmun
    ),
    cmun = stringr::str_pad(cmun, 3, "left", "0")
  )%>%
  select(-c(dc, codauto)) %>%
  group_by(cpro, cmun) %>%
  summarise(
    nombre = last(nombre)
  )


write_csv(municipios, "data-processed/municipios-ine.csv")

