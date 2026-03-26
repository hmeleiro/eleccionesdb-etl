# DESCARGA FICHEROS csv DE LA WEB DEL PARLAMENTO CANARIO:
# https://datos.parcan.es/dataset?q=resultados&sort=score+desc%2C+metadata_modified+desc
# ES NECESARIO DESCARGAR MUNICIPIO A MUNICIPIO, PORQUE EN LOS FICHEROS DE ISLA
# VIENE UN CÓDIGO DE MUNICIPIO DE DOS DÍGITOS QUE HACE DIFÍCIL LA IDENTIFICACIÓN
# DEL MISMO SIN PASAR POR EL NOMBRE DEL MUNICIPIO

library(readr)
library(dplyr)
library(purrr)
library(readxl)

INPUT_DIR <- "data-raw/hechos/05-canarias/"
OUTPUT_DIR <- "data-raw/hechos/05-canarias/parcan/"

fechas <-
  read_csv("tablas-finales/dimensiones/elecciones", show_col_types = F) %>%
  filter(tipo_eleccion == "A" & codigo_ccaa == "05") %>%
  transmute(eleccion_id = id, year = as.character(year))

codigos_municipios <-
  map_df(2:3, function(sheet) {
    read_xlsx(file.path(INPUT_DIR, "24codislas.xlsx"), skip = 2, sheet = sheet) %>%
      janitor::clean_names() %>%
      select(cpro, cisla, cmun) %>%
      transmute(
        circ = case_when(
          cisla == "382" ~ "1",
          cisla == "351" ~ "2",
          cisla == "352" ~ "3",
          cisla == "381" ~ "4",
          cisla == "353" ~ "5",
          cisla == "383" ~ "6",
          cisla == "384" ~ "7",
          T ~ "99"
        ),
        cmun = paste0(cpro, cmun)
      )
  })

codigos_municipios <-
  rbind(
    codigos_municipios,
    mutate(codigos_municipios, circ = 8)
  )

options <-
  map_df(fechas$year, function(y) {
  codigos_municipios %>% mutate(year = y)
})%>%
  filter(!(as.character(year) <= 2015 & as.numeric(circ) == 8))

options_cera <-
  options %>%
  mutate(
    cmun = case_when(
      circ == 1 ~ "38997",
      circ == 2 ~ "35993",
      circ == 3 ~ "35991",
      circ == 4 ~ "38996",
      circ == 5 ~ "35992",
      circ == 6 ~ "38995",
      circ == 7 ~ "38994",
      circ == 8 & year == 2019 ~ "98990",
      circ == 8 & year == 2023 ~ "38990",
    )
  ) %>%
  distinct()

options <- rbind(options, options_cera) %>%
  mutate(
    cmun = ifelse(year == "2019" & circ == "8", paste0("9", substr(cmun, 2, 5)), cmun),
    filename = paste0(year, "_", circ, "_", cmun))

existing <- list.files(OUTPUT_DIR)

options <-
  options %>%
  filter(!filename %in% existing)

for (i in 1:nrow(options)) {
  year <- options$year[i]
  circ <- options$circ[i]
  cmun <- options$cmun[i]
  cmun <- ifelse(year == "2019" & circ == "8", paste0("9", substr(cmun, 2, 5)), cmun)

  url <- sprintf("https://parcan.es/api/resultados_electorales/%s/%s/%s?format=csv", year, circ, cmun)

  tryCatch({
    tmp <- readr::read_csv(url, show_col_types = F)

    if(nrow(tmp) > 0) {
      filename <- paste0(year, "_", circ, "_", cmun)
      write_csv(tmp, paste0(OUTPUT_DIR, filename))
    }
  }, error = function(e){
    message(sprintf("Error: %s", url))
  })

}
