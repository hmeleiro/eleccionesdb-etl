library(dplyr)
library(rvest)

OUTPUT_DIR <- "data-raw/hechos/03-asturias"

url <- "https://www.sadei.es/sadei/pxweb/es/01/19__04/190402.px"


res <- read_html(url)

options <- res %>%
  html_elements("option")

code <- options %>%
  html_attr("value")
value <-
  options %>%
  html_text()

tibble(code, value) %>%
  filter(nchar(code) == 5) %>%
  saveRDS(file.path(OUTPUT_DIR, "nomenclator.rds"))



