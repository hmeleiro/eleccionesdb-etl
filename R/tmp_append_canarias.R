library(dplyr)
library(stringr)
library(readr)

files <- list.files("data-raw/hechos/05-canarias/parcan/")
parts <- str_match(files, "^(\\d{4})_(\\d)_(\\d{5})$")
df <- tibble(year = parts[, 2], circ_parcan = parts[, 3], cod_mun = parts[, 4]) %>%
    filter(!is.na(year))

df <- df %>%
    mutate(
        circ = case_when(
            circ_parcan == "1" ~ "382", circ_parcan == "2" ~ "351",
            circ_parcan == "3" ~ "352", circ_parcan == "4" ~ "381",
            circ_parcan == "5" ~ "353", circ_parcan == "6" ~ "383",
            circ_parcan == "7" ~ "384"
        ),
        codigo_provincia = substr(cod_mun, 1, 2),
        codigo_municipio = substr(cod_mun, 3, 5)
    )

corresp_new <- df %>%
    filter(
        !is.na(circ), as.numeric(codigo_municipio) < 990,
        codigo_provincia %in% c("35", "38")
    ) %>%
    distinct(codigo_provincia, codigo_municipio,
        codigo_circunscripcion = circ
    ) %>%
    arrange(codigo_circunscripcion, codigo_provincia, codigo_municipio)

existing <- read_csv("data-raw/codigos_territorios/correspondencia_municipio_circunscripcion.csv",
    col_types = cols(.default = "c")
)
combined <- bind_rows(existing, corresp_new)
write_csv(combined, "data-raw/codigos_territorios/correspondencia_municipio_circunscripcion.csv")
cat("Done.", nrow(existing), "+", nrow(corresp_new), "=", nrow(combined), "\n")
