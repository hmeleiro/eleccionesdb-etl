# install_deps.R
# Bootstrap secundario para entornos sin renv. La fuente canonica de
# reproducibilidad del proyecto es renv.lock.

cran_packages <- c(
    "renv",
    "arrow",
    "chromote",
    "data.table",
    "DBI",
    "dotenv",
    "dplyr",
    "dtplyr",
    "fs",
    "furrr",
    "future",
    "httr",
    "infoelectoral",
    "janitor",
    "lintr",
    "paws.storage",
    "promises",
    "purrr",
    "pxR",
    "readr",
    "readxl",
    "RPostgreSQL",
    "RSQLite",
    "rvest",
    "stringr",
    "targets",
    "tibble",
    "tidyr",
    "writexl",
    "zip"
)

to_install <- setdiff(cran_packages, rownames(installed.packages()))
if (length(to_install) > 0) {
    install.packages(to_install)
}

cat("\nDependencias instaladas correctamente.\n")
