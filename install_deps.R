# install_deps.R
# Script para instalar las dependencias necesarias del proyecto new-eleccionesdb

# Lista de paquetes CRAN requeridos
cran_packages <- c(
    "dplyr", "readr", "stringr", "DBI", "RPostgreSQL", "dotenv"
)

# Instala paquetes CRAN que falten
to_install <- setdiff(cran_packages, rownames(installed.packages()))
if (length(to_install) > 0) {
    install.packages(to_install)
}

# Instala paquetes desde GitHub si es necesario (ejemplo)
# if (!requireNamespace("remotes", quietly = TRUE)) install.packages("remotes")
# remotes::install_github("usuario/repositorio")

cat("\nDependencias instaladas correctamente.\n")
