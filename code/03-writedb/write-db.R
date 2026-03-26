library(DBI)
library(RPostgreSQL)

source("code/utils.R", encoding = "UTF-8")
source("code/tests/validate_tablas_finales.R", encoding = "UTF-8")

# DIMENSIONES
tipos_eleccion <- read_csv("tablas-finales/dimensiones/tipos_eleccion", show_col_types = FALSE)
elecciones <- read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE)
territorios <- read_csv("tablas-finales/dimensiones/territorios", show_col_types = FALSE)
partidos_recode <- read_csv("tablas-finales/dimensiones/partidos_recode", show_col_types = FALSE, na = "NNNNAAAA")
partidos <- read_csv("tablas-finales/dimensiones/partidos", show_col_types = FALSE, na = "NNNNAAAA")

# HECHOS
info <- readRDS("tablas-finales/hechos/info.rds")
votos <- readRDS("tablas-finales/hechos/votos.rds")

# Validación previa de consistencia con el esquema de base de datos
run_dimension_checks()
run_fact_checks()

con <- connect()

dbExecute(con, "TRUNCATE tipos_eleccion, elecciones, territorios, partidos,
          resumen_territorial, votos_territoriales, partidos_recode,
          resumen_cera, votos_cera, resumen_territorial, votos_territoriales RESTART IDENTITY")

tryCatch(
  {
    dbWriteTable(con, "tipos_eleccion", tipos_eleccion, append = TRUE, row.names = FALSE)
    dbWriteTable(con, "elecciones", elecciones, append = TRUE, row.names = FALSE)
    dbWriteTable(con, "territorios", territorios, append = TRUE, row.names = FALSE)
    dbWriteTable(con, "partidos_recode", partidos_recode, append = TRUE, row.names = FALSE)
    dbWriteTable(con, "partidos", partidos, append = TRUE, row.names = FALSE)

    dbWriteTable(con, "resumen_territorial", info, append = TRUE, row.names = FALSE)
    dbWriteTable(con, "votos_territoriales", votos, append = TRUE, row.names = FALSE)
  },
  error = function(e) {
    message("An error occurred: ", e$message)
  },
  finally = dbDisconnect(con)
)
