library(DBI)
library(RPostgreSQL)
library(readr)
library(data.table)

source("R/utils.R", encoding = "UTF-8")
source("R/tests/validate_tablas_finales.R", encoding = "UTF-8")

write_table_chunked <- function(con, table, df, chunk_size = 1000000L) {
  n <- nrow(df)
  if (!n) {
    return(invisible(NULL))
  }

  starts <- seq.int(1L, n, by = chunk_size)
  for (start in starts) {
    end <- min(start + chunk_size - 1L, n)
    message(sprintf("[DB] Escribiendo %s filas %d-%d de %d", table, start, end, n))
    DBI::dbWriteTable(
      con,
      table,
      df[start:end, ],
      append = TRUE,
      row.names = FALSE
    )
  }

  invisible(NULL)
}

drop_load_indexes <- function(con) {
  DBI::dbExecute(con, "DROP INDEX IF EXISTS idx_resumen_territorio")
  DBI::dbExecute(con, "DROP INDEX IF EXISTS idx_votos_eleccion_territorio")
  DBI::dbExecute(con, "DROP INDEX IF EXISTS idx_votos_partido")
}

recreate_load_indexes <- function(con) {
  DBI::dbExecute(
    con,
    "CREATE INDEX IF NOT EXISTS idx_resumen_territorio
       ON resumen_territorial(territorio_id)"
  )
  DBI::dbExecute(
    con,
    "CREATE INDEX IF NOT EXISTS idx_votos_eleccion_territorio
       ON votos_territoriales(eleccion_id, territorio_id)"
  )
  DBI::dbExecute(
    con,
    "CREATE INDEX IF NOT EXISTS idx_votos_partido
       ON votos_territoriales(partido_id)"
  )
  DBI::dbExecute(con, "ANALYZE resumen_territorial")
  DBI::dbExecute(con, "ANALYZE votos_territoriales")
}

# DIMENSIONES
tipos_eleccion <- read_csv("tablas-finales/dimensiones/tipos_eleccion", show_col_types = FALSE)
elecciones <- read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE)
territorios <- read_csv("tablas-finales/dimensiones/territorios", show_col_types = FALSE)
partidos_recode <- read_csv("tablas-finales/dimensiones/partidos_recode", show_col_types = FALSE, na = "NNNNAAAA")
partidos <- read_csv("tablas-finales/dimensiones/partidos", show_col_types = FALSE, na = "NNNNAAAA")
elecciones_fuentes <- read_csv("tablas-finales/dimensiones/elecciones_fuentes", show_col_types = FALSE)

# HECHOS
info <- readRDS("tablas-finales/hechos/info.rds")
votos <- readRDS("tablas-finales/hechos/votos.rds")
data.table::setDT(info)
data.table::setDT(votos)

# Validacion previa de consistencia con el esquema de base de datos.
# Reuse already-loaded objects to avoid reading the large fact tables twice.
run_dimension_checks(
  tipos_eleccion = tipos_eleccion,
  elecciones = elecciones,
  territorios = territorios,
  partidos_recode = partidos_recode,
  partidos = partidos,
  elecciones_fuentes = elecciones_fuentes
)
run_fact_checks(
  info = info,
  votos = votos,
  elecciones = elecciones,
  territorios = territorios,
  partidos = partidos
)

con <- connect()
indexes_dropped <- FALSE

tryCatch(
  {
    DBI::dbExecute(con, "SET synchronous_commit TO off")
    drop_load_indexes(con)
    indexes_dropped <- TRUE

    dbExecute(con, "TRUNCATE tipos_eleccion, elecciones, territorios, partidos,
          resumen_territorial, votos_territoriales, partidos_recode,
          resumen_cera, votos_cera, elecciones_fuentes RESTART IDENTITY")

    dbWriteTable(con, "tipos_eleccion", tipos_eleccion, append = TRUE, row.names = FALSE)
    dbWriteTable(con, "elecciones", elecciones, append = TRUE, row.names = FALSE)
    dbWriteTable(con, "territorios", territorios, append = TRUE, row.names = FALSE)
    dbWriteTable(con, "partidos_recode", partidos_recode, append = TRUE, row.names = FALSE)
    dbWriteTable(con, "partidos", partidos, append = TRUE, row.names = FALSE)
    dbWriteTable(con, "elecciones_fuentes", elecciones_fuentes, append = TRUE, row.names = FALSE)

    write_table_chunked(con, "resumen_territorial", info)
    rm(info)
    invisible(gc())

    write_table_chunked(con, "votos_territoriales", votos)

    recreate_load_indexes(con)
    indexes_dropped <- FALSE
  },
  error = function(e) {
    message("An error occurred: ", e$message)
    if (isTRUE(indexes_dropped)) {
      message("Recreating load indexes after error...")
      try(recreate_load_indexes(con), silent = TRUE)
    }
    stop(e)
  },
  finally = dbDisconnect(con)
)
