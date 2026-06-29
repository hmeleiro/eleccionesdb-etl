library(DBI)
library(RPostgreSQL)
library(readr)
library(data.table)

source("R/utils.R", encoding = "UTF-8")
source("R/tests/validate_tablas_finales.R", encoding = "UTF-8")

PUBLIC_SCHEMA <- "public"
STAGING_SCHEMA <- "pg_temp"
LOAD_CHUNK_SIZE <- 1000000L

final_load_tables <- c(
  "tipos_eleccion",
  "elecciones",
  "territorios",
  "partidos_recode",
  "partidos",
  "elecciones_fuentes",
  "resumen_territorial",
  "votos_territoriales"
)

truncate_tables <- c(
  "tipos_eleccion",
  "elecciones",
  "territorios",
  "partidos",
  "resumen_territorial",
  "votos_territoriales",
  "partidos_recode",
  "resumen_cera",
  "votos_cera",
  "elecciones_fuentes"
)

identity_tables <- c(
  "elecciones",
  "territorios",
  "partidos_recode",
  "partidos",
  "resumen_territorial",
  "votos_territoriales",
  "resumen_cera",
  "votos_cera"
)

sql_ident <- function(x) {
  paste0('"', gsub('"', '""', x, fixed = TRUE), '"')
}

sql_table <- function(schema, table) {
  paste(sql_ident(schema), sql_ident(table), sep = ".")
}

sql_column_list <- function(columns) {
  paste(vapply(columns, sql_ident, character(1)), collapse = ", ")
}

sql_string <- function(con, value) {
  as.character(DBI::dbQuoteString(con, value))
}

staging_table_name <- function(table) {
  paste0("stg_", table)
}

staging_table_ref <- function(table) {
  sql_table(STAGING_SCHEMA, staging_table_name(table))
}

create_staging_table <- function(con, table, columns) {
  staging_name <- staging_table_name(table)
  DBI::dbExecute(
    con,
    sprintf("DROP TABLE IF EXISTS %s", staging_table_ref(table))
  )
  DBI::dbExecute(
    con,
    sprintf(
      "CREATE TEMP TABLE %s ON COMMIT PRESERVE ROWS AS
       SELECT %s
       FROM %s
       WITH NO DATA",
      sql_ident(staging_name),
      sql_column_list(columns),
      sql_table(PUBLIC_SCHEMA, table)
    )
  )
}

load_staging_table_chunked <- function(con, table, df, chunk_size = LOAD_CHUNK_SIZE) {
  n <- nrow(df)
  if (!n) {
    return(invisible(NULL))
  }

  staging_name <- staging_table_name(table)
  starts <- seq.int(1L, n, by = chunk_size)
  for (start in starts) {
    end <- min(start + chunk_size - 1L, n)
    message(sprintf("[DB] Staging %s filas %d-%d de %d", table, start, end, n))
    DBI::dbWriteTable(
      con,
      staging_name,
      df[start:end, , drop = FALSE],
      append = TRUE,
      row.names = FALSE
    )
  }

  invisible(NULL)
}

stage_table <- function(con, table, df) {
  create_staging_table(con, table, names(df))
  load_staging_table_chunked(con, table, df)
  invisible(NULL)
}

insert_staging_into_final <- function(con, table, columns) {
  columns_sql <- sql_column_list(columns)
  DBI::dbExecute(
    con,
    sprintf(
      "INSERT INTO %s (%s)
       SELECT %s
       FROM %s",
      sql_table(PUBLIC_SCHEMA, table),
      columns_sql,
      columns_sql,
      staging_table_ref(table)
    )
  )
}

reset_identity_sequence <- function(con, table, column = "id") {
  DBI::dbExecute(
    con,
    sprintf(
      "SELECT setval(
         pg_get_serial_sequence(%s, %s),
         COALESCE((SELECT MAX(%s) FROM %s), 1),
         EXISTS (SELECT 1 FROM %s)
       )",
      sql_string(con, paste(PUBLIC_SCHEMA, table, sep = ".")),
      sql_string(con, column),
      sql_ident(column),
      sql_table(PUBLIC_SCHEMA, table),
      sql_table(PUBLIC_SCHEMA, table)
    )
  )
}

drop_load_indexes <- function(con) {
  DBI::dbExecute(con, sprintf("DROP INDEX IF EXISTS %s", sql_table(PUBLIC_SCHEMA, "idx_resumen_territorio")))
  DBI::dbExecute(con, sprintf("DROP INDEX IF EXISTS %s", sql_table(PUBLIC_SCHEMA, "idx_votos_eleccion_territorio")))
  DBI::dbExecute(con, sprintf("DROP INDEX IF EXISTS %s", sql_table(PUBLIC_SCHEMA, "idx_votos_partido")))
}

recreate_load_indexes <- function(con) {
  DBI::dbExecute(
    con,
    sprintf(
      "CREATE INDEX IF NOT EXISTS %s
       ON %s(territorio_id)",
      sql_ident("idx_resumen_territorio"),
      sql_table(PUBLIC_SCHEMA, "resumen_territorial")
    )
  )
  DBI::dbExecute(
    con,
    sprintf(
      "CREATE INDEX IF NOT EXISTS %s
       ON %s(eleccion_id, territorio_id)",
      sql_ident("idx_votos_eleccion_territorio"),
      sql_table(PUBLIC_SCHEMA, "votos_territoriales")
    )
  )
  DBI::dbExecute(
    con,
    sprintf(
      "CREATE INDEX IF NOT EXISTS %s
       ON %s(partido_id)",
      sql_ident("idx_votos_partido"),
      sql_table(PUBLIC_SCHEMA, "votos_territoriales")
    )
  )
  DBI::dbExecute(con, sprintf("ANALYZE %s", sql_table(PUBLIC_SCHEMA, "resumen_territorial")))
  DBI::dbExecute(con, sprintf("ANALYZE %s", sql_table(PUBLIC_SCHEMA, "votos_territoriales")))
}

ensure_partidos_recode_metadata_columns <- function(con) {
  metadata_columns <- c(
    bloque = "VARCHAR(50)",
    color_pastel = "VARCHAR(7)",
    color_oscuro = "VARCHAR(7)"
  )

  for (column in names(metadata_columns)) {
    DBI::dbExecute(
      con,
      sprintf(
        "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s NULL",
        sql_table(PUBLIC_SCHEMA, "partidos_recode"),
        sql_ident(column),
        metadata_columns[[column]]
      )
    )
  }
}

replace_final_tables_from_staging <- function(con, table_columns) {
  DBI::dbExecute(con, "SET LOCAL synchronous_commit TO off")
  DBI::dbExecute(con, "SET LOCAL maintenance_work_mem TO '512MB'")
  drop_load_indexes(con)

  DBI::dbExecute(
    con,
    sprintf(
      "TRUNCATE %s RESTART IDENTITY",
      paste(vapply(truncate_tables, function(table) {
        sql_table(PUBLIC_SCHEMA, table)
      }, character(1)), collapse = ", ")
    )
  )

  for (table in final_load_tables) {
    message(sprintf("[DB] Reemplazando %s desde staging", table))
    insert_staging_into_final(con, table, table_columns[[table]])
  }

  for (table in identity_tables) {
    reset_identity_sequence(con, table)
  }

  recreate_load_indexes(con)
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

table_columns <- list(
  tipos_eleccion = names(tipos_eleccion),
  elecciones = names(elecciones),
  territorios = names(territorios),
  partidos_recode = names(partidos_recode),
  partidos = names(partidos),
  elecciones_fuentes = names(elecciones_fuentes),
  resumen_territorial = names(info),
  votos_territoriales = names(votos)
)

con <- connect()
transaction_open <- FALSE

tryCatch(
  {
    ensure_partidos_recode_metadata_columns(con)
    message("[DB] Preparando staging temporal...")
    stage_table(con, "tipos_eleccion", tipos_eleccion)
    stage_table(con, "elecciones", elecciones)
    stage_table(con, "territorios", territorios)
    stage_table(con, "partidos_recode", partidos_recode)
    stage_table(con, "partidos", partidos)
    stage_table(con, "elecciones_fuentes", elecciones_fuentes)

    rm(tipos_eleccion, elecciones, territorios, partidos_recode, partidos, elecciones_fuentes)
    invisible(gc())

    stage_table(con, "resumen_territorial", info)
    rm(info)
    invisible(gc())

    stage_table(con, "votos_territoriales", votos)
    rm(votos)
    invisible(gc())

    message("[DB] Reemplazando tablas finales en una transaccion...")
    DBI::dbBegin(con)
    transaction_open <- TRUE
    replace_final_tables_from_staging(con, table_columns)
    DBI::dbCommit(con)
    transaction_open <- FALSE
    message("[DB] Carga transaccional completada.")
  },
  error = function(e) {
    message("An error occurred: ", e$message)
    if (isTRUE(transaction_open)) {
      message("[DB] Rollback de la transaccion final...")
      try(DBI::dbRollback(con), silent = TRUE)
    }
    stop(e)
  },
  finally = DBI::dbDisconnect(con)
)
