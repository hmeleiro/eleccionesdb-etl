# export-descargas.R
#
# Genera tres formatos de descarga a partir de tablas-finales/:
#   1. Parquet  → descargas/parquet/
#   2. SQLite   → descargas/eleccionesdb.sqlite
#   3. CSV planos (pre-joineados) → descargas/csv/
#
# Uso:
#   Rscript code/04-export/export-descargas.R

library(readr)
library(dplyr)
library(arrow)
library(DBI)
library(RSQLite)

# ---------------------------------------------------------------------------
# 0. Leer tablas finales
# ---------------------------------------------------------------------------

tipos_eleccion <- read_csv("tablas-finales/dimensiones/tipos_eleccion", show_col_types = FALSE)
elecciones <- read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE)
territorios <- read_csv("tablas-finales/dimensiones/territorios", show_col_types = FALSE)
partidos_recode <- read_csv("tablas-finales/dimensiones/partidos_recode", show_col_types = FALSE, na = "NNNNAAAA")
partidos <- read_csv("tablas-finales/dimensiones/partidos", show_col_types = FALSE, na = "NNNNAAAA")

info <- readRDS("tablas-finales/hechos/info.rds")
votos <- readRDS("tablas-finales/hechos/votos.rds")

message("[EXPORT] Tablas cargadas correctamente")

# ---------------------------------------------------------------------------
# 1. PARQUET
# ---------------------------------------------------------------------------

dir.create("descargas/parquet/dimensiones", recursive = TRUE, showWarnings = FALSE)
dir.create("descargas/parquet/hechos", recursive = TRUE, showWarnings = FALSE)

write_parquet(tipos_eleccion, "descargas/parquet/dimensiones/tipos_eleccion.parquet")
write_parquet(elecciones, "descargas/parquet/dimensiones/elecciones.parquet")
write_parquet(territorios, "descargas/parquet/dimensiones/territorios.parquet")
write_parquet(partidos_recode, "descargas/parquet/dimensiones/partidos_recode.parquet")
write_parquet(partidos, "descargas/parquet/dimensiones/partidos.parquet")

write_parquet(info, "descargas/parquet/hechos/resumen_territorial.parquet")
write_parquet(votos, "descargas/parquet/hechos/votos_territoriales.parquet")

message("[EXPORT] Parquet exportado en descargas/parquet/")

# ---------------------------------------------------------------------------
# 2. SQLite (con esquema relacional: PKs, FKs, UNIQUE, índices)
# ---------------------------------------------------------------------------

dir.create("descargas", recursive = TRUE, showWarnings = FALSE)

sqlite_path <- "descargas/eleccionesdb.sqlite"
if (file.exists(sqlite_path)) invisible(file.remove(sqlite_path))

con_lite <- dbConnect(SQLite(), sqlite_path)

tryCatch(
    {
        # Activar foreign keys (desactivadas por defecto en SQLite)
        dbExecute(con_lite, "PRAGMA foreign_keys = ON")

        # --- DDL: crear tablas con restricciones ---

        dbExecute(con_lite, "
      CREATE TABLE tipos_eleccion (
        codigo      TEXT PRIMARY KEY,
        descripcion TEXT NOT NULL
      )")

        dbExecute(con_lite, "
      CREATE TABLE elecciones (
        id             INTEGER PRIMARY KEY,
        tipo_eleccion  TEXT NOT NULL,
        year           TEXT NOT NULL,
        mes            TEXT NOT NULL,
        dia            TEXT NOT NULL,
        fecha          TEXT,
        codigo_ccaa    TEXT,
        numero_vuelta  INTEGER DEFAULT 1,
        descripcion    TEXT,
        ambito         TEXT,
        slug           TEXT,
        FOREIGN KEY (tipo_eleccion) REFERENCES tipos_eleccion(codigo),
        UNIQUE (tipo_eleccion, year, mes, codigo_ccaa, numero_vuelta)
      )")

        dbExecute(con_lite, "
      CREATE TABLE territorios (
        id                     INTEGER PRIMARY KEY,
        tipo                   TEXT NOT NULL,
        codigo_ccaa            TEXT,
        codigo_provincia       TEXT,
        codigo_municipio       TEXT,
        codigo_distrito        TEXT,
        codigo_seccion         TEXT,
        codigo_circunscripcion TEXT,
        nombre                 TEXT,
        codigo_completo        TEXT,
        parent_id              INTEGER,
        FOREIGN KEY (parent_id) REFERENCES territorios(id),
        UNIQUE (tipo, codigo_ccaa, codigo_provincia, codigo_municipio,
                codigo_distrito, codigo_seccion, codigo_circunscripcion)
      )")

        dbExecute(con_lite, "
      CREATE TABLE partidos_recode (
        id              INTEGER PRIMARY KEY,
        partido_recode  TEXT NOT NULL,
        agrupacion      TEXT,
        color           TEXT,
        UNIQUE (partido_recode)
      )")

        dbExecute(con_lite, "
      CREATE TABLE partidos (
        id                INTEGER PRIMARY KEY,
        partido_recode_id INTEGER,
        siglas            TEXT,
        denominacion      TEXT,
        FOREIGN KEY (partido_recode_id) REFERENCES partidos_recode(id),
        UNIQUE (siglas, denominacion)
      )")

        dbExecute(con_lite, "
      CREATE TABLE resumen_territorial (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        eleccion_id     INTEGER NOT NULL,
        territorio_id   INTEGER NOT NULL,
        censo_ine       INTEGER,
        participacion_1 INTEGER,
        participacion_2 INTEGER,
        votos_validos   INTEGER,
        abstenciones    INTEGER,
        votos_blancos   INTEGER,
        votos_nulos     INTEGER,
        nrepresentantes INTEGER,
        FOREIGN KEY (eleccion_id)   REFERENCES elecciones(id),
        FOREIGN KEY (territorio_id) REFERENCES territorios(id),
        UNIQUE (eleccion_id, territorio_id)
      )")

        dbExecute(con_lite, "
      CREATE TABLE votos_territoriales (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        eleccion_id    INTEGER NOT NULL,
        territorio_id  INTEGER NOT NULL,
        partido_id     INTEGER NOT NULL,
        votos          INTEGER,
        representantes INTEGER,
        FOREIGN KEY (eleccion_id)   REFERENCES elecciones(id),
        FOREIGN KEY (territorio_id) REFERENCES territorios(id),
        FOREIGN KEY (partido_id)    REFERENCES partidos(id),
        UNIQUE (eleccion_id, territorio_id, partido_id)
      )")

        # --- Insertar datos ---
        dbWriteTable(con_lite, "tipos_eleccion", tipos_eleccion, append = TRUE, row.names = FALSE)
        dbWriteTable(con_lite, "elecciones", elecciones, append = TRUE, row.names = FALSE)
        dbWriteTable(con_lite, "territorios", territorios, append = TRUE, row.names = FALSE)
        dbWriteTable(con_lite, "partidos_recode", partidos_recode, append = TRUE, row.names = FALSE)
        dbWriteTable(con_lite, "partidos", partidos, append = TRUE, row.names = FALSE)
        dbWriteTable(con_lite, "resumen_territorial", info, append = TRUE, row.names = FALSE)
        dbWriteTable(con_lite, "votos_territoriales", votos, append = TRUE, row.names = FALSE)

        # --- Índices adicionales ---
        dbExecute(con_lite, "CREATE INDEX idx_elecciones_tipo ON elecciones(tipo_eleccion)")
        dbExecute(con_lite, "CREATE INDEX idx_territorios_tipo ON territorios(tipo)")
        dbExecute(con_lite, "CREATE INDEX idx_territorios_parent ON territorios(parent_id)")
        dbExecute(con_lite, "CREATE INDEX idx_partidos_recode ON partidos_recode(partido_recode)")
        dbExecute(con_lite, "CREATE INDEX idx_partidos_siglas ON partidos(siglas)")
        dbExecute(con_lite, "CREATE INDEX idx_partidos_recode_id ON partidos(partido_recode_id)")
        dbExecute(con_lite, "CREATE INDEX idx_resumen_eleccion ON resumen_territorial(eleccion_id)")
        dbExecute(con_lite, "CREATE INDEX idx_resumen_territorio ON resumen_territorial(territorio_id)")
        dbExecute(con_lite, "CREATE INDEX idx_votos_eleccion_territorio ON votos_territoriales(eleccion_id, territorio_id)")
        dbExecute(con_lite, "CREATE INDEX idx_votos_partido ON votos_territoriales(partido_id)")
    },
    error = function(e) {
        message("[EXPORT] Error SQLite: ", e$message)
    },
    finally = dbDisconnect(con_lite)
)

message("[EXPORT] SQLite exportado en ", sqlite_path)

# ---------------------------------------------------------------------------
# 3. CSVs planos (pre-joineados)
# ---------------------------------------------------------------------------

dir.create("descargas/csv", recursive = TRUE, showWarnings = FALSE)

# Preparar dimensiones para los JOINs
elecciones_join <- elecciones %>%
    left_join(tipos_eleccion, by = c("tipo_eleccion" = "codigo"), suffix = c("", "_tipo")) %>%
    rename(tipo_eleccion_descripcion = descripcion_tipo, descripcion_eleccion = descripcion) %>%
    select(
        eleccion_id = id, tipo_eleccion, tipo_eleccion_descripcion,
        fecha, year, mes, dia, descripcion_eleccion, ambito, slug
    )

territorios_join <- territorios %>%
    select(
        territorio_id = id, tipo_territorio = tipo,
        codigo_ccaa, codigo_provincia, codigo_municipio,
        codigo_distrito, codigo_seccion, codigo_circunscripcion,
        nombre_territorio = nombre, codigo_completo
    )

partidos_join <- partidos %>%
    left_join(partidos_recode, by = c("partido_recode_id" = "id")) %>%
    select(
        partido_id = id, siglas, denominacion,
        partido_recode, agrupacion
    )

# --- CSV 1: Resumen territorial (plano) ---
resumen_plano <- info %>%
    left_join(elecciones_join, by = "eleccion_id") %>%
    left_join(territorios_join, by = "territorio_id") %>%
    select(
        # Elección
        tipo_eleccion, tipo_eleccion_descripcion, fecha, year, mes,
        descripcion_eleccion, ambito, slug,
        # Territorio
        tipo_territorio, nombre_territorio,
        codigo_ccaa, codigo_provincia, codigo_municipio,
        codigo_distrito, codigo_seccion, codigo_completo,
        # Datos
        censo_ine, participacion_1, participacion_2,
        votos_validos, abstenciones, votos_blancos, votos_nulos, nrepresentantes
    )

write_csv(resumen_plano, "descargas/csv/resumen_territorial.csv", na = "")

message(
    "[EXPORT] CSV resumen_territorial.csv generado (",
    format(nrow(resumen_plano), big.mark = ","), " filas)"
)

# --- CSV 2: Votos por partido (plano) ---
votos_plano <- votos %>%
    left_join(elecciones_join, by = "eleccion_id") %>%
    left_join(territorios_join, by = "territorio_id") %>%
    left_join(partidos_join, by = "partido_id") %>%
    select(
        # Elección
        tipo_eleccion, tipo_eleccion_descripcion, fecha, year, mes,
        descripcion_eleccion, ambito, slug,
        # Territorio
        tipo_territorio, nombre_territorio,
        codigo_ccaa, codigo_provincia, codigo_municipio,
        codigo_distrito, codigo_seccion, codigo_completo,
        # Partido
        siglas, denominacion, partido_recode, agrupacion,
        # Datos
        votos, representantes
    )

write_csv(votos_plano, "descargas/csv/votos_territoriales.csv", na = "")

message(
    "[EXPORT] CSV votos_territoriales.csv generado (",
    format(nrow(votos_plano), big.mark = ","), " filas)"
)

# ---------------------------------------------------------------------------
# Resumen final
# ---------------------------------------------------------------------------

parquet_size <- sum(file.info(list.files("descargas/parquet", recursive = TRUE, full.names = TRUE))$size)
sqlite_size <- file.info(sqlite_path)$size
csv_size <- sum(file.info(list.files("descargas/csv", full.names = TRUE))$size)

format_mb <- function(bytes) sprintf("%.1f MB", bytes / 1024^2)

message("\n[EXPORT] === Resumen ===")
message("  Parquet: ", format_mb(parquet_size))
message("  SQLite:  ", format_mb(sqlite_size))
message("  CSV:     ", format_mb(csv_size))
message("[EXPORT] Completado.")
