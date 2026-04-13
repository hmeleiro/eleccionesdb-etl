library(dplyr)
library(purrr)
library(readr)
library(infoelectoral)

OUTPUT_DIR <- "data-raw/hechos/00-congreso/"
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

# --- Fechas de elecciones generales ---
fechas <-
    read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE) %>%
    filter(tipo_eleccion == "G") %>%
    transmute(year = as.character(year), mes)

# --- Mesas ---
cat("Descargando mesas...\n")
data_mesas <- map_df(
    split(fechas, seq_len(nrow(fechas))),
    ~ {
        cat("  mesas()", .x$year, .x$mes, "\n")
        mesas(tipo_eleccion = "congreso", anno = .x$year, mes = .x$mes)
    }
)
saveRDS(data_mesas, paste0(OUTPUT_DIR, "mesas.rds"))
cat("  ->", nrow(data_mesas), "filas\n")

# --- Provincias ---
cat("Descargando provincias...\n")
data_provincias <- map_df(
    split(fechas, seq_len(nrow(fechas))),
    ~ {
        cat("  provincias()", .x$year, .x$mes, "\n")
        provincias(tipo_eleccion = "congreso", anno = .x$year, mes = .x$mes)
    }
)
saveRDS(data_provincias, paste0(OUTPUT_DIR, "provincias.rds"))
cat("  ->", nrow(data_provincias), "filas\n")

# --- Municipios (fallback para elecciones sin mesas) ---
cat("Descargando municipios (fallback)...\n")
data_municipios <- map_df(
    split(fechas, seq_len(nrow(fechas))),
    ~ {
        cat("  municipios()", .x$year, .x$mes, "\n")
        municipios(tipo_eleccion = "congreso", anno = .x$year, mes = .x$mes, distritos = FALSE)
    }
)
saveRDS(data_municipios, paste0(OUTPUT_DIR, "municipios.rds"))
cat("  ->", nrow(data_municipios), "filas\n")

# --- Tabla de codigos CCAA (infoelectoral -> INE) ---
saveRDS(codigos_ccaa, paste0(OUTPUT_DIR, "codigos_ccaa.rds"))

cat("Datos descargados en", OUTPUT_DIR, "\n")
