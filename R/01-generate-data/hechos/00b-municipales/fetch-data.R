library(dplyr)
library(purrr)
library(readr)
library(infoelectoral)

OUTPUT_DIR <- "data-raw/hechos/00b-municipales/"
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

# --- Fechas de elecciones municipales ---
fechas <-
    read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE) %>%
    filter(tipo_eleccion == "L") %>%
    transmute(year = as.character(year), mes)

safe_mesas <- possibly(
    function(anno, mes) mesas(tipo_eleccion = "municipales", anno = anno, mes = mes),
    otherwise = tibble()
)

safe_provincias <- possibly(
    function(anno, mes) provincias(tipo_eleccion = "municipales", anno = anno, mes = mes),
    otherwise = tibble()
)

safe_municipios <- possibly(
    function(anno, mes) {
        municipios(tipo_eleccion = "municipales", anno = anno, mes = mes, distritos = FALSE)
    },
    otherwise = tibble()
)

# --- Mesas ---
cat("Descargando mesas...\n")
data_mesas <-
    map(
        split(fechas, seq_len(nrow(fechas))),
        ~ {
            cat("  mesas()", .x$year, .x$mes, "\n")
            safe_mesas(.x$year, .x$mes)
        }
    ) %>%
    list_rbind()
saveRDS(data_mesas, paste0(OUTPUT_DIR, "mesas.rds"))
cat("  ->", nrow(data_mesas), "filas\n")

# --- Provincias ---
cat("Descargando provincias...\n")
data_provincias <-
    map(
        split(fechas, seq_len(nrow(fechas))),
        ~ {
            cat("  provincias()", .x$year, .x$mes, "\n")
            safe_provincias(.x$year, .x$mes)
        }
    ) %>%
    list_rbind()
saveRDS(data_provincias, paste0(OUTPUT_DIR, "provincias.rds"))
cat("  ->", nrow(data_provincias), "filas\n")

# --- Municipios (fallback para elecciones sin mesas) ---
cat("Descargando municipios (fallback)...\n")
data_municipios <-
    map(
        split(fechas, seq_len(nrow(fechas))),
        ~ {
            cat("  municipios()", .x$year, .x$mes, "\n")
            safe_municipios(.x$year, .x$mes)
        }
    ) %>%
    list_rbind()
saveRDS(data_municipios, paste0(OUTPUT_DIR, "municipios.rds"))
cat("  ->", nrow(data_municipios), "filas\n")

# --- Tabla de codigos CCAA (infoelectoral -> INE) ---
saveRDS(codigos_ccaa, paste0(OUTPUT_DIR, "codigos_ccaa.rds"))

cat("Datos descargados en", OUTPUT_DIR, "\n")
