library(dplyr)
library(DBI)
library(readr)
library(data.table)

source("R/utils.R", encoding = "UTF-8")
source("R/tests/validate_tablas_finales.R")

read_rds_dt <- function(path) {
  dt <- readRDS(path)
  data.table::setDT(dt)
  dt
}

info_files <- list.files("data-processed/", recursive = TRUE, full.names = TRUE, pattern = "info")
votos_files <- list.files("data-processed/", recursive = TRUE, full.names = TRUE, pattern = "votos")

nrepresentantes <- get_nrepresentantes()
representantes <- get_representantes()

data.table::setDT(nrepresentantes)
data.table::setDT(representantes)

partidos <- readr::read_csv("tablas-finales/dimensiones/partidos", show_col_types = FALSE, na = "NNNNAAAA")
data.table::setDT(partidos)
partidos[, denominacion_lower := normalize_lower(denominacion)]
partidos[, siglas_lower := normalize_lower(siglas)]
partidos <- partidos[, .(partido_id = id, denominacion_lower, siglas_lower)]

votos <- data.table::rbindlist(lapply(votos_files, read_rds_dt), use.names = TRUE, fill = TRUE)
data.table::setorder(votos, eleccion_id, territorio_id, -votos)
votos[, denominacion_lower := normalize_lower(denominacion)]
votos[, siglas_lower := normalize_lower(siglas)]

# ASIGNAR ID DE PARTIDO A votos
votos[partidos, partido_id := i.partido_id, on = .(denominacion_lower, siglas_lower)]

votos_sin_id <- votos[
  is.na(partido_id),
  .(votos = sum(votos, na.rm = TRUE)),
  by = .(denominacion, siglas)
][order(-votos)]

if (nrow(votos_sin_id) > 0) {
  print(head(as.data.frame(votos_sin_id)))
  stop("Hay partidos sin partido_id asignado en votos.")
}

# ASIGNAR partido_id A REPRESENTANTES EN EL CONTEXTO ELECCION-TERRITORIO
# El Excel histórico no siempre usa las mismas siglas que los resultados
# (p. ej. "COALICION CANARIA" frente a "CC"). Se prioriza coincidencia exacta
# y se recurre a denominación o siglas solo cuando identifican un único partido
# dentro del reparto concreto.
representantes <- match_representantes_to_votos(representantes, votos)

info <- data.table::rbindlist(lapply(info_files, read_rds_dt), use.names = TRUE, fill = TRUE)
data.table::setorder(info, eleccion_id, territorio_id)

# JOIN CON EL NUMERO DE REPRESENTANTES (G, A, L)
if (!"nrepresentantes" %in% names(info)) {
  info[, nrepresentantes := NA_real_]
}

nrepresentantes <- nrepresentantes[, .(
  eleccion_id,
  territorio_id,
  nrepresentantes_lookup = nrepresentantes
)]
info[
  nrepresentantes,
  nrepresentantes_lookup := i.nrepresentantes_lookup,
  on = .(eleccion_id, territorio_id)
]
info[is.na(votos_validos), votos_validos := censo_ine - abstenciones - votos_nulos]
info[, nrepresentantes := data.table::fcoalesce(nrepresentantes, nrepresentantes_lookup)]
info[, nrepresentantes_lookup := NULL]

representantes <- representantes[, .(eleccion_id, territorio_id, partido_id, representantes)]
votos[
  representantes,
  representantes := i.representantes,
  on = .(eleccion_id, territorio_id, partido_id)
]
votos[, c("denominacion_lower", "siglas_lower", "denominacion", "siglas") := NULL]

votos_grouped <- votos[
  ,
  .(
    votos = sum(votos, na.rm = TRUE),
    representantes = sum(representantes, na.rm = TRUE)
  ),
  by = .(eleccion_id, territorio_id, partido_id)
]
data.table::setcolorder(votos_grouped, c("eleccion_id", "territorio_id", "partido_id", "votos", "representantes"))
data.table::setorder(votos_grouped, eleccion_id, territorio_id, partido_id)

rm(votos, partidos, representantes, nrepresentantes)
invisible(gc())

# Asegurar que columnas enteras no tengan decimales (sum() en R convierte integer a double)
int_cols <- intersect(
  c(
    "censo_ine", "participacion_1", "participacion_2", "participacion_3",
    "votos_validos", "abstenciones", "votos_blancos", "votos_nulos", "nrepresentantes"
  ),
  names(info)
)
info[, (int_cols) := lapply(.SD, function(x) as.integer(round(x))), .SDcols = int_cols]

# CHECKS
validate_hechos_info(info, label = "[HECHOS] info")
validate_hechos_votos(votos_grouped, label = "[HECHOS] votos")

data.table::setDF(info)
data.table::setDF(votos_grouped)

saveRDS(info, "tablas-finales/hechos/info.rds")
saveRDS(votos_grouped, "tablas-finales/hechos/votos.rds")

message("[HECHOS] Hechos info y votos generados en tablas-finales/hechos/")
