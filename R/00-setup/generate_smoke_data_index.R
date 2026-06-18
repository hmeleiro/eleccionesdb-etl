# Generates a small, deterministic data-raw manifest for CI smoke runs.

source_manifest <- "data-manifest.csv"
output_manifest <- "data-manifest-smoke.csv"

smoke_regions <- Sys.getenv("SMOKE_HECHOS_REGIONS", unset = "17-la-rioja,00-congreso,02-aragon")
smoke_regions <- trimws(strsplit(smoke_regions, ",", fixed = TRUE)[[1]])
smoke_regions <- smoke_regions[nzchar(smoke_regions)]

index <- read.csv(source_manifest, stringsAsFactors = FALSE, check.names = FALSE)

required_exact <- c(
  "eleccionesdb-etl/data-raw/.gitkeep",
  "eleccionesdb-etl/data-raw/elecciones_fuentes.csv",
  "eleccionesdb-etl/data-raw/fechas_elecciones.csv",
  "eleccionesdb-etl/data-raw/nombres_municipios.csv",
  "eleccionesdb-etl/data-raw/partido_id_overrides.csv",
  "eleccionesdb-etl/data-raw/partidos_colores.xlsx",
  "eleccionesdb-etl/data-raw/partidos_recodes.xlsx"
)

required_prefixes <- c(
  "eleccionesdb-etl/data-raw/codigos_territorios/",
  "eleccionesdb-etl/data-raw/representantes/",
  paste0("eleccionesdb-etl/data-raw/hechos/", smoke_regions, "/")
)

starts_with_any <- function(x, prefixes) {
  matches <- vapply(prefixes, function(prefix) startsWith(x, prefix), logical(length(x)))
  rowSums(matches) > 0
}

keep <- index$key %in% required_exact | starts_with_any(index$key, required_prefixes)
smoke_index <- index[keep, , drop = FALSE]

if (!nrow(smoke_index)) {
  stop("No se genero ninguna fila para el manifiesto smoke.", call. = FALSE)
}

write.csv(smoke_index, output_manifest, row.names = FALSE, na = "")

message(
  "Manifiesto smoke generado: ", output_manifest,
  " (", nrow(smoke_index), " objetos; regiones: ",
  paste(smoke_regions, collapse = ", "), ")"
)
