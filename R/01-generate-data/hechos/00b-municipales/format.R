source("R/01-generate-data/hechos/format_mesas_hierarchy_dt.R")

process_large_hechos_dt(
    region_dir = "00b-municipales",
    tipo_eleccion = "L",
    label = "00b-municipales",
    filter_empty_parties = TRUE
)
