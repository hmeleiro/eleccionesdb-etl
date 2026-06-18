source("R/01-generate-data/hechos/format_mesas_hierarchy_dt.R")

process_large_hechos_dt(
    region_dir = "00c-europeas",
    tipo_eleccion = "E",
    label = "00c-europeas",
    min_year = 1987,
    filter_empty_parties = TRUE
)
