source("R/01-generate-data/hechos/format_mesas_hierarchy_dt.R")

process_large_hechos_dt(
    region_dir = "00-congreso",
    tipo_eleccion = "G",
    label = "00-congreso",
    nrepresentantes_col = "n_diputados",
    filter_empty_parties = FALSE
)
