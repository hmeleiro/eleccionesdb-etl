# run.R
# Entry point for the eleccionesdb ETL pipeline.
#
# Usage:
#   source("run.R")          # loads helpers into your session
#   run_all()                # run entire pipeline (skips up-to-date targets)
#   run_dims()               # run only dimension targets
#   run_hechos()             # run dimensions + all regional hechos
#   run_hechos_ordered()     # run regional hechos sequentially in _targets.R order
#   run_bind()               # run dimensions + hechos + bind
#   run_writedb()            # run everything up to and including DB write
#   run_export()             # run everything up to and including export
#   run_export_ordered()     # run export with regional hechos in _targets.R order
#   run_export_pipeline_ordered() # run export + quality with ordered hechos
#   show_pipeline()          # visualize the DAG in the viewer
#   targets::tar_outdated()  # see which targets need re-running

library(targets)

source("R/hechos_regions.R")

tar_make_in_order <- function(target_names) {
    for (target_name in target_names) {
        tar_make(names = tidyselect::all_of(target_name))
    }

    invisible(target_names)
}

run_all <- function() {
    tar_make()
}

run_dims <- function() {
    tar_make(names = starts_with("dim_"))
}

run_hechos <- function() {
    tar_make(names = c(starts_with("dim_"), starts_with("hechos_")))
}

run_hechos_ordered <- function() {
    tar_make(names = starts_with("dim_"))
    tar_make_in_order(hechos_target_names())
}

run_bind <- function() {
    tar_make(names = c(starts_with("dim_"), starts_with("hechos_"), "partidos_sin_id", "bind_hechos"))
}

run_writedb <- function() {
    tar_make(names = c(starts_with("dim_"), starts_with("hechos_"), "partidos_sin_id", "bind_hechos", "writedb"))
}

run_export <- function() {
    tar_make(names = c(starts_with("dim_"), starts_with("hechos_"), "partidos_sin_id", "bind_hechos", "export"))
}

run_export_ordered <- function() {
    run_hechos_ordered()
    tar_make_in_order(c("partidos_sin_id", "bind_hechos", "export"))
}

run_export_pipeline_ordered <- function() {
    run_hechos_ordered()
    tar_make_in_order(c("partidos_sin_id", "bind_hechos", "export", "gen_diagnosticos", "export_calidad"))
}

run_export_calidad <- function() {
    tar_make(names = c(starts_with("dim_"), starts_with("hechos_"), "partidos_sin_id", "bind_hechos", "gen_diagnosticos", "export_calidad"))
}

show_pipeline <- function() {
    tar_visnetwork()
}
