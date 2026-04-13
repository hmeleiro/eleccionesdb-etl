# run.R
# Entry point for the eleccionesdb ETL pipeline.
#
# Usage:
#   source("run.R")          # loads helpers into your session
#   run_all()                # run entire pipeline (skips up-to-date targets)
#   run_dims()               # run only dimension targets
#   run_hechos()             # run dimensions + all regional hechos
#   run_bind()               # run dimensions + hechos + bind
#   run_writedb()            # run everything up to and including DB write
#   run_export()             # run everything up to and including export
#   show_pipeline()          # visualize the DAG in the viewer
#   targets::tar_outdated()  # see which targets need re-running

library(targets)

run_all <- function() {
    tar_make()
}

run_dims <- function() {
    tar_make(names = starts_with("dim_"))
}

run_hechos <- function() {
    tar_make(names = c(starts_with("dim_"), starts_with("hechos_")))
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

show_pipeline <- function() {
    tar_visnetwork()
}
