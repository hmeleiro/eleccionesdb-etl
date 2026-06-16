# check_project.R
# Chequeos rapidos para CI: parseo de scripts R y carga del pipeline targets.

message("[ci] Comprobando sintaxis de scripts R")

root_r_files <- list.files(".", pattern = "\\.R$", full.names = TRUE)
r_files <- c(
    root_r_files,
    list.files("R", pattern = "\\.R$", recursive = TRUE, full.names = TRUE)
)
r_files <- sort(unique(r_files))

parse_errors <- list()
for (file in r_files) {
    tryCatch(
        parse(file = file, keep.source = FALSE),
        error = function(e) {
            parse_errors[[file]] <<- conditionMessage(e)
        }
    )
}

if (length(parse_errors) > 0) {
    for (file in names(parse_errors)) {
        message("[ci] Error de parseo en ", file, ": ", parse_errors[[file]])
    }
    stop("Hay scripts R con errores de sintaxis", call. = FALSE)
}

message("[ci] ", length(r_files), " scripts R parseados correctamente")

message("[ci] Comprobando manifiesto de targets")
if (!requireNamespace("targets", quietly = TRUE)) {
    stop("Falta el paquete targets", call. = FALSE)
}

manifest <- targets::tar_manifest(callr_function = NULL)
if (!"name" %in% names(manifest) || nrow(manifest) == 0) {
    stop("El manifiesto de targets esta vacio", call. = FALSE)
}

message("[ci] Targets detectados: ", nrow(manifest))
message("[ci] OK")
