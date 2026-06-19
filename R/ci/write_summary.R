# write_summary.R
# Genera un resumen Markdown del ETL para GitHub Actions y uso local.

library(readr)

count_csv <- function(path) {
    if (!file.exists(path)) return(NA_real_)
    nrow(readr::read_csv(path, show_col_types = FALSE))
}

count_rds <- function(path) {
    if (!file.exists(path)) return(NA_real_)
    nrow(readRDS(path))
}

format_count <- function(x) {
    if (is.na(x)) return("n/a")
    format(x, big.mark = ",", scientific = FALSE)
}

format_size <- function(path) {
    if (!file.exists(path)) return("n/a")
    sprintf("%.1f MB", file.info(path)$size / 1024^2)
}

metrics <- data.frame(
    metrica = c(
        "Elecciones",
        "Territorios",
        "Partidos",
        "Resumen territorial",
        "Votos territoriales",
        "Incidencias calidad"
    ),
    valor = c(
        format_count(count_csv("tablas-finales/dimensiones/elecciones")),
        format_count(count_csv("tablas-finales/dimensiones/territorios")),
        format_count(count_csv("tablas-finales/dimensiones/partidos")),
        format_count(count_rds("tablas-finales/hechos/info.rds")),
        format_count(count_rds("tablas-finales/hechos/votos.rds")),
        format_count(count_csv("docs-site/data/diagnosticos/incidencias.csv"))
    )
)

artifacts <- data.frame(
    artefacto = c("Parquet ZIP", "SQLite ZIP", "SQLite manifest", "CSV ZIP"),
    ruta = c(
        "descargas/eleccionesdb_parquet.zip",
        "descargas/eleccionesdb_sqlite.zip",
        "descargas/eleccionesdb_sqlite.json",
        "descargas/eleccionesdb_csv.zip"
    )
)
artifacts$tamano <- vapply(artifacts$ruta, format_size, character(1))

to_md_table <- function(df) {
    header <- paste0("| ", paste(names(df), collapse = " | "), " |")
    sep <- paste0("| ", paste(rep("---", ncol(df)), collapse = " | "), " |")
    rows <- apply(df, 1, function(row) paste0("| ", paste(row, collapse = " | "), " |"))
    c(header, sep, rows)
}

summary_lines <- c(
    "# Resumen ETL",
    "",
    "## Metricas",
    "",
    to_md_table(metrics),
    "",
    "## Artefactos",
    "",
    to_md_table(artifacts),
    ""
)

summary_file <- Sys.getenv("CI_SUMMARY_FILE", unset = "ci-summary.md")
writeLines(summary_lines, summary_file)

github_summary <- Sys.getenv("GITHUB_STEP_SUMMARY", unset = "")
if (nzchar(github_summary)) {
    cat(paste(summary_lines, collapse = "\n"), file = github_summary, append = TRUE)
    cat("\n", file = github_summary, append = TRUE)
}

message("[ci] Resumen escrito en ", summary_file)
