# validate_data_processed.R
#
# Validates intermediate hechos data BEFORE saving to data-processed/ and
# BEFORE the bind step (R/02-clean-and-bind/02-bind-hechos.R).
#
# Checks performed:
#   - Required columns present
#   - No unrecognised / misspelled column names (schema drift)
#   - Key IDs are numeric and non-NA
#   - UNIQUE constraints that the DB will enforce after load
#   - Numeric vote/count columns are non-negative
#   - Logical consistency (e.g. votos_blancos <= votos_validos)
#   - FK integrity against already-built dimension tables
#   - Cross-file consistency: every (eleccion_id, territorio_id) in votos
#     has a matching row in info (same directory/granularity)
#   - Early detection of parties in votos not present in data-processed/partidos
#
# ----- Usage A: inside a processing script (validates in memory) -----
#
#   source("R/tests/validate_data_processed.R")
#
#   # After building the dataframe, before saveRDS():
#   validate_info(info_muni,  label = "00-congreso/info_municipios")
#   validate_votos(votos_muni, label = "00-congreso/votos_municipios")
#
#   # Optional: cross-check that info and votos cover the same keys:
#   validate_info_votos_consistency(info_muni, votos_muni, label = "00-congreso/municipios")
#
#   # Optional: early check for parties not in the partidos dimension:
#   validate_votos_partido_match(votos_muni, label = "00-congreso/votos_municipios")
#
#   saveRDS(info_muni,  "data-processed/hechos/00-congreso/info_municipios.rds")
#   saveRDS(votos_muni, "data-processed/hechos/00-congreso/votos_municipios.rds")
#
# ----- Usage B: batch validation of all data-processed/ files -----
#
#   source("R/tests/validate_data_processed.R")
#   run_data_processed_checks()          # or:
#   Rscript R/tests/validate_data_processed.R

library(dplyr)
library(purrr)
library(readr)

# ---------------------------------------------------------------------------
# Allowed column sets (must match db_schema.sql + canonical intermediate names)
# ---------------------------------------------------------------------------

# resumen_territorial-bound: mandatory keys
INFO_REQUIRED_COLS <- c("eleccion_id", "territorio_id")

# All recognised column names for info files (superset; extra cols = warning)
INFO_ALLOWED_COLS <- c(
    "eleccion_id", "territorio_id",
    "censo_ine",
    "participacion_1", "participacion_2", "participacion_3",
    "votos_validos", "abstenciones",
    "votos_blancos", "votos_nulos",
    "nrepresentantes"
)

# Known misspellings / legacy names that should be renamed before the bind
INFO_KNOWN_TYPOS <- c(
    "abstencion" = "abstenciones", # seen in 07-cyl
    "validos" = "votos_validos", # seen in 07-cyl
    "abstenciones_1" = "abstenciones"
)

# votos_territoriales-bound: mandatory keys
VOTOS_REQUIRED_COLS <- c("eleccion_id", "territorio_id", "siglas", "denominacion", "votos")

# All recognised column names for votos files
VOTOS_ALLOWED_COLS <- c(
    "eleccion_id", "territorio_id",
    "siglas", "denominacion",
    "votos", "representantes"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

stop_if_not <- function(cond, msg) {
    if (!isTRUE(cond)) stop(msg, call. = FALSE)
}

stop_if_any <- function(cond, msg) {
    if (any(cond, na.rm = TRUE)) stop(msg, call. = FALSE)
}

warn_if_any <- function(cond, msg) {
    if (any(cond, na.rm = TRUE)) warning(msg, call. = FALSE)
}

file_label <- function(path) {
    parts <- tail(strsplit(path, "/|\\\\")[[1]], 2)
    paste(parts, collapse = "/")
}

# ---------------------------------------------------------------------------
# Core validators — work on dataframes (call these from processing scripts)
# ---------------------------------------------------------------------------

#' Validate an info (resumen_territorial) dataframe before saving.
#'
#' @param df    Dataframe to validate.
#' @param label Short label used in error/warning messages, e.g. "07-cyl/info".
#'
#' @return Invisibly returns df on success; stops on any hard error.
#'
#' @examples
#' # In a processing script, before saveRDS():
#' source("R/tests/validate_data_processed.R")
#' validate_info(info_muni, label = "00-congreso/info_municipios")
#' saveRDS(info_muni, "data-processed/hechos/00-congreso/info_municipios.rds")
validate_info <- function(df, label = "info") {
    lbl <- label

    # 1. Non-empty
    stop_if_not(
        nrow(df) > 0,
        sprintf("[%s] El dataframe está vacío (0 filas)", lbl)
    )

    # 2. Required columns exist
    missing <- setdiff(INFO_REQUIRED_COLS, names(df))
    stop_if_not(
        length(missing) == 0,
        sprintf("[%s] Faltan columnas requeridas: %s", lbl, paste(missing, collapse = ", "))
    )

    # 3. Flag known misspellings (typos that break the bind silently)
    found_typos <- intersect(names(df), names(INFO_KNOWN_TYPOS))
    if (length(found_typos) > 0) {
        corrections <- paste(
            sprintf("'%s' -> '%s'", found_typos, INFO_KNOWN_TYPOS[found_typos]),
            collapse = ", "
        )
        stop(sprintf(
            "[%s] Nombres de columna incorrectos (renombrar antes del bind): %s",
            lbl, corrections
        ), call. = FALSE)
    }

    # 4. Warn about any other unrecognised columns
    extra <- setdiff(names(df), INFO_ALLOWED_COLS)
    if (length(extra) > 0) {
        warning(sprintf(
            "[%s] Columnas no reconocidas (se ignorarán en el bind): %s",
            lbl, paste(extra, collapse = ", ")
        ), call. = FALSE)
    }

    # 5. Key ID columns: numeric and non-NA
    stop_if_not(
        is.numeric(df$eleccion_id),
        sprintf("[%s] eleccion_id debe ser numérico", lbl)
    )
    stop_if_any(
        is.na(df$eleccion_id),
        sprintf("[%s] eleccion_id contiene NAs", lbl)
    )
    stop_if_not(
        is.numeric(df$territorio_id),
        sprintf("[%s] territorio_id debe ser numérico", lbl)
    )
    stop_if_any(
        is.na(df$territorio_id),
        sprintf("[%s] territorio_id contiene NAs", lbl)
    )
    stop_if_any(
        df$eleccion_id <= 0,
        sprintf("[%s] eleccion_id contiene valores <= 0", lbl)
    )
    stop_if_any(
        df$territorio_id <= 0,
        sprintf("[%s] territorio_id contiene valores <= 0", lbl)
    )

    # 6. UNIQUE (eleccion_id, territorio_id)  [maps to uq_resumen_eleccion_territorio]
    dupes <- duplicated(df[, c("eleccion_id", "territorio_id")])
    stop_if_any(
        dupes,
        sprintf(
            "[%s] Combinación (eleccion_id, territorio_id) no es única (%d duplicados). Viola UNIQUE constraint en resumen_territorial.",
            lbl, sum(dupes)
        )
    )

    # 7. Numeric vote/count columns must be non-negative
    numeric_cols <- intersect(
        c(
            "censo_ine", "participacion_1", "participacion_2", "participacion_3",
            "votos_validos", "abstenciones", "votos_blancos", "votos_nulos", "nrepresentantes"
        ),
        names(df)
    )
    for (col in numeric_cols) {
        stop_if_not(
            is.numeric(df[[col]]),
            sprintf("[%s] %s debe ser numérico", lbl, col)
        )
        neg <- !is.na(df[[col]]) & df[[col]] < 0
        stop_if_any(
            neg,
            sprintf("[%s] %s contiene %d valores negativos", lbl, col, sum(neg))
        )
    }

    # 8. Logical consistency: votos_blancos <= votos_validos
    if (all(c("votos_blancos", "votos_validos") %in% names(df))) {
        check_rows <- !is.na(df$votos_blancos) & !is.na(df$votos_validos)
        if (any(check_rows)) {
            stop_if_any(
                check_rows & df$votos_blancos > df$votos_validos,
                sprintf("[%s] votos_blancos supera votos_validos en algunas filas", lbl)
            )
        }
    }

    # 9. Logical consistency: abstenciones + votos_validos <= censo_ine (warn only)
    if (all(c("censo_ine", "abstenciones", "votos_validos") %in% names(df))) {
        check_rows <- !is.na(df$censo_ine) & !is.na(df$abstenciones) & !is.na(df$votos_validos)
        if (any(check_rows)) {
            over <- check_rows & (df$abstenciones + df$votos_validos > df$censo_ine)
            warn_if_any(
                over,
                sprintf(
                    "[%s] %d filas con abstenciones + votos_validos > censo_ine (posible error de datos o metodología)",
                    lbl, sum(over)
                )
            )
        }
    }

    # 10. votos_nulos <= votos_validos (warn only; some methodologies exclude nulos from validos)
    if (all(c("votos_nulos", "votos_validos") %in% names(df))) {
        check_rows <- !is.na(df$votos_nulos) & !is.na(df$votos_validos)
        if (any(check_rows)) {
            warn_if_any(
                check_rows & df$votos_nulos > df$votos_validos,
                sprintf("[%s] votos_nulos supera votos_validos en algunas filas (revisar metodología)", lbl)
            )
        }
    }

    message(sprintf("  [OK] %s (%d filas)", lbl, nrow(df)))
    invisible(df)
}

#' Validate a votos (votos_territoriales) dataframe before saving.
#'
#' @param df    Dataframe to validate.
#' @param label Short label used in error/warning messages, e.g. "07-cyl/votos".
#'
#' @return Invisibly returns df on success; stops on any hard error.
#'
#' @examples
#' source("R/tests/validate_data_processed.R")
#' validate_votos(votos_muni, label = "00-congreso/votos_municipios")
#' saveRDS(votos_muni, "data-processed/hechos/00-congreso/votos_municipios.rds")
validate_votos <- function(df, label = "votos") {
    lbl <- label

    # 1. Non-empty
    stop_if_not(
        nrow(df) > 0,
        sprintf("[%s] El dataframe está vacío (0 filas)", lbl)
    )

    # 2. Required columns
    missing <- setdiff(VOTOS_REQUIRED_COLS, names(df))
    stop_if_not(
        length(missing) == 0,
        sprintf("[%s] Faltan columnas requeridas: %s", lbl, paste(missing, collapse = ", "))
    )

    # 3. Warn about unrecognised columns
    extra <- setdiff(names(df), VOTOS_ALLOWED_COLS)
    if (length(extra) > 0) {
        warning(sprintf(
            "[%s] Columnas no reconocidas (se ignorarán en el bind): %s",
            lbl, paste(extra, collapse = ", ")
        ), call. = FALSE)
    }

    # 4. Key ID columns: numeric and non-NA
    stop_if_not(
        is.numeric(df$eleccion_id),
        sprintf("[%s] eleccion_id debe ser numérico", lbl)
    )
    stop_if_any(
        is.na(df$eleccion_id),
        sprintf("[%s] eleccion_id contiene NAs", lbl)
    )
    stop_if_not(
        is.numeric(df$territorio_id),
        sprintf("[%s] territorio_id debe ser numérico", lbl)
    )
    stop_if_any(
        is.na(df$territorio_id),
        sprintf("[%s] territorio_id contiene NAs", lbl)
    )
    stop_if_any(
        df$eleccion_id <= 0,
        sprintf("[%s] eleccion_id contiene valores <= 0", lbl)
    )
    stop_if_any(
        df$territorio_id <= 0,
        sprintf("[%s] territorio_id contiene valores <= 0", lbl)
    )

    # 5. Cada fila debe tener al menos un identificador de partido no-NA
    both_na <- is.na(df$siglas) & is.na(df$denominacion)
    stop_if_any(
        both_na,
        sprintf("[%s] %d filas con siglas y denominacion ambas NA (partido no identificable)", lbl, sum(both_na))
    )

    # 6. votos: numérico y no negativo
    stop_if_not(
        is.numeric(df$votos),
        sprintf("[%s] votos debe ser numérico", lbl)
    )
    neg_votos <- !is.na(df$votos) & df$votos < 0
    stop_if_any(
        neg_votos,
        sprintf("[%s] votos contiene %d valores negativos", lbl, sum(neg_votos))
    )

    # 7. representantes: numérico y no negativo (si existe)
    if ("representantes" %in% names(df)) {
        stop_if_not(
            is.numeric(df$representantes),
            sprintf("[%s] representantes debe ser numérico", lbl)
        )
        neg_rep <- !is.na(df$representantes) & df$representantes < 0
        stop_if_any(
            neg_rep,
            sprintf("[%s] representantes contiene %d valores negativos", lbl, sum(neg_rep))
        )
    }

    # 8. UNIQUE (eleccion_id, territorio_id, siglas, denominacion)
    #    Pre-bind proxy for uq_votos_eleccion_territorio_partido
    key_cols <- c("eleccion_id", "territorio_id", "siglas", "denominacion")
    dupes <- duplicated(df[, key_cols])
    stop_if_any(
        dupes,
        sprintf(
            "[%s] Combinación (eleccion_id, territorio_id, siglas, denominacion) no es única (%d duplicados). Producirá duplicados al asignar partido_id.",
            lbl, sum(dupes)
        )
    )

    message(sprintf("  [OK] %s (%d filas)", lbl, nrow(df)))
    invisible(df)
}

#' Check that info and votos cover the same (eleccion_id, territorio_id) pairs.
#'
#' @param info_df  Info dataframe.
#' @param votos_df Votos dataframe.
#' @param label    Label for messages, e.g. "07-cyl/municipios".
validate_info_votos_consistency <- function(info_df, votos_df, label = "") {
    lbl <- label

    info_keys <- paste(info_df$eleccion_id, info_df$territorio_id, sep = "-")
    votos_keys <- paste(votos_df$eleccion_id, votos_df$territorio_id, sep = "-")

    orphan_votos <- setdiff(unique(votos_keys), unique(info_keys))
    warn_if_any(
        length(orphan_votos) > 0,
        sprintf(
            "[%s] %d combinaciones (eleccion_id, territorio_id) en votos sin fila en info: %s",
            lbl, length(orphan_votos),
            paste(head(orphan_votos, 5), collapse = ", ")
        )
    )

    orphan_info <- setdiff(unique(info_keys), unique(votos_keys))
    warn_if_any(
        length(orphan_info) > 0,
        sprintf(
            "[%s] %d combinaciones (eleccion_id, territorio_id) en info sin votos asociados: %s",
            lbl, length(orphan_info),
            paste(head(orphan_info, 5), collapse = ", ")
        )
    )

    invisible(NULL)
}

#' Check that all (siglas, denominacion) pairs in votos exist in the partidos
#' dimension (data-processed/partidos). Emits a warning (not a stop) listing
#' unmatched parties sorted by total votes.
#'
#' @param df    Votos dataframe (must contain siglas, denominacion, votos).
#' @param label Short label for messages.
#' @param partidos_path Path to the partidos CSV.
validate_votos_partido_match <- function(df, label = "votos",
                                         partidos_path = "data-processed/partidos") {
    lbl <- label

    if (!file.exists(partidos_path)) {
        message(sprintf("  [SKIP] %s: %s no encontrado, omitiendo check de partidos", lbl, partidos_path))
        return(invisible(NULL))
    }

    norm_lower <- function(x) tolower(trimws(gsub("\\s+", " ", x)))

    partidos <- read_csv(partidos_path, show_col_types = FALSE, na = "NNNNAAAA") %>%
        mutate(across(c(denominacion, siglas), norm_lower, .names = "{.col}_lower")) %>%
        select(denominacion_lower, siglas_lower)

    votos_with_key <- df %>%
        mutate(across(c(denominacion, siglas), norm_lower, .names = "{.col}_lower"))

    sin_match <- votos_with_key %>%
        anti_join(partidos, by = c("denominacion_lower", "siglas_lower")) %>%
        group_by(denominacion, siglas) %>%
        summarise(votos = sum(votos, na.rm = TRUE), .groups = "drop") %>%
        arrange(desc(votos))

    if (nrow(sin_match) > 0) {
        msg <- sprintf(
            "[%s] %d partidos en votos sin match en %s (top por votos):",
            lbl, nrow(sin_match), partidos_path
        )
        warning(msg, call. = FALSE)
        print(head(sin_match, 20))
    } else {
        message(sprintf("  [OK] %s: todos los partidos tienen match en %s", lbl, partidos_path))
    }

    invisible(sin_match)
}

# ---------------------------------------------------------------------------
# File-based wrappers — used by run_data_processed_checks() (batch mode)
# ---------------------------------------------------------------------------

check_info_file <- function(path) {
    validate_info(readRDS(path), label = file_label(path))
}

check_votos_file <- function(path) {
    validate_votos(readRDS(path), label = file_label(path))
}

check_info_votos_consistency <- function(info_path, votos_path) {
    validate_info_votos_consistency(
        readRDS(info_path),
        readRDS(votos_path),
        label = paste(file_label(dirname(info_path)),
            basename(tools::file_path_sans_ext(info_path)),
            sep = "/"
        )
    )
}

# ---------------------------------------------------------------------------
# FK integrity against dimension tables
# ---------------------------------------------------------------------------

check_fks_against_dimensions <- function(
    info_files,
    votos_files,
    elecciones_path = "tablas-finales/dimensiones/elecciones",
    territorios_path = "tablas-finales/dimensiones/territorios") {
    dims_present <- file.exists(elecciones_path) && file.exists(territorios_path)
    if (!dims_present) {
        message("[FK] Tablas de dimensiones no encontradas; omitiendo checks de FK.")
        message("     Genera primero las dimensiones con R/01-generate-data/dimensiones/.")
        return(invisible(NULL))
    }

    elecciones <- read_csv(elecciones_path, show_col_types = FALSE)
    territorios <- read_csv(territorios_path, show_col_types = FALSE)

    all_info <- map(info_files, readRDS) |> bind_rows()
    all_votos <- map(votos_files, readRDS) |> bind_rows()

    # info: eleccion_id -> elecciones$id
    bad <- setdiff(unique(all_info$eleccion_id), elecciones$id)
    stop_if_not(
        length(bad) == 0,
        sprintf(
            "FK: info$eleccion_id tiene %d valores no presentes en elecciones$id: %s",
            length(bad), paste(bad, collapse = ", ")
        )
    )

    # info: territorio_id -> territorios$id
    bad <- setdiff(unique(all_info$territorio_id), territorios$id)
    stop_if_not(
        length(bad) == 0,
        sprintf(
            "FK: info$territorio_id tiene %d valores no presentes en territorios$id: %s",
            length(bad), paste(head(bad, 10), collapse = ", ")
        )
    )

    # votos: eleccion_id -> elecciones$id
    bad <- setdiff(unique(all_votos$eleccion_id), elecciones$id)
    stop_if_not(
        length(bad) == 0,
        sprintf(
            "FK: votos$eleccion_id tiene %d valores no presentes en elecciones$id: %s",
            length(bad), paste(bad, collapse = ", ")
        )
    )

    # votos: territorio_id -> territorios$id
    bad <- setdiff(unique(all_votos$territorio_id), territorios$id)
    stop_if_not(
        length(bad) == 0,
        sprintf(
            "FK: votos$territorio_id tiene %d valores no presentes en territorios$id: %s",
            length(bad), paste(head(bad, 10), collapse = ", ")
        )
    )

    message("[FK] Foreign keys OK.")
}

# ---------------------------------------------------------------------------
# Global consistency across all files after bind
# ---------------------------------------------------------------------------

check_global_uniqueness <- function(info_files, votos_files) {
    all_info <- map(info_files, readRDS) |> bind_rows()

    # After list_rbind(), the bound info must still satisfy UNIQUE(eleccion_id, territorio_id)
    dupes <- duplicated(all_info[, c("eleccion_id", "territorio_id")])
    stop_if_any(
        dupes,
        sprintf(
            "GLOBAL: La unión de todos los archivos info produce %d filas con (eleccion_id, territorio_id) duplicados. Viola UNIQUE en resumen_territorial.",
            sum(dupes)
        )
    )
    message(sprintf("  [OK] UNIQUE global info: %d filas, sin duplicados", nrow(all_info)))
}

# ---------------------------------------------------------------------------
# Summary: coverage report
# ---------------------------------------------------------------------------

print_coverage_summary <- function(info_files, votos_files) {
    message("\n--- Cobertura de datos procesados ---")
    all_info <- map(info_files, readRDS) |>
        bind_rows() |>
        distinct(eleccion_id, territorio_id)

    all_votos <- map(votos_files, readRDS) |>
        bind_rows() |>
        distinct(eleccion_id, territorio_id)

    message(sprintf(
        "  Archivos info : %d  |  Pares únicos (eleccion, territorio): %d",
        length(info_files), nrow(all_info)
    ))
    message(sprintf(
        "  Archivos votos: %d  |  Pares únicos (eleccion, territorio): %d",
        length(votos_files), nrow(all_votos)
    ))
    message(sprintf(
        "  Elecciones cubiertas (info) : %s",
        paste(sort(unique(map(info_files, readRDS) |> bind_rows() |> pull(eleccion_id))),
            collapse = ", "
        )
    ))
    message("-------------------------------------\n")
}

# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

run_data_processed_checks <- function(base_dir = "data-processed/hechos") {
    info_files <- list.files(base_dir, recursive = TRUE, full.names = TRUE, pattern = "info")
    votos_files <- list.files(base_dir, recursive = TRUE, full.names = TRUE, pattern = "votos")

    message(sprintf(
        "[VALIDACION] %d archivos info, %d archivos votos encontrados en '%s'",
        length(info_files), length(votos_files), base_dir
    ))

    # ---- Per-file checks -----------------------------------------------------
    message("\n[VALIDACION] Comprobando archivos info...")
    walk(info_files, check_info_file)

    message("\n[VALIDACION] Comprobando archivos votos...")
    walk(votos_files, check_votos_file)

    # ---- Cross-file consistency (same directory/granularity) -----------------
    message("\n[VALIDACION] Consistencia info <-> votos por directorio...")
    all_dirs <- union(dirname(info_files), dirname(votos_files))

    for (d in sort(unique(all_dirs))) {
        dir_info <- info_files[dirname(info_files) == d]
        dir_votos <- votos_files[dirname(votos_files) == d]

        if (length(dir_info) == 0 || length(dir_votos) == 0) next

        if (length(dir_info) == 1 && length(dir_votos) == 1) {
            # Simple 1-to-1 case (most CCAA)
            check_info_votos_consistency(dir_info, dir_votos)
        } else {
            # Multiple granularities in the same directory (e.g. 00-congreso)
            # Match by shared name suffix: info_prov <-> votos_prov, etc.
            for (i_path in dir_info) {
                suffix <- sub("^info_?", "", tools::file_path_sans_ext(basename(i_path)))
                if (nchar(suffix) == 0) suffix <- "." # handle plain "info.rds"
                matched_votos <- dir_votos[grepl(suffix, basename(dir_votos))]
                if (length(matched_votos) == 1) {
                    check_info_votos_consistency(i_path, matched_votos)
                }
            }
        }
        message(sprintf("  [OK] %s", basename(d)))
    }

    # ---- Global uniqueness after hypothetical bind ---------------------------
    message("\n[VALIDACION] Unicidad global tras bind de info...")
    check_global_uniqueness(info_files, votos_files)

    # ---- FK checks -----------------------------------------------------------
    message("\n[VALIDACION] Foreign keys contra tablas de dimensiones...")
    check_fks_against_dimensions(info_files, votos_files)

    # ---- Coverage summary ----------------------------------------------------
    print_coverage_summary(info_files, votos_files)

    message("[VALIDACION] Todos los checks de data-processed completados.")
}

# ---------------------------------------------------------------------------
# Entry point when run as a script
# ---------------------------------------------------------------------------

if (sys.nframe() == 0) {
    run_data_processed_checks()
}
