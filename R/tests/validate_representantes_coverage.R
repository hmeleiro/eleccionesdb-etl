# validate_representantes_coverage.R
#
# Audita la cobertura de representantes en las tablas finales y en los cuatro
# Excel manuales. Las incidencias no detienen el pipeline: se resumen mediante
# warnings y se exportan como listas de trabajo.

library(data.table)
library(readxl)
library(writexl)

REPRESENTANTES_PENDING_PATH <- "data-processed/representantes_pending.xlsx"

REPRESENTANTES_RAW_PATHS <- c(
  nrepresentantes_prov = "data-raw/representantes/nrepresentantes_prov.xlsx",
  nrepresentantes_muni = "data-raw/representantes/nrepresentantes_muni.xlsx",
  representantes_prov = "data-raw/representantes/representantes_prov.xlsx",
  representantes_muni = "data-raw/representantes/representantes_muni.xlsx"
)

normalize_representantes_raw_paths <- function(raw_paths) {
  raw_paths <- as.character(raw_paths)

  if (is.null(names(raw_paths)) || any(names(raw_paths) == "")) {
    if (length(raw_paths) != length(REPRESENTANTES_RAW_PATHS)) {
      stop(
        sprintf(
          "Se esperaban %d rutas de Excel manuales y llegaron %d.",
          length(REPRESENTANTES_RAW_PATHS),
          length(raw_paths)
        ),
        call. = FALSE
      )
    }
    names(raw_paths) <- names(REPRESENTANTES_RAW_PATHS)
  }

  raw_paths
}

pad_coverage_code <- function(x, width) {
  x <- as.character(x)
  is_integer <- !is.na(x) & grepl("^[0-9]+$", x)
  x[is_integer] <- sprintf(paste0("%0", width, "d"), as.integer(x[is_integer]))
  x
}

normalize_coverage_keys <- function(df) {
  dt <- data.table::as.data.table(data.table::copy(df))
  widths <- c(
    year = 4L,
    mes = 2L,
    codigo_ccaa = 2L,
    codigo_provincia = 2L,
    codigo_municipio = 3L
  )

  for (column in intersect(names(widths), names(dt))) {
    data.table::set(dt, j = column, value = pad_coverage_code(dt[[column]], widths[[column]]))
  }
  if ("codigo_circunscripcion" %in% names(dt)) {
    dt[, codigo_circunscripcion := as.character(codigo_circunscripcion)]
  }
  if ("tipo_eleccion" %in% names(dt)) {
    dt[, tipo_eleccion := as.character(tipo_eleccion)]
  }
  dt
}

build_reparto_universe <- function(info, elecciones, territorios) {
  info_dt <- data.table::as.data.table(data.table::copy(info))
  elec_dt <- normalize_coverage_keys(elecciones)[
    , .(
      eleccion_id = as.integer(id),
      tipo_eleccion,
      year,
      mes
    )
  ]
  terr_dt <- normalize_coverage_keys(territorios)[
    , .(
      territorio_id = as.integer(id),
      tipo_territorio = tipo,
      codigo_ccaa,
      codigo_provincia,
      codigo_circunscripcion,
      codigo_municipio
    )
  ]

  info_dt[, `:=`(
    eleccion_id = as.integer(eleccion_id),
    territorio_id = as.integer(territorio_id)
  )]
  enriched <- merge(info_dt, elec_dt, by = "eleccion_id", all.x = TRUE)
  enriched <- merge(enriched, terr_dt, by = "territorio_id", all.x = TRUE)
  enriched <- enriched[tipo_eleccion %chin% c("G", "A", "L")]

  ga <- enriched[
    tipo_eleccion %chin% c("G", "A") &
      tipo_territorio %chin% c("provincia", "circunscripcion")
  ]
  ga[, tiene_circunscripcion := any(tipo_territorio == "circunscripcion"),
     by = .(eleccion_id, codigo_provincia)]
  ga <- ga[
    (tiene_circunscripcion & tipo_territorio == "circunscripcion") |
      (!tiene_circunscripcion & tipo_territorio == "provincia")
  ]
  ga[, `:=`(
    nivel_reparto = "prov",
    codigo_circunscripcion_salida = data.table::fifelse(
      tipo_territorio == "circunscripcion",
      codigo_circunscripcion,
      codigo_provincia
    )
  )]

  locales <- enriched[tipo_eleccion == "L" & tipo_territorio == "municipio"]
  locales[, `:=`(
    nivel_reparto = "muni",
    codigo_circunscripcion_salida = codigo_circunscripcion
  )]

  universe <- data.table::rbindlist(list(ga, locales), use.names = TRUE, fill = TRUE)[
    , .(
      eleccion_id,
      territorio_id,
      year,
      mes,
      tipo_eleccion,
      codigo_ccaa,
      codigo_provincia,
      codigo_circunscripcion = codigo_circunscripcion_salida,
      codigo_municipio,
      nivel_reparto,
      nrepresentantes
    )
  ]
  data.table::setorder(
    universe,
    year, mes, tipo_eleccion, codigo_ccaa, codigo_provincia,
    codigo_circunscripcion, codigo_municipio
  )
  unique(universe, by = c("eleccion_id", "territorio_id"))
}

prepare_relevant_votes <- function(votos, universe, partidos) {
  votos_dt <- data.table::as.data.table(data.table::copy(votos))
  votos_dt[, `:=`(
    eleccion_id = as.integer(eleccion_id),
    territorio_id = as.integer(territorio_id),
    partido_id = as.integer(partido_id)
  )]
  keys <- unique(universe[, .(eleccion_id, territorio_id)])
  relevant <- votos_dt[keys, on = .(eleccion_id, territorio_id), nomatch = 0L]

  partidos_dt <- data.table::as.data.table(data.table::copy(partidos))[
    , .(
      partido_id = as.integer(id),
      denominacion = as.character(denominacion),
      siglas = as.character(siglas)
    )
  ]
  relevant[partidos_dt, `:=`(
    denominacion = i.denominacion,
    siglas = i.siglas
  ), on = "partido_id"]
  relevant
}

audit_final_representantes <- function(universe, relevant_votes) {
  totals <- relevant_votes[
    , .(representantes_total = sum(representantes, na.rm = TRUE)),
    by = .(eleccion_id, territorio_id)
  ]
  result <- merge(
    data.table::copy(universe),
    totals,
    by = c("eleccion_id", "territorio_id"),
    all.x = TRUE
  )
  result[is.na(representantes_total), representantes_total := 0]
  result[, `:=`(
    nrepresentantes_invalido = is.na(nrepresentantes) | nrepresentantes <= 0,
    representantes_ausentes = representantes_total <= 0,
    representantes_descuadrados = !is.na(nrepresentantes) &
      nrepresentantes > 0 & representantes_total != nrepresentantes
  )]
  result
}

summarize_nrepresentantes_workbook <- function(df, key_cols) {
  dt <- normalize_coverage_keys(df)
  dt[
    , .(nrepresentantes_excel = {
      valid <- nrepresentantes[!is.na(nrepresentantes) & nrepresentantes > 0]
      if (length(valid) == 0L) NA_real_ else valid[[1L]]
    }),
    by = key_cols
  ]
}

summarize_representantes_workbook <- function(df, key_cols) {
  dt <- normalize_coverage_keys(df)
  dt[
    , .(representantes_excel = sum(representantes, na.rm = TRUE)),
    by = key_cols
  ]
}

build_pending_sheets <- function(
    universe,
    relevant_votes,
    nrepresentantes_prov,
    nrepresentantes_muni,
    representantes_prov,
    representantes_muni) {
  prov_keys <- c(
    "year", "mes", "tipo_eleccion", "codigo_ccaa",
    "codigo_provincia", "codigo_circunscripcion"
  )
  muni_keys <- c(
    "year", "mes", "tipo_eleccion", "codigo_ccaa",
    "codigo_provincia", "codigo_municipio"
  )
  rep_prov_keys <- c(
    "year", "mes", "tipo_eleccion", "codigo_ccaa", "codigo_circunscripcion"
  )

  expected_prov <- unique(universe[nivel_reparto == "prov", c(prov_keys, "nrepresentantes"), with = FALSE])
  expected_muni <- unique(universe[nivel_reparto == "muni", c(muni_keys, "nrepresentantes"), with = FALSE])

  nrep_prov_summary <- summarize_nrepresentantes_workbook(nrepresentantes_prov, prov_keys)
  nrep_muni_summary <- summarize_nrepresentantes_workbook(nrepresentantes_muni, muni_keys)

  checked_nrep_prov <- merge(expected_prov, nrep_prov_summary, by = prov_keys, all.x = TRUE)
  checked_nrep_muni <- merge(expected_muni, nrep_muni_summary, by = muni_keys, all.x = TRUE)

  pending_nrep_prov <- checked_nrep_prov[
    is.na(nrepresentantes_excel) | nrepresentantes_excel <= 0,
    ..prov_keys
  ]
  pending_nrep_muni <- checked_nrep_muni[
    is.na(nrepresentantes_excel) | nrepresentantes_excel <= 0,
    ..muni_keys
  ]

  expected_rep_prov <- merge(
    expected_prov,
    nrep_prov_summary,
    by = prov_keys,
    all.x = TRUE
  )
  expected_rep_prov[, nrepresentantes_esperados := data.table::fcoalesce(
    as.numeric(nrepresentantes),
    as.numeric(nrepresentantes_excel)
  )]
  expected_rep_prov[, codigo_provincia := NULL]

  expected_rep_muni <- merge(
    expected_muni,
    nrep_muni_summary,
    by = muni_keys,
    all.x = TRUE
  )
  expected_rep_muni[, nrepresentantes_esperados := data.table::fcoalesce(
    as.numeric(nrepresentantes),
    as.numeric(nrepresentantes_excel)
  )]

  rep_prov_summary <- summarize_representantes_workbook(representantes_prov, rep_prov_keys)
  rep_muni_summary <- summarize_representantes_workbook(representantes_muni, muni_keys)

  checked_rep_prov <- merge(expected_rep_prov, rep_prov_summary, by = rep_prov_keys, all.x = TRUE)
  checked_rep_muni <- merge(expected_rep_muni, rep_muni_summary, by = muni_keys, all.x = TRUE)

  affected_rep_prov <- checked_rep_prov[
    is.na(representantes_excel) |
      representantes_excel <= 0 |
      (!is.na(nrepresentantes_esperados) &
         nrepresentantes_esperados > 0 &
         representantes_excel != nrepresentantes_esperados),
    ..rep_prov_keys
  ]
  affected_rep_muni <- checked_rep_muni[
    is.na(representantes_excel) |
      representantes_excel <= 0 |
      (!is.na(nrepresentantes_esperados) &
         nrepresentantes_esperados > 0 &
         representantes_excel != nrepresentantes_esperados),
    ..muni_keys
  ]

  party_context <- relevant_votes[
    universe,
    on = .(eleccion_id, territorio_id),
    nomatch = 0L
  ]
  party_context <- party_context[
    , .(
      year,
      mes,
      tipo_eleccion,
      codigo_ccaa,
      codigo_provincia,
      codigo_circunscripcion,
      codigo_municipio,
      nivel_reparto,
      denominacion,
      siglas
    )
  ]

  pending_rep_prov <- party_context[nivel_reparto == "prov"][
    affected_rep_prov,
    on = rep_prov_keys,
    nomatch = 0L
  ][
    , .(year, mes, tipo_eleccion, codigo_ccaa, codigo_circunscripcion, denominacion, siglas)
  ]
  pending_rep_muni <- party_context[nivel_reparto == "muni"][
    affected_rep_muni,
    on = muni_keys,
    nomatch = 0L
  ][
    , .(year, mes, tipo_eleccion, codigo_ccaa, codigo_provincia, codigo_municipio, denominacion, siglas)
  ]

  sheets <- list(
    representantes_prov = unique(pending_rep_prov),
    representantes_muni = unique(pending_rep_muni),
    nrepresentantes_prov = unique(pending_nrep_prov),
    nrepresentantes_muni = unique(pending_nrep_muni)
  )
  lapply(sheets, function(sheet) {
    data.table::setorderv(sheet, names(sheet))
    data.frame(sheet, check.names = FALSE)
  })
}

format_coverage_example <- function(df) {
  if (nrow(df) == 0L) return("ninguno")
  examples <- head(df, 5L)
  paste(
    sprintf(
      "%s-%s %s %s/%s/%s",
      examples$year,
      examples$mes,
      examples$tipo_eleccion,
      examples$codigo_provincia,
      examples$codigo_circunscripcion,
      examples$codigo_municipio
    ),
    collapse = "; "
  )
}

warn_representantes_coverage <- function(final_coverage, sheets) {
  nrep_bad <- final_coverage[nrepresentantes_invalido == TRUE]
  rep_absent <- final_coverage[representantes_ausentes == TRUE]
  rep_mismatch <- final_coverage[representantes_descuadrados == TRUE]
  pending_counts <- vapply(sheets, nrow, integer(1))

  if (nrow(nrep_bad) + nrow(rep_absent) + nrow(rep_mismatch) > 0L) {
    warning(sprintf(
      paste0(
        "[representantes] Cobertura final incompleta: %d repartos sin nrepresentantes válido, ",
        "%d sin representantes y %d con suma descuadrada. Ejemplos: %s"
      ),
      nrow(nrep_bad),
      nrow(rep_absent),
      nrow(rep_mismatch),
      format_coverage_example(data.table::rbindlist(
        list(nrep_bad, rep_absent, rep_mismatch),
        use.names = TRUE,
        fill = TRUE
      ))
    ), call. = FALSE)
  }

  if (sum(pending_counts) > 0L) {
    warning(sprintf(
      "[representantes] Pendientes Excel: %s",
      paste(sprintf("%s=%d", names(pending_counts), pending_counts), collapse = ", ")
    ), call. = FALSE)
  }
}

audit_representantes_coverage <- function(
    info,
    votos,
    elecciones,
    territorios,
    partidos,
    nrepresentantes_prov,
    nrepresentantes_muni,
    representantes_prov,
    representantes_muni,
    warn = TRUE) {
  universe <- build_reparto_universe(info, elecciones, territorios)
  relevant_votes <- prepare_relevant_votes(votos, universe, partidos)
  final_coverage <- audit_final_representantes(universe, relevant_votes)
  sheets <- build_pending_sheets(
    universe,
    relevant_votes,
    nrepresentantes_prov,
    nrepresentantes_muni,
    representantes_prov,
    representantes_muni
  )

  if (isTRUE(warn)) {
    warn_representantes_coverage(final_coverage, sheets)
  }

  list(
    universe = universe,
    final_coverage = final_coverage,
    sheets = sheets
  )
}

validate_representantes_coverage <- function(
    output_path = REPRESENTANTES_PENDING_PATH,
    raw_paths = REPRESENTANTES_RAW_PATHS) {
  message("[representantes] Leyendo tablas finales y Excel manuales...")
  raw_paths <- normalize_representantes_raw_paths(raw_paths)
  result <- audit_representantes_coverage(
    info = readRDS("tablas-finales/hechos/info.rds"),
    votos = readRDS("tablas-finales/hechos/votos.rds"),
    elecciones = data.table::fread("tablas-finales/dimensiones/elecciones", colClasses = "character"),
    territorios = data.table::fread("tablas-finales/dimensiones/territorios", colClasses = "character"),
    partidos = data.table::fread("tablas-finales/dimensiones/partidos", colClasses = "character"),
    nrepresentantes_prov = readxl::read_xlsx(raw_paths[["nrepresentantes_prov"]]),
    nrepresentantes_muni = readxl::read_xlsx(raw_paths[["nrepresentantes_muni"]]),
    representantes_prov = readxl::read_xlsx(raw_paths[["representantes_prov"]]),
    representantes_muni = readxl::read_xlsx(raw_paths[["representantes_muni"]])
  )

  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  writexl::write_xlsx(result$sheets, output_path)
  message(sprintf(
    "[representantes] Pendientes escritos en %s (%d filas)",
    output_path,
    sum(vapply(result$sheets, nrow, integer(1)))
  ))
  invisible(output_path)
}

if (sys.nframe() == 0L) {
  validate_representantes_coverage()
}
