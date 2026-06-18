library(data.table)
library(readr)
library(stringr)

source("R/tests/validate_data_processed.R")

sum_na <- function(x) {
    sum(x, na.rm = TRUE)
}

drop_unused_cols <- function(dt, keep) {
    drop <- setdiff(names(dt), keep)
    if (length(drop) > 0) {
        dt[, (drop) := NULL]
    }
    invisible(dt)
}

read_csv_dt <- function(path) {
    dt <- readr::read_csv(path, show_col_types = FALSE)
    data.table::setDT(dt)
    dt
}

remap_ccaa_dt <- function(dt, ccaa_map) {
    if (!nrow(dt)) {
        return(dt)
    }

    dt[ccaa_map, codigo_ccaa_ine := i.codigo_ccaa_ine, on = "codigo_ccaa"]
    dt[, codigo_ccaa := codigo_ccaa_ine]
    dt[, codigo_ccaa_ine := NULL]
    dt
}

aggregate_info_dt <- function(dt, keys) {
    value_cols <- c("censo_ine", "abstenciones", "votos_validos", "votos_blancos", "votos_nulos")
    dt[, lapply(.SD, sum_na), by = keys, .SDcols = value_cols]
}

aggregate_votos_dt <- function(dt, keys) {
    dt[, .(votos = sum_na(votos)), by = keys]
}

join_level_dt <- function(dt, territorios, territorios_keys, fechas) {
    if (!nrow(dt)) {
        return(data.table())
    }

    out <- merge(dt, fechas, by = c("year", "mes"), all.x = TRUE, sort = FALSE)
    out <- merge(out, territorios, by = territorios_keys, all.x = TRUE, sort = FALSE)

    drop <- c("year", "mes", grep("^codigo_", names(out), value = TRUE))
    drop <- intersect(drop, names(out))
    if (length(drop) > 0) {
        out[, (drop) := NULL]
    }

    first_cols <- c("eleccion_id", "territorio_id")
    data.table::setcolorder(out, c(first_cols, setdiff(names(out), first_cols)))
    out
}

write_partial <- function(dt, path) {
    saveRDS(dt, path)
    invisible(path)
}

read_partials <- function(paths) {
    data.table::rbindlist(lapply(paths, readRDS), use.names = TRUE, fill = TRUE)
}

prepare_fechas_dt <- function(tipo_eleccion_value, min_year = NULL) {
    fechas <- read_csv_dt("tablas-finales/dimensiones/elecciones")
    fechas <- fechas[tipo_eleccion == tipo_eleccion_value]

    if (!is.null(min_year)) {
        fechas <- fechas[as.integer(year) >= min_year]
    }

    fechas[, .(eleccion_id = id, year = as.character(year), mes)]
}

prepare_territorios_dt <- function() {
    territorios_raw <- read_csv_dt("tablas-finales/dimensiones/territorios")

    list(
        sec = territorios_raw[
            tipo == "seccion",
            .(territorio_id = id, codigo_provincia, codigo_municipio, codigo_distrito, codigo_seccion)
        ],
        mun = territorios_raw[
            tipo == "municipio",
            .(territorio_id = id, codigo_provincia, codigo_municipio)
        ],
        prov = territorios_raw[
            tipo == "provincia",
            .(territorio_id = id, codigo_provincia)
        ],
        ccaa = territorios_raw[
            tipo == "ccaa",
            .(territorio_id = id, codigo_ccaa)
        ]
    )
}

prepare_ccaa_map_dt <- function(input_dir) {
    ccaa_map <- readRDS(file.path(input_dir, "codigos_ccaa.rds"))
    data.table::setDT(ccaa_map)
    ccaa_map[, .(codigo_ccaa, codigo_ccaa_ine)]
}

prepare_prov_enrichment_dt <- function(input_dir, nrepresentantes_col = NULL) {
    prov <- readRDS(file.path(input_dir, "provincias.rds"))
    data.table::setDT(prov)

    if (!nrow(prov)) {
        return(data.table(year = character(), mes = integer(), codigo_provincia = character()))
    }

    keep <- c("anno", "mes", "codigo_provincia", "participacion_1", "participacion_2")
    if (!is.null(nrepresentantes_col)) {
        keep <- c(keep, nrepresentantes_col)
    }
    keep <- intersect(keep, names(prov))

    drop_unused_cols(prov, keep)
    prov <- prov[codigo_provincia != "99"]
    prov[, year := as.character(anno)]
    prov[, anno := NULL]

    if (!is.null(nrepresentantes_col) && nrepresentantes_col %in% names(prov)) {
        data.table::setnames(prov, nrepresentantes_col, "nrepresentantes")
    }

    unique(prov)
}

prepare_mesas_dt <- function(input_dir, ccaa_map) {
    mesas <- readRDS(file.path(input_dir, "mesas.rds"))
    data.table::setDT(mesas)

    if (!nrow(mesas)) {
        return(mesas)
    }

    keep <- c(
        "anno", "mes", "codigo_ccaa", "codigo_provincia", "codigo_municipio",
        "municipio", "codigo_distrito", "codigo_seccion",
        "censo_ine", "votos_candidaturas", "votos_blancos", "votos_nulos",
        "siglas", "denominacion", "votos"
    )
    drop_unused_cols(mesas, keep)

    mesas[, year := as.character(anno)]
    mesas[, anno := NULL]
    mesas[, codigo_seccion := stringr::str_pad(as.character(codigo_seccion), 4, side = "left", pad = "0")]
    remap_ccaa_dt(mesas, ccaa_map)
    data.table::setindex(mesas, year, mes)
    mesas
}

prepare_municipios_fallback_dt <- function(input_dir, ccaa_map, missing_elections) {
    municipios <- readRDS(file.path(input_dir, "municipios.rds"))
    data.table::setDT(municipios)

    if (!nrow(municipios)) {
        return(municipios)
    }

    keep <- c(
        "anno", "mes", "codigo_ccaa", "codigo_provincia", "codigo_municipio",
        "censo_ine", "votos_candidaturas", "votos_blancos", "votos_nulos",
        "siglas", "denominacion", "votos"
    )
    drop_unused_cols(municipios, keep)

    municipios[, year := as.character(anno)]
    municipios[, anno := NULL]
    remap_ccaa_dt(municipios, ccaa_map)
    municipios <- municipios[missing_elections[, .(year, mes)], on = .(year, mes), nomatch = 0]
    data.table::setindex(municipios, year, mes)
    municipios
}

build_info_levels_dt <- function(info_muni, prov_enrichment) {
    info_prov <- aggregate_info_dt(info_muni, c("year", "mes", "codigo_ccaa", "codigo_provincia"))

    if (nrow(prov_enrichment)) {
        info_prov <- merge(
            info_prov,
            prov_enrichment,
            by = c("year", "mes", "codigo_provincia"),
            all.x = TRUE,
            sort = FALSE
        )
    }

    info_ccaa_base <- data.table::copy(info_prov)
    drop <- intersect(c("participacion_1", "participacion_2", "nrepresentantes"), names(info_ccaa_base))
    if (length(drop) > 0) {
        info_ccaa_base[, (drop) := NULL]
    }

    list(
        prov = info_prov,
        ccaa = aggregate_info_dt(info_ccaa_base, c("year", "mes", "codigo_ccaa"))
    )
}

build_votos_levels_dt <- function(votos_muni) {
    votos_prov <- aggregate_votos_dt(
        votos_muni,
        c("year", "mes", "codigo_ccaa", "codigo_provincia", "siglas", "denominacion")
    )

    list(
        prov = votos_prov,
        ccaa = aggregate_votos_dt(
            votos_prov,
            c("year", "mes", "codigo_ccaa", "siglas", "denominacion")
        )
    )
}

finalize_and_write_partial <- function(info_levels, votos_levels, fechas, territorios, info_path, votos_path) {
    info <- data.table::rbindlist(
        list(
            join_level_dt(info_levels$ccaa, territorios$ccaa, "codigo_ccaa", fechas),
            join_level_dt(info_levels$prov, territorios$prov, "codigo_provincia", fechas),
            join_level_dt(info_levels$mun, territorios$mun, c("codigo_provincia", "codigo_municipio"), fechas),
            join_level_dt(
                info_levels$sec,
                territorios$sec,
                c("codigo_provincia", "codigo_municipio", "codigo_distrito", "codigo_seccion"),
                fechas
            )
        ),
        use.names = TRUE,
        fill = TRUE
    )

    votos <- data.table::rbindlist(
        list(
            join_level_dt(votos_levels$ccaa, territorios$ccaa, "codigo_ccaa", fechas),
            join_level_dt(votos_levels$prov, territorios$prov, "codigo_provincia", fechas),
            join_level_dt(votos_levels$mun, territorios$mun, c("codigo_provincia", "codigo_municipio"), fechas),
            join_level_dt(
                votos_levels$sec,
                territorios$sec,
                c("codigo_provincia", "codigo_municipio", "codigo_distrito", "codigo_seccion"),
                fechas
            )
        ),
        use.names = TRUE,
        fill = TRUE
    )

    write_partial(info, info_path)
    write_partial(votos, votos_path)
    invisible(NULL)
}

process_mesas_election_dt <- function(mesas, year_value, mes_value, fechas, territorios,
                                      prov_enrichment, filter_empty_parties,
                                      info_path, votos_path) {
    data_cer <- mesas[year == year_value & mes == mes_value & municipio != "CERA"]
    data_cer[, municipio := NULL]

    info_input <- unique(data_cer[, .(
        year, mes, codigo_ccaa, codigo_provincia, codigo_municipio,
        codigo_distrito, codigo_seccion,
        censo_ine,
        abstenciones = censo_ine - votos_candidaturas - votos_blancos - votos_nulos,
        votos_validos = votos_candidaturas + votos_blancos,
        votos_blancos,
        votos_nulos
    )])

    info_seccion <- aggregate_info_dt(
        info_input,
        c("year", "mes", "codigo_ccaa", "codigo_provincia", "codigo_municipio", "codigo_distrito", "codigo_seccion")
    )
    info_muni <- aggregate_info_dt(
        info_seccion,
        c("year", "mes", "codigo_ccaa", "codigo_provincia", "codigo_municipio")
    )
    info_levels <- build_info_levels_dt(info_muni, prov_enrichment)
    info_levels$mun <- info_muni
    info_levels$sec <- info_seccion

    votos_input <- data_cer
    if (filter_empty_parties) {
        votos_input <- votos_input[!is.na(siglas) | !is.na(denominacion)]
    }

    votos_seccion <- aggregate_votos_dt(
        votos_input,
        c(
            "year", "mes", "codigo_ccaa", "codigo_provincia", "codigo_municipio",
            "codigo_distrito", "codigo_seccion", "siglas", "denominacion"
        )
    )
    votos_muni <- aggregate_votos_dt(
        votos_seccion,
        c("year", "mes", "codigo_ccaa", "codigo_provincia", "codigo_municipio", "siglas", "denominacion")
    )
    votos_levels <- build_votos_levels_dt(votos_muni)
    votos_levels$mun <- votos_muni
    votos_levels$sec <- votos_seccion

    finalize_and_write_partial(info_levels, votos_levels, fechas, territorios, info_path, votos_path)
    rm(data_cer, info_input, info_seccion, info_muni, info_levels, votos_input, votos_seccion, votos_muni, votos_levels)
    invisible(gc())
}

process_fallback_election_dt <- function(municipios, year_value, mes_value, fechas, territorios,
                                         prov_enrichment, filter_empty_parties,
                                         info_path, votos_path) {
    data_muni <- municipios[year == year_value & mes == mes_value]

    info_input <- unique(data_muni[, .(
        year, mes, codigo_ccaa, codigo_provincia, codigo_municipio,
        censo_ine,
        abstenciones = censo_ine - votos_candidaturas - votos_blancos - votos_nulos,
        votos_validos = votos_candidaturas + votos_blancos,
        votos_blancos,
        votos_nulos
    )])

    info_muni <- aggregate_info_dt(
        info_input,
        c("year", "mes", "codigo_ccaa", "codigo_provincia", "codigo_municipio")
    )
    info_levels <- build_info_levels_dt(info_muni, prov_enrichment)
    info_levels$mun <- info_muni
    info_levels$sec <- data.table()

    votos_input <- data_muni
    if (filter_empty_parties) {
        votos_input <- votos_input[!is.na(siglas) | !is.na(denominacion)]
    }

    votos_muni <- aggregate_votos_dt(
        votos_input,
        c("year", "mes", "codigo_ccaa", "codigo_provincia", "codigo_municipio", "siglas", "denominacion")
    )
    votos_levels <- build_votos_levels_dt(votos_muni)
    votos_levels$mun <- votos_muni
    votos_levels$sec <- data.table()

    finalize_and_write_partial(info_levels, votos_levels, fechas, territorios, info_path, votos_path)
    rm(data_muni, info_input, info_muni, info_levels, votos_input, votos_muni, votos_levels)
    invisible(gc())
}

process_large_hechos_dt <- function(region_dir, tipo_eleccion, label = region_dir,
                                    min_year = NULL, nrepresentantes_col = NULL,
                                    filter_empty_parties = TRUE) {
    input_dir <- file.path("data-raw/hechos", region_dir)
    output_dir <- file.path("data-processed/hechos", region_dir)
    dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

    partials_dir <- tempfile(paste0(region_dir, "-partials-"))
    dir.create(partials_dir, showWarnings = FALSE, recursive = TRUE)
    on.exit(unlink(partials_dir, recursive = TRUE), add = TRUE)

    fechas <- prepare_fechas_dt(tipo_eleccion, min_year = min_year)
    territorios <- prepare_territorios_dt()
    ccaa_map <- prepare_ccaa_map_dt(input_dir)
    prov_enrichment <- prepare_prov_enrichment_dt(input_dir, nrepresentantes_col = nrepresentantes_col)
    mesas <- prepare_mesas_dt(input_dir, ccaa_map)

    mesa_elections <- unique(mesas[municipio != "CERA", .(year, mes)])
    missing_elections <- fechas[!mesa_elections, on = .(year, mes)]

    info_paths <- character()
    votos_paths <- character()
    partial_index <- 0L

    if (nrow(mesa_elections) > 0) {
        data.table::setorder(mesa_elections, year, mes)
        for (i in seq_len(nrow(mesa_elections))) {
            partial_index <- partial_index + 1L
            year_value <- mesa_elections$year[[i]]
            mes_value <- mesa_elections$mes[[i]]
            message("[", label, "] Procesando mesas ", year_value, "-", mes_value)

            info_path <- file.path(partials_dir, sprintf("info_%03d.rds", partial_index))
            votos_path <- file.path(partials_dir, sprintf("votos_%03d.rds", partial_index))
            process_mesas_election_dt(
                mesas, year_value, mes_value, fechas, territorios, prov_enrichment,
                filter_empty_parties, info_path, votos_path
            )
            info_paths <- c(info_paths, info_path)
            votos_paths <- c(votos_paths, votos_path)
        }
    }

    if (nrow(missing_elections) > 0) {
        message(
            "Fallback: leyendo municipios() para elecciones sin mesas: ",
            paste(missing_elections$year, collapse = ", ")
        )
        municipios <- prepare_municipios_fallback_dt(input_dir, ccaa_map, missing_elections)
        data.table::setorder(missing_elections, year, mes)

        for (i in seq_len(nrow(missing_elections))) {
            partial_index <- partial_index + 1L
            year_value <- missing_elections$year[[i]]
            mes_value <- missing_elections$mes[[i]]
            message("[", label, "] Procesando fallback municipios ", year_value, "-", mes_value)

            info_path <- file.path(partials_dir, sprintf("info_%03d.rds", partial_index))
            votos_path <- file.path(partials_dir, sprintf("votos_%03d.rds", partial_index))
            process_fallback_election_dt(
                municipios, year_value, mes_value, fechas, territorios, prov_enrichment,
                filter_empty_parties, info_path, votos_path
            )
            info_paths <- c(info_paths, info_path)
            votos_paths <- c(votos_paths, votos_path)
        }

        rm(municipios)
        invisible(gc())
    }

    rm(mesas)
    invisible(gc())

    info <- read_partials(info_paths)
    votos <- read_partials(votos_paths)

    info <- info[!is.na(territorio_id)]
    votos <- votos[!is.na(territorio_id)]

    if ("abstenciones" %in% names(info)) {
        info[!is.na(abstenciones) & abstenciones < 0, abstenciones := NA_real_]
    }

    data.table::setorder(info, eleccion_id, territorio_id)
    data.table::setorder(votos, eleccion_id, territorio_id)

    info_order <- intersect(
        c(
            "eleccion_id", "territorio_id", "censo_ine", "abstenciones", "votos_validos",
            "votos_blancos", "votos_nulos", "participacion_1", "participacion_2", "nrepresentantes"
        ),
        names(info)
    )
    data.table::setcolorder(info, c(info_order, setdiff(names(info), info_order)))
    data.table::setcolorder(votos, c("eleccion_id", "territorio_id", "siglas", "denominacion", "votos"))

    data.table::setDF(info)
    data.table::setDF(votos)

    validate_info(info, label = paste0(label, "/info"))
    validate_votos(votos, label = paste0(label, "/votos"))
    validate_info_votos_consistency(info, votos, label = label)
    validate_votos_partido_match(votos, label = paste0(label, "/votos"))

    saveRDS(info, file.path(output_dir, "info.rds"))
    saveRDS(votos, file.path(output_dir, "votos.rds"))

    invisible(list(info = file.path(output_dir, "info.rds"), votos = file.path(output_dir, "votos.rds")))
}
