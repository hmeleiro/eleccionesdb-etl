# _targets.R
# Pipeline definition for eleccionesdb ETL
# Run with: targets::tar_make()
# Visualize: targets::tar_visnetwork()

library(targets)

options(dplyr.summarise.inform = FALSE, readr.show_col_types = FALSE)

tar_option_set(
    error = Sys.getenv("TARGETS_ERROR", unset = "continue"),
    garbage_collection = TRUE,
    # Silenciar messages de todos los targets:
    deployment = "main"
)

source("R/hechos_regions.R")
source("R/pipeline_helpers.R")
ensure_pipeline_dirs()

# Build one target per region
hechos_targets <- lapply(names(hechos_regions), function(name) {
    tar_target_raw(
        name = paste0("hechos_", name),
        command = substitute(
            run_hechos(region_dir, dim_elecciones, dim_territorios),
            list(region_dir = hechos_regions[[name]])
        ),
        format = "file"
    )
})

# Symbol list for all hechos targets (used as dependency in bind)
all_hechos_syms <- lapply(names(hechos_regions), function(name) {
    as.symbol(paste0("hechos_", name))
})

list(
    # ---------------------------------------------------------------------------
    # Phase 1: Dimensiones
    # ---------------------------------------------------------------------------
    tar_target(raw_fechas_elecciones, "data-raw/fechas_elecciones.csv", format = "file"),
    tar_target(dim_tipos_eleccion, run_dim_tipos_eleccion(), format = "file"),
    tar_target(dim_territorios, run_dim_territorios(), format = "file"),
    tar_target(dim_elecciones, run_dim_elecciones(raw_fechas_elecciones), format = "file"),
    tar_target(dim_elecciones_fuentes, run_dim_elecciones_fuentes(dim_elecciones), format = "file"),
    tar_target(dim_partidos, run_dim_partidos(), format = "file"),

    # ---------------------------------------------------------------------------
    # Phase 2: Hechos (one target per region, all depend on dims)
    # ---------------------------------------------------------------------------
    hechos_targets,

    # ---------------------------------------------------------------------------
    # Phase 3a: Completar dimensión partidos con los que faltan en votos
    # ---------------------------------------------------------------------------
    tar_target_raw(
        name = "partidos_sin_id",
        command = substitute(
            run_partidos_sin_id(all_hechos, dim_partidos),
            list(all_hechos = as.call(c(as.symbol("list"), all_hechos_syms)))
        ),
        format = "file"
    ),

    # ---------------------------------------------------------------------------
    # Phase 3b: Bind all hechos
    # ---------------------------------------------------------------------------
    tar_target_raw(
        name = "bind_hechos",
        command = substitute(
            run_bind_hechos(all_hechos, partidos_sin_id),
            list(all_hechos = as.call(c(as.symbol("list"), all_hechos_syms)))
        ),
        format = "file"
    ),

    # ---------------------------------------------------------------------------
    # Phase 4: Write to DB
    # ---------------------------------------------------------------------------
    tar_target(writedb, run_writedb(
        bind_hechos, dim_tipos_eleccion, dim_elecciones, dim_elecciones_fuentes,
        dim_territorios, dim_partidos
    )),

    # ---------------------------------------------------------------------------
    # Phase 5: Export
    # ---------------------------------------------------------------------------
    tar_target(export, run_export(
        bind_hechos, dim_tipos_eleccion, dim_elecciones, dim_elecciones_fuentes,
        dim_territorios, dim_partidos
    ),
    format = "file"
    ),

    # ---------------------------------------------------------------------------
    # Phase 6: Diagnósticos de calidad + página web
    # ---------------------------------------------------------------------------
    tar_target(gen_diagnosticos, run_gen_diagnosticos(bind_hechos), format = "file"),
    tar_target(export_calidad,   run_export_calidad(gen_diagnosticos), format = "file")
)
