# _targets.R
# Pipeline definition for eleccionesdb ETL
# Run with: targets::tar_make()
# Visualize: targets::tar_visnetwork()

library(targets)

options(dplyr.summarise.inform = FALSE, readr.show_col_types = FALSE)

tar_option_set(
    error = "continue",
    # Silenciar messages de todos los targets:
    deployment = "main"
)

source("R/pipeline_helpers.R")

# Region directories under code/01-generate-data/hechos/
# All use format.R as the main script
hechos_regions <- c(
    congreso    = "00-congreso",
    municipales = "00b-municipales",
    andalucia   = "01-andalucia",
    aragon      = "02-aragon",
    asturias    = "03-asturias",
    baleares    = "04-baleares",
    canarias    = "05-canarias",
    cantabria   = "06-cantabria",
    cyl         = "07-cyl",
    clm         = "08-clm",
    catalunya   = "09-catalunya",
    valencia    = "10-comunidad-valenciana",
    extremadura = "11-extremadura",
    galicia     = "12-galicia",
    madrid      = "13-comunidad-madrid",
    murcia      = "14-murcia",
    navarra     = "15-navarra",
    pais_vasco  = "16-pais-vasco",
    la_rioja    = "17-la-rioja",
    # Escrutinio provisional (Minsait): elecciones más recientes sin datos definitivos
    minsait     = "minsait"
)

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
    tar_target(dim_territorios, run_dim_territorios(), format = "file"),
    tar_target(dim_elecciones, run_dim_elecciones(), format = "file"),
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
        bind_hechos, dim_elecciones, dim_elecciones_fuentes,
        dim_territorios, dim_partidos
    )),

    # ---------------------------------------------------------------------------
    # Phase 5: Export
    # ---------------------------------------------------------------------------
    tar_target(export, run_export(
        bind_hechos, dim_elecciones, dim_elecciones_fuentes,
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
