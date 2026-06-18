# Regional fact targets, in the preferred execution order.
hechos_regions <- c(
    europeas    = "00c-europeas",
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
    minsait     = "minsait"
)

filter_hechos_regions <- function(regions) {
    selected <- Sys.getenv("ETL_HECHOS_REGIONS", unset = "")
    if (!nzchar(selected)) {
        return(regions)
    }

    selected <- trimws(strsplit(selected, ",", fixed = TRUE)[[1]])
    selected <- selected[nzchar(selected)]

    keep <- names(regions) %in% selected | unname(regions) %in% selected
    unknown <- setdiff(selected, c(names(regions), unname(regions)))

    if (length(unknown) > 0) {
        stop(
            "ETL_HECHOS_REGIONS contiene regiones desconocidas: ",
            paste(unknown, collapse = ", "),
            call. = FALSE
        )
    }

    regions[keep]
}

hechos_regions <- filter_hechos_regions(hechos_regions)

hechos_target_names <- function() {
    paste0("hechos_", names(hechos_regions))
}
