# Regional fact targets, in the preferred execution order.
hechos_regions <- c(
    cyl         = "07-cyl",
    congreso    = "00-congreso",
    municipales = "00b-municipales",
    europeas    = "00c-europeas",
    andalucia   = "01-andalucia",
    aragon      = "02-aragon",
    asturias    = "03-asturias",
    baleares    = "04-baleares",
    canarias    = "05-canarias",
    cantabria   = "06-cantabria",
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

hechos_target_names <- function() {
    paste0("hechos_", names(hechos_regions))
}
