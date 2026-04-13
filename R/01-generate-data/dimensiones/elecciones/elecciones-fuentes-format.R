library(dplyr)
library(readr)

# Leer tabla de elecciones generada previamente
elecciones <- read_csv("tablas-finales/dimensiones/elecciones", show_col_types = FALSE)

# Leer fuentes de elecciones
# El CSV tiene una fila por (tipo_eleccion, codigo_ccaa), no por elección individual.
# Se une a elecciones por esas claves para obtener el eleccion_id.
fuentes_raw <- read_csv("data-raw/elecciones_fuentes.csv", show_col_types = FALSE)

# Unir por tipo_eleccion y codigo_ccaa
elecciones_fuentes <- elecciones %>%
    left_join(fuentes_raw, by = c("tipo_eleccion", "codigo_ccaa")) %>%
    select(
        eleccion_id = id,
        fuente,
        url_fuente,
        observaciones
    )

# Validación: avisar si hay elecciones sin fuente documentada
faltan_fuente <- elecciones_fuentes %>% filter(is.na(fuente) | is.na(url_fuente))
if (nrow(faltan_fuente) > 0) {
    warning(sprintf(
        "Hay %d elecciones sin fuente documentada. Revisa data-raw/elecciones_fuentes.csv.\nIDs afectados: %s",
        nrow(faltan_fuente),
        paste(faltan_fuente$eleccion_id, collapse = ", ")
    ))
}

write_csv(elecciones_fuentes, "tablas-finales/dimensiones/elecciones_fuentes")
message(sprintf("[OK] elecciones_fuentes generada (%d filas)", nrow(elecciones_fuentes)))
