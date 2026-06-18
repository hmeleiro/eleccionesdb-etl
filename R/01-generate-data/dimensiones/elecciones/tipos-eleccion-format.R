library(readr)

tipos_eleccion <- data.frame(
  codigo = c("G", "A", "L", "E", "S"),
  descripcion = c("Congreso", "Autonomicas", "Locales", "Europeas", "Senado"),
  stringsAsFactors = FALSE
)

write_csv(tipos_eleccion, "tablas-finales/dimensiones/tipos_eleccion")
