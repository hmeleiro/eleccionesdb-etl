library(dplyr)
library(readr)

get_hexcode <- function(color) {
  rgb_vals <- col2rgb(color)
  hex_code <- rgb(rgb_vals[1], rgb_vals[2], rgb_vals[3], maxColorValue = 255)
  return(hex_code)
}

# Este script queda como util para futuras extensiones (colores, etc.).
# El workflow principal de sincronizacion esta en sync-partidos.R
