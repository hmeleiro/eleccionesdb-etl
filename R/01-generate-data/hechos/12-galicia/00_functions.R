read_file <- function(file) {
  year <- str_extract(file, "[0-9]{4}")

  rename_cols <- c(
    "codigo_provincia" = "cod_cir",
    "codigo_provincia" = "c_d_cir",
    "codigo_provincia" = "cod_provincia",
    "codigo_municipio" = "cod_con",
    "codigo_municipio" = "c_d_con",
    "codigo_municipio" = "cod_municipio",
    "codigo_municipio" = "cod_municipio_3",
    "codigo_distrito" = "distrito",
    "codigo_seccion" = "seccion",
    "codigo_mesa" = "mesa",
    "votos_blancos" = "votos_brancos",
    "votos_blancos" = "votos_en_branco",
    "abstencion" = "total_abstencion",
    "validos" = "votos_validos"
  )

  remove_cols <- c("certif_alta", "certif_correc")

  tmp <- suppressMessages(read_excel(file)) %>%
    # select(!any_of("CÓD. MUNICIPIO...4")) %>%
    janitor::clean_names()

  # Índice de columna donde empiezan los partidos
  c <- 1 + max(which(str_detect(colnames(tmp), "votos")))
  tmp %>%
    rename(any_of(rename_cols)) %>%
    pivot_longer(any_of(c):ncol(.), names_to = "partido", values_to = "votos") %>%
    mutate(
      year,
      across(starts_with("codigo_"), as.character),
      .before = 1
    ) %>%
    select(!any_of(remove_cols)) %>%
    filter(!is.na(codigo_municipio))
}


read_file_provincias <- function(file) {
  year <- str_extract(file, "[0-9]{4}")
  rename_cols <- c(
    "codigo_provincia" = "cod_prov"
  )

  tmp <-
    suppressMessages(read_excel(file, sheet = "PROVINCIAS (OFICIALES)")) %>%
    janitor::clean_names()

  c <- 1 + max(which(colnames(tmp) %in% c("validos", "nulos")))

  tmp %>%
    rename(any_of(rename_cols)) %>%
    pivot_longer(any_of(c):ncol(.), names_to = "partido", values_to = "votos") %>%
    mutate(year, .before = 1)
}
