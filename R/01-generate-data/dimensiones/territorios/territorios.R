library(dplyr)
library(readr)
library(purrr)

INPUT_DIR <- "data-raw/codigos_territorios"

files <- list.files(INPUT_DIR, pattern = "\\.rds$", full.names = T)
codigos_secciones <-
  map(files, readRDS) %>%
  list_rbind() %>%
  distinct()

# Circunscripciones y correspondencia municipio-circunscripción (genérico)
circunscripciones_def <- read_csv(
  file.path(INPUT_DIR, "circunscripciones.csv"),
  show_col_types = FALSE, col_types = cols(.default = "c")
)
correspondencia <- read_csv(
  file.path(INPUT_DIR, "correspondencia_municipio_circunscripcion.csv"),
  show_col_types = FALSE, col_types = cols(.default = "c")
)

ccaa_names <-
  infoelectoral::codigos_ccaa %>%
  transmute(tipo = "ccaa", codigo_ccaa = codigo_ccaa_ine, nombre = ccaa)

prov_names <-
  infoelectoral::codigos_provincias %>%
  transmute(tipo = "provincia", codigo_provincia, nombre = provincia)

muni_names <-
  read_csv("data-raw/nombres_municipios.csv", show_col_types = F) %>%
  mutate(
    tipo = "municipio",
    codigo_provincia,
    codigo_municipio,
    nombre, .keep = "used"
  ) %>%
  mutate(nombre = ifelse(is.na(nombre), paste0(codigo_provincia, codigo_municipio), nombre))

ccaa <- codigos_secciones %>%
  group_by(codigo_ccaa) %>%
  count() %>%
  select(-n) %>%
  mutate(
    codigo_provincia = "99",
    codigo_circunscripcion = NA_character_,
    codigo_municipio = "999",
    codigo_distrito = "99",
    codigo_seccion = "9999"
  ) %>%
  mutate(tipo = "ccaa", .before = 1) %>%
  left_join(ccaa_names, by = c("codigo_ccaa", "tipo"))

provincia <- codigos_secciones %>%
  filter(codigo_circunscripcion != "99") %>%
  group_by(codigo_ccaa, codigo_provincia = codigo_circunscripcion) %>%
  count() %>%
  select(-n) %>%
  mutate(
    codigo_circunscripcion = NA_character_,
    codigo_municipio = "999",
    codigo_distrito = "99",
    codigo_seccion = "9999"
  ) %>%
  mutate(tipo = "provincia", .before = 1) %>%
  left_join(prov_names, by = c("codigo_provincia", "tipo"))

municipio <-
  muni_names %>%
  left_join(
    select(provincia, codigo_ccaa, codigo_provincia),
    by = "codigo_provincia"
  ) %>%
  mutate(
    codigo_distrito = "99",
    codigo_seccion = "9999"
  ) %>%
  mutate(tipo = "municipio", .before = 1)

municipio <- codigos_secciones %>%
  group_by(codigo_ccaa, codigo_provincia, codigo_municipio) %>%
  count() %>%
  select(-n) %>%
  mutate(
    codigo_distrito = "99",
    codigo_seccion = "9999"
  ) %>%
  mutate(tipo = "municipio", .before = 1) %>%
  full_join(muni_names, by = c("codigo_provincia", "codigo_municipio", "tipo"))

distrito <- codigos_secciones %>%
  group_by(codigo_ccaa, codigo_provincia, codigo_municipio, codigo_distrito) %>%
  count() %>%
  select(-n) %>%
  mutate(
    codigo_seccion = "9999"
  ) %>%
  mutate(tipo = "distrito", .before = 1) %>%
  filter(codigo_distrito != "99")

secciones <- codigos_secciones %>%
  select(-codigo_circunscripcion) %>%
  mutate(tipo = "seccion", .before = 1) %>%
  distinct()

circunscripcion <- circunscripciones_def %>%
  transmute(
    tipo = "circunscripcion",
    codigo_ccaa,
    codigo_provincia,
    codigo_circunscripcion,
    codigo_municipio = "999",
    codigo_distrito = "99",
    codigo_seccion = "9999",
    nombre
  )

territorios <-
  bind_rows(
    ccaa,
    provincia,
    circunscripcion,
    municipio,
    distrito,
    secciones
  ) %>%
  mutate(
    codigo_seccion = stringr::str_pad(codigo_seccion, width = 4, side = "left", pad = "0")
  ) %>%
  filter(
    !is.na(codigo_ccaa),
    nchar(codigo_provincia) == 2,
    !(tipo == "municipio" & codigo_municipio == "999"),
    !(tipo == "distrito" & codigo_municipio == "999"),
    !(tipo == "seccion" & codigo_municipio == "999"),
    !(tipo == "seccion" & codigo_seccion %in% c("9999", "0000"))
  ) %>%
  distinct() %>%
  # Asignar codigo_circunscripcion desde correspondencia CSV; resto usa codigo_provincia
  # Filtrar códigos centinela (999, 990) que no representan municipios reales
  left_join(
    correspondencia %>%
      filter(!codigo_municipio %in% c("999", "990")) %>%
      select(codigo_provincia, codigo_municipio, circ_auto = codigo_circunscripcion),
    by = c("codigo_provincia", "codigo_municipio")
  ) %>%
  mutate(
    codigo_circunscripcion = case_when(
      tipo %in% c("ccaa", "provincia") ~ NA_character_,
      !is.na(circ_auto) ~ circ_auto,
      TRUE ~ coalesce(codigo_circunscripcion, codigo_provincia)
    )
  ) %>%
  select(-circ_auto) %>%
  mutate(
    codigo_completo = paste0(
      codigo_ccaa,
      codigo_provincia,
      codigo_municipio,
      codigo_distrito,
      codigo_seccion
    ),
    parent_id =
      case_when(
        tipo == "ccaa" ~ NA_character_,
        tipo == "provincia" ~ paste0(codigo_ccaa, "9999999"),
        tipo == "circunscripcion" ~ paste0(codigo_ccaa, codigo_provincia, "99999"),
        tipo == "municipio" ~ paste0(codigo_ccaa, codigo_provincia, "99999"),
        tipo == "distrito" ~ paste0(codigo_ccaa, codigo_provincia, codigo_municipio, "999"),
        tipo == "seccion" ~ paste0(codigo_ccaa, codigo_provincia, codigo_municipio, codigo_distrito, "999")
      )
  ) %>%
  ungroup() %>%
  mutate(id = row_number(), .before = 1)


# GENERAR EL PARENT ID
territorios <- territorios %>%
  mutate(id = as.character(id))

territorios <- territorios %>%
  left_join(
    territorios %>% filter(tipo == "ccaa") %>%
      select(id_parent = id, codigo_ccaa),
    by = "codigo_ccaa"
  ) %>%
  mutate(parent_id = if_else(
    tipo == "provincia",
    id_parent,
    parent_id
  )) %>%
  select(-id_parent)

territorios <- territorios %>%
  left_join(
    territorios %>% filter(tipo == "provincia") %>%
      select(id_parent = id, codigo_ccaa, codigo_provincia),
    by = c("codigo_ccaa", "codigo_provincia")
  ) %>%
  mutate(parent_id = if_else(
    tipo == "municipio" | (tipo == "circunscripcion" & codigo_provincia != "99"),
    id_parent,
    parent_id
  )) %>%
  select(-id_parent)

# Circunscripciones de nivel CCAA (codigo_provincia == "99") → parent = ccaa
territorios <- territorios %>%
  left_join(
    territorios %>% filter(tipo == "ccaa") %>%
      select(id_parent = id, codigo_ccaa),
    by = "codigo_ccaa"
  ) %>%
  mutate(parent_id = if_else(
    tipo == "circunscripcion" & codigo_provincia == "99",
    id_parent,
    parent_id
  )) %>%
  select(-id_parent)

# Municipios con circunscripción sub-provincial → parent = circunscripcion
territorios <- territorios %>%
  left_join(
    territorios %>% filter(tipo == "circunscripcion") %>%
      select(id_parent = id, codigo_ccaa, codigo_circunscripcion),
    by = c("codigo_ccaa", "codigo_circunscripcion")
  ) %>%
  mutate(parent_id = if_else(
    tipo == "municipio" & !is.na(id_parent),
    id_parent,
    parent_id
  )) %>%
  select(-id_parent)

territorios <- territorios %>%
  left_join(
    territorios %>% filter(tipo == "municipio") %>%
      select(id_parent = id, codigo_ccaa, codigo_provincia, codigo_municipio),
    by = c("codigo_ccaa", "codigo_provincia", "codigo_municipio")
  ) %>%
  mutate(parent_id = if_else(
    tipo == "distrito",
    id_parent,
    parent_id
  )) %>%
  select(-id_parent)

territorios <-
  territorios %>%
  left_join(
    territorios %>% filter(tipo == "distrito") %>%
      select(id_parent = id, codigo_ccaa, codigo_provincia, codigo_municipio, codigo_distrito),
    by = c("codigo_ccaa", "codigo_provincia", "codigo_municipio", "codigo_distrito")
  ) %>%
  mutate(parent_id = if_else(
    tipo == "seccion",
    id_parent,
    parent_id
  )) %>%
  select(-id_parent)

territorios <-
  territorios %>%
  mutate(
    id = as.integer(id),
    parent_id = as.integer(parent_id)
  )

write_csv(territorios, "tablas-finales/dimensiones/territorios")
