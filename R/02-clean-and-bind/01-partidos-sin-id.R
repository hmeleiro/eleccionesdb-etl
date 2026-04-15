library(dplyr)
library(purrr)
library(readr)

INPUT_DIR <- "data-processed/hechos/"

votos_files <- list.files(INPUT_DIR, recursive = T, full.names = T, pattern = "votos")

normalize_lower <- function(x) tolower(trimws(gsub("\\s+", " ", x)))

votos <- map(votos_files, readRDS) %>%
  list_rbind() %>%
  arrange(eleccion_id, territorio_id, -votos) %>%
  mutate(across(c(denominacion, siglas), normalize_lower, .names = "{.col}_lower"))

# Read partidos from data-processed (has: id, recode, denominacion, siglas, agrupacion)
partidos_raw <- read_csv("data-processed/partidos", show_col_types = F, na = "NNNNAAAA")

# Map recode -> partido_recode_id using the partidos_recode dimension
partidos_recode <- read_csv("tablas-finales/dimensiones/partidos_recode", show_col_types = F, na = "NNNNAAAA") %>%
  select(partido_recode_id = id, recode = partido_recode)

partidos <- partidos_raw %>%
  left_join(partidos_recode, by = "recode") %>%
  select(id, partido_recode_id, siglas, denominacion)

partidos_lower <-
  partidos %>%
  mutate(across(c(denominacion, siglas), normalize_lower, .names = "{.col}_lower")) %>%
  select(partido_id = id, denominacion_lower, siglas_lower) %>%
  filter(!duplicated(paste(denominacion_lower, siglas_lower)))

votos <-
  left_join(votos, partidos_lower, by = c("denominacion_lower", "siglas_lower"))

# territorios <- read_csv("tablas-finales/dimensiones/territorios", show_col_types = F)
# elecciones <- read_csv("tablas-finales/dimensiones/elecciones", show_col_types = F)

# Apply manual overrides for partido_id conflicts caused by ambiguous siglas/denominacion joins.
# To add a new case, add a row to data-raw/partido_id_overrides.csv.
partido_id_overrides <- read_csv(
  "data-raw/partido_id_overrides.csv",
  show_col_types = FALSE,
  na = "NA"
)

votos <-
  votos %>%
  left_join(
    partido_id_overrides %>%
      select(partido_id_asignado, eleccion_id, partido_id_correcto) %>%
      mutate(.has_override = TRUE),
    by = c("partido_id" = "partido_id_asignado", "eleccion_id")
  ) %>%
  mutate(
    .has_override = if_else(is.na(.has_override), FALSE, .has_override),
    partido_id = if_else(.has_override, partido_id_correcto, partido_id)
  ) %>%
  select(-.has_override)

partidos_sin_id <- votos %>%
  filter(is.na(partido_id)) %>%
  group_by(denominacion, siglas) %>%
  summarise(
    votos = sum(votos, na.rm = T)
  ) %>%
  arrange(-votos)

print(partidos_sin_id, n = 50)

PARTIDOS_IMPORTANTES <- c(
  "PSOE"      = "psoe|socialista obrero|pspv|psc|pse|psm",
  "PP"        = "^pp$|partido popular",
  "Podemos"   = "podemos",
  "UP"        = "unidas podemos|unidos podemos",
  "Sumar"     = "^sumar$",
  "Vox"       = "^vox$",
  "IU"        = "^iu$|izquierda unida",
  "ERC"       = "^erc$|esquerra republicana",
  "Junts"     = "junts|convergencia|ciu|pdcat|pdecat",
  "PNV"       = "^pnv$|^eaj$|partido nacionalista vasco",
  "EH Bildu"  = "bildu",
  "BNG"       = "^bng$|bloque nacionalista galego",
  "Cs"        = "^cs$|ciudadanos",
  "CUP"       = "^cup$",
  "Compromís" = "compromis",
  "Na+"       = "navarra suma|na\\+"
)

detectar_partidos_importantes_sin_id <- function(df) {
  if (nrow(df) == 0) return(invisible(NULL))

  encontrados <- purrr::imap_dfr(PARTIDOS_IMPORTANTES, function(patron, nombre) {
    df %>%
      filter(
        grepl(patron, siglas,       ignore.case = TRUE) |
        grepl(patron, denominacion, ignore.case = TRUE)
      ) %>%
      mutate(partido_posible = nombre)
  })

  if (nrow(encontrados) > 0) {
    warning(
      "Partidos importantes SIN partido_id asignado:\n",
      paste(
        sprintf(
          "  [%s]  siglas='%s'  denominacion='%s'  (votos=%s)",
          encontrados$partido_posible,
          encontrados$siglas,
          encontrados$denominacion,
          format(encontrados$votos, big.mark = ".")
        ),
        collapse = "\n"
      ),
      call. = FALSE
    )
  }

  invisible(encontrados)
}

partidos_sin_id_revisar <- detectar_partidos_importantes_sin_id(partidos_sin_id)

writexl::write_xlsx(partidos_sin_id_revisar, "data-processed/partidos_sin_id_pending.xlsx")


partidos <-
  bind_rows(partidos, partidos_sin_id) %>%
  mutate(.dedup_key = paste(
    tolower(trimws(gsub("\\s+", " ", denominacion))),
    tolower(trimws(gsub("\\s+", " ", siglas)))
  )) %>%
  filter(!duplicated(.dedup_key)) %>%
  select(-.dedup_key) %>%
  mutate(id = row_number()) %>%
  select(id, partido_recode_id, siglas, denominacion)

write_csv(partidos, "tablas-finales/dimensiones/partidos", na = "NNNNAAAA")
