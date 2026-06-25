.libPaths(c("renv/library/windows/R-4.5/x86_64-w64-mingw32", .libPaths()))

source("R/tests/validate_representantes_coverage.R", encoding = "UTF-8")
source("R/utils.R", encoding = "UTF-8")

matching_votos <- data.table::data.table(
  eleccion_id = 1L,
  territorio_id = 101L,
  partido_id = 1:3,
  denominacion_lower = c("partido uno", "partido dos", "partido tres"),
  siglas_lower = c("p1", "p2", "p3")
)
matching_representantes <- data.table::data.table(
  eleccion_id = 1L,
  territorio_id = 101L,
  denominacion_lower = c("partido uno", "partido dos", "nombre alternativo"),
  siglas_lower = c("p1", "partido dos", "p3"),
  representantes = c(1, 1, 1)
)
matching_result <- match_representantes_to_votos(
  matching_representantes,
  matching_votos
)
stopifnot(identical(sort(matching_result$partido_id), 1:3))

elecciones <- data.frame(
  id = 1:5,
  tipo_eleccion = c("G", "A", "L", "E", "A"),
  year = c("2000", "2001", "2002", "2003", "2004"),
  mes = c("03", "04", "05", "06", "07"),
  stringsAsFactors = FALSE
)

territorios <- data.frame(
  id = c(101, 201, 202, 203, 301, 401, 501),
  tipo = c(
    "provincia", "provincia", "circunscripcion", "circunscripcion",
    "municipio", "provincia", "provincia"
  ),
  codigo_ccaa = c("01", "03", "03", "03", "01", "01", "02"),
  codigo_provincia = c("01", "33", "33", "33", "04", "01", "02"),
  codigo_circunscripcion = c("99", "99", "331", "332", "04", "99", "99"),
  codigo_municipio = c("999", "999", "999", "999", "001", "999", "999"),
  stringsAsFactors = FALSE
)

info <- data.frame(
  eleccion_id = c(1, 2, 2, 2, 3, 4, 5),
  territorio_id = territorios$id,
  nrepresentantes = c(3, 5, 2, 1, 3, 10, 4)
)

votos <- data.frame(
  eleccion_id = c(1, 1, 2, 2, 2, 2, 3, 3, 4, 5, 5),
  territorio_id = c(101, 101, 201, 202, 202, 203, 301, 301, 401, 501, 501),
  partido_id = c(1, 2, 1, 1, 2, 3, 1, 2, 1, 1, 2),
  representantes = c(2, 1, 5, 2, 0, 0, 2, 0, 10, 3, 1)
)

partidos <- data.frame(
  id = 1:3,
  denominacion = c("Partido Uno", "Partido Dos", "Partido Tres"),
  siglas = c("P1", "P2", "P3"),
  stringsAsFactors = FALSE
)

nrepresentantes_prov <- data.frame(
  year = c("2000", "2001", "2001", "2004"),
  mes = c("03", "04", "04", "07"),
  tipo_eleccion = c("G", "A", "A", "A"),
  codigo_ccaa = c("01", "03", "03", "02"),
  codigo_provincia = c("01", "33", "33", "02"),
  codigo_circunscripcion = c("01", "331", "332", "02"),
  nrepresentantes = c(3, 2, 1, 4)
)

nrepresentantes_muni <- data.frame(
  year = "2002",
  mes = "05",
  tipo_eleccion = "L",
  codigo_ccaa = "01",
  codigo_provincia = "04",
  codigo_municipio = "001",
  nrepresentantes = 0
)

representantes_prov <- data.frame(
  year = c("2000", "2000", "2001", "2001", "2004", "2004"),
  mes = c("03", "03", "04", "04", "07", "07"),
  tipo_eleccion = c("G", "G", "A", "A", "A", "A"),
  codigo_ccaa = c("01", "01", "03", "03", "02", "02"),
  codigo_circunscripcion = c("01", "01", "331", "332", "02", "02"),
  denominacion = c(
    "Partido Uno", "Partido Dos", "Partido Uno", "Partido Tres",
    "Partido Uno", "Partido Dos"
  ),
  siglas = c("P1", "P2", "P1", "P3", "P1", "P2"),
  representantes = c(2, 1, 2, 0, 4, 1)
)

representantes_muni <- data.frame(
  year = c("2002", "2002"),
  mes = c("05", "05"),
  tipo_eleccion = c("L", "L"),
  codigo_ccaa = c("01", "01"),
  codigo_provincia = c("04", "04"),
  codigo_municipio = c("001", "001"),
  denominacion = c("Partido Uno", "Partido Dos"),
  siglas = c("P1", "P2"),
  representantes = c(2, 0)
)

result <- audit_representantes_coverage(
  info,
  votos,
  elecciones,
  territorios,
  partidos,
  nrepresentantes_prov,
  nrepresentantes_muni,
  representantes_prov,
  representantes_muni,
  warn = FALSE
)

stopifnot(nrow(result$universe) == 5L)
stopifnot(!401L %in% result$universe$territorio_id)
stopifnot(!201L %in% result$universe$territorio_id)
stopifnot(all(c(202L, 203L) %in% result$universe$territorio_id))
stopifnot(result$universe[territorio_id == 101L, codigo_circunscripcion] == "01")
stopifnot(result$universe[territorio_id == 301L, codigo_municipio] == "001")

final <- result$final_coverage
stopifnot(sum(final$nrepresentantes_invalido) == 0L)
stopifnot(sum(final$representantes_ausentes) == 1L)
stopifnot(sum(final$representantes_descuadrados) == 2L)

stopifnot(nrow(result$sheets$nrepresentantes_prov) == 0L)
stopifnot(nrow(result$sheets$nrepresentantes_muni) == 1L)
stopifnot(nrow(result$sheets$representantes_prov) == 3L)
stopifnot(nrow(result$sheets$representantes_muni) == 2L)
stopifnot(identical(result$sheets$nrepresentantes_muni$codigo_municipio, "001"))
stopifnot(identical(
  names(result$sheets$representantes_prov),
  c("year", "mes", "tipo_eleccion", "codigo_ccaa", "codigo_circunscripcion", "denominacion", "siglas")
))

# Con los cuatro Excel completos no debe quedar ninguna fila pendiente, aunque
# la auditoría final siga señalando los hechos sintéticos deliberadamente malos.
nrepresentantes_muni$nrepresentantes <- 3
representantes_prov$representantes[representantes_prov$year == "2001" &
  representantes_prov$codigo_circunscripcion == "332"] <- 1
representantes_prov$representantes[representantes_prov$year == "2004"] <- c(3, 1)
representantes_muni$representantes <- c(3, 0)

complete <- audit_representantes_coverage(
  info,
  votos,
  elecciones,
  territorios,
  partidos,
  nrepresentantes_prov,
  nrepresentantes_muni,
  representantes_prov,
  representantes_muni,
  warn = FALSE
)
stopifnot(all(vapply(complete$sheets, nrow, integer(1)) == 0L))

circ_join_territorios <- data.frame(
  id = c(5010, 5351),
  tipo = c("circunscripcion", "circunscripcion"),
  codigo_ccaa = c("05", "05"),
  codigo_provincia = c("99", "35"),
  codigo_circunscripcion = c("050", "351"),
  codigo_municipio = c("999", "999"),
  codigo_distrito = c("99", "99"),
  codigo_seccion = c("9999", "9999"),
  stringsAsFactors = FALSE
)

circ_join_representantes <- data.frame(
  codigo_ccaa = c("05", "05"),
  codigo_circunscripcion = c("050", "351"),
  codigo_municipio = c("999", "999"),
  codigo_distrito = c("99", "99"),
  codigo_seccion = c("9999", "9999"),
  stringsAsFactors = FALSE
)

circ_join_result <- dplyr::left_join(
  circ_join_representantes,
  dplyr::select(
    dplyr::filter(circ_join_territorios, tipo == "circunscripcion"),
    codigo_ccaa, codigo_circunscripcion, codigo_municipio,
    codigo_distrito, codigo_seccion, territorio_id = id
  ),
  by = dplyr::join_by(
    codigo_ccaa, codigo_circunscripcion, codigo_municipio,
    codigo_distrito, codigo_seccion
  )
)
stopifnot(identical(circ_join_result$territorio_id, c(5010, 5351)))

prop_elecciones <- data.frame(
  id = c(1, 2, 3, 6),
  tipo_eleccion = c("G", "A", "L", "A"),
  year = c("2000", "2001", "2002", "2006"),
  mes = c("03", "04", "05", "06"),
  stringsAsFactors = FALSE
)

prop_territorios <- data.frame(
  id = c(11, 101, 31, 201, 202, 203, 41, 401, 301, 61, 601, 602, 603),
  parent_id = c(NA, 11, NA, 31, 201, 201, NA, 41, 401, NA, 61, 601, 601),
  tipo = c(
    "ccaa", "provincia",
    "ccaa", "provincia", "circunscripcion", "circunscripcion",
    "ccaa", "provincia", "municipio",
    "ccaa", "provincia", "circunscripcion", "circunscripcion"
  ),
  codigo_ccaa = c("01", "01", "03", "03", "03", "03", "04", "04", "04", "06", "06", "06", "06"),
  codigo_provincia = c("99", "01", "99", "33", "33", "33", "99", "04", "04", "99", "66", "66", "66"),
  codigo_circunscripcion = c("99", "99", "99", "99", "331", "332", "99", "99", "04", "99", "99", "661", "662"),
  codigo_municipio = c("999", "999", "999", "999", "999", "999", "999", "999", "001", "999", "999", "999", "999"),
  stringsAsFactors = FALSE
)

prop_info <- data.frame(
  eleccion_id = c(1, 1, 2, 2, 2, 2, 3, 3, 3, 6, 6, 6, 6),
  territorio_id = prop_territorios$id,
  nrepresentantes = c(0, 3, 0, 0, 2, 1, 0, 0, 3, 0, 0, 2, 1)
)

prop_votos <- data.table::data.table(
  eleccion_id = c(
    1, 1, 1, 1,
    2, 2, 2, 2, 2, 2, 2,
    3, 3, 3, 3, 3, 3,
    6, 6, 6, 6, 6, 6, 6
  ),
  territorio_id = c(
    101, 101, 11, 11,
    202, 202, 203, 201, 201, 31, 31,
    301, 301, 401, 401, 41, 41,
    602, 602, 603, 601, 601, 61, 61
  ),
  partido_id = c(
    1, 2, 1, 2,
    1, 2, 3, 1, 3, 1, 3,
    1, 2, 1, 2, 1, 2,
    1, 2, 3, 1, 3, 1, 3
  ),
  votos = 1L,
  representantes = c(
    2, 1, 0, 0,
    2, 0, 1, 0, 0, 0, 0,
    2, 1, 0, 0, 0, 0,
    2, 0, 0, 0, 0, 0, 0
  )
)

prop_result <- propagate_representantes_to_ancestors(
  prop_info,
  prop_votos,
  prop_elecciones,
  prop_territorios
)
prop_info_out <- prop_result$info
prop_votos_out <- prop_result$votos

stopifnot(prop_info_out[eleccion_id == 1L & territorio_id == 11L, nrepresentantes] == 3)
stopifnot(prop_votos_out[eleccion_id == 1L & territorio_id == 11L & partido_id == 1L, representantes] == 2)
stopifnot(prop_votos_out[eleccion_id == 1L & territorio_id == 11L & partido_id == 2L, representantes] == 1)

stopifnot(prop_info_out[eleccion_id == 2L & territorio_id == 201L, nrepresentantes] == 3)
stopifnot(prop_info_out[eleccion_id == 2L & territorio_id == 31L, nrepresentantes] == 3)
stopifnot(prop_votos_out[eleccion_id == 2L & territorio_id == 201L & partido_id == 1L, representantes] == 2)
stopifnot(prop_votos_out[eleccion_id == 2L & territorio_id == 201L & partido_id == 3L, representantes] == 1)
stopifnot(prop_votos_out[eleccion_id == 2L & territorio_id == 31L & partido_id == 3L, representantes] == 1)

stopifnot(prop_info_out[eleccion_id == 3L & territorio_id == 401L, nrepresentantes] == 3)
stopifnot(prop_info_out[eleccion_id == 3L & territorio_id == 41L, nrepresentantes] == 3)
stopifnot(prop_votos_out[eleccion_id == 3L & territorio_id == 401L & partido_id == 1L, representantes] == 2)
stopifnot(prop_votos_out[eleccion_id == 3L & territorio_id == 41L & partido_id == 2L, representantes] == 1)

stopifnot(prop_info_out[eleccion_id == 6L & territorio_id == 601L, nrepresentantes] == 0)
stopifnot(prop_info_out[eleccion_id == 6L & territorio_id == 61L, nrepresentantes] == 0)
stopifnot(prop_votos_out[eleccion_id == 6L & territorio_id == 601L & partido_id == 1L, representantes] == 0)
stopifnot(prop_votos_out[eleccion_id == 6L & territorio_id == 61L & partido_id == 1L, representantes] == 0)

bad_votos <- prop_votos[!(eleccion_id == 2L & territorio_id == 31L & partido_id == 3L)]
missing_party_error <- tryCatch(
  {
    propagate_representantes_to_ancestors(
      prop_info,
      bad_votos,
      prop_elecciones,
      prop_territorios
    )
    NULL
  },
  error = function(e) e
)
stopifnot(inherits(missing_party_error, "error"))
stopifnot(grepl("No existen filas de votos", conditionMessage(missing_party_error), fixed = TRUE))

message("[OK] Pruebas sintéticas de cobertura de representantes")
