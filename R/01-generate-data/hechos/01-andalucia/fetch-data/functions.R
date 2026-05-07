get <- function(endpoint, ...) {
  require(httr)
  BASEURL <- "https://ws040.juntadeandalucia.es/siel-api/v1"
  url <- sprintf("%s/%s?", BASEURL, endpoint)
  # GET(url,
  #     config = httr::config(
  #       connecttimeout = 120,  # segundos para conectar (incluye handshake TLS)
  #       timeout = 300         # segundos máximo total de la petición
  #     ),
  #     ...)

  RETRY(
    "GET",
    url,
    times = 5,
    pause_base = 2,
    config = config(connecttimeout = 100, timeout = 300),
    ...
  )

}

get_once <- function(endpoint, ...) {
  require(httr)
  BASEURL <- "https://ws040.juntadeandalucia.es/siel-api/v1"
  url <- sprintf("%s/%s?", BASEURL, endpoint)

  GET(
    url,
    config = config(connecttimeout = 100, timeout = 300),
    ...
  )
}

parse_idescrutinio <- function(x) {
  require(purrr)
  require(tibble)
  require(tidyr)
  x[sapply(x, is.null)] <- NULL
  x <- map(x, as_tibble)

  out <- as_tibble(x) %>%
    unnest(1:ncol(.), names_sep = "_")
  return(out)
}

parse_votos <- function(x) {
  require(purrr)
  require(tibble)
  x %>%
    map_df(function(x) {
      x[sapply(x, is.null)] <- NULL
      as_tibble(x)
    })
}
