library(dplyr)
library(chromote)
library(rvest)
library(purrr)
library(stringr)
library(readr)
library(promises)

# --- Helpers ---

extract_html <- function(b) {
  b$Runtime$evaluate("document.documentElement.outerHTML")$result$value %>%
    read_html()
}

parse_tables <- function(html) {
  tables <- html %>% html_elements("table")

  parsed <- map(tables, function(table) {
    html_table(table, convert = FALSE) %>%
      janitor::clean_names() %>%
      select(where(~ !all(is.na(.)))) %>%
      as_tibble()
  })

  if (length(parsed) > 2) parsed <- parsed[1:2]

  parsed
}

get_election_hrefs <- function(html) {
  html %>%
    html_elements("a") %>%
    html_attr("href") %>%
    keep(~ grepl("javascript:selectProcess", .x) & grepl("'u'", .x)) %>%
    discard(~ .x == "javascript:selectProcess('','c');") %>%
    str_remove(";$")
}

extract_election_id <- function(href) {
  str_match(href, "selectProcess\\('([^']+)'")[, 2]
}

navigate_wait <- function(b, url) {
  p_load <- b$Page$loadEventFired(wait_ = FALSE)
  b$Page$navigate(url, wait_ = FALSE)
  p_load
}

eval_and_wait <- function(b, js) {
  p_load <- b$Page$loadEventFired(wait_ = FALSE)
  b$Runtime$evaluate(js, wait_ = FALSE)
  p_load
}

# --- Persistencia incremental ---

OUTPUT_DIR <- "data-raw/hechos/02-aragon/scrape"
INFO_FILE <- file.path(OUTPUT_DIR, "info.csv")
RESULTADOS_FILE <- file.path(OUTPUT_DIR, "resultados.csv")

dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

load_done <- function() {
  done <- character(0)
  if (file.exists(INFO_FILE)) {
    existing <- read_csv(INFO_FILE, col_types = cols(.default = "c"), show_col_types = FALSE)
    if (nrow(existing) > 0) {
      done <- paste(existing$eleccion_id, existing$codigo_municipio, sep = "|")
    }
  }
  done
}

append_rows <- function(df, file) {
  write_header <- !file.exists(file)
  write_csv(df, file, append = !write_header, col_names = write_header)
}

# Procesa una elección: selecciona y recorre municipios pendientes
scrape_election <- function(session, href, codigos, worker_id, done_set) {
  eleccion_id <- extract_election_id(href)

  pending <- codigos[!paste(eleccion_id, codigos, sep = "|") %in% done_set]
  message(sprintf("[W%d] === %s === (%d pendientes, %d ya hechos)",
    worker_id, eleccion_id, length(pending), length(codigos) - length(pending)))

  if (length(pending) == 0) return(promise_resolve(NULL))

  navigate_wait(session, URL_MAIN)$then(function(value) {
    eval_and_wait(session, href)
  })$then(function(value) {
    reduce(seq_along(pending), function(p, i) {
      p$then(function(value) {
        codigo <- pending[i]
        if (i %% 100 == 1 || i == length(pending)) {
          message(sprintf("[W%d] %s [%d/%d] %s", worker_id, eleccion_id, i, length(pending), codigo))
        }

        navigate_wait(session, sprintf(URL_MUNI, codigo))$then(function(value) {
          html <- extract_html(session)
          tables <- parse_tables(html)
          if (length(tables) >= 2) {
            info_row <- mutate(tables[[1]],
              eleccion_id = eleccion_id, codigo_municipio = codigo
            )
            res_row <- mutate(tables[[2]],
              eleccion_id = eleccion_id, codigo_municipio = codigo
            )
            append_rows(info_row, INFO_FILE)
            append_rows(res_row, RESULTADOS_FILE)
          } else {
            message(sprintf("[W%d] WARN: %s/%s sin tablas", worker_id, eleccion_id, codigo))
          }
        })$catch(function(e) {
          message(sprintf("[W%d] ERROR %s/%s: %s", worker_id, eleccion_id, codigo, conditionMessage(e)))
        })
      })
    }, .init = promise_resolve(NULL))
  })
}

# --- Config ---

URL_MAIN <- "https://servicios.aragon.es/aeaw/processSelect.do"
URL_MUNI <- "https://servicios.aragon.es/aeaw/globalReport.do?codigo=%s&tipo=u&ambito=m"
N_WORKERS <- 4L

codigos_municipios <-
  infoelectoral::codigos_municipios %>%
  filter(codigo_provincia %in% c("22", "44", "50")) %>%
  transmute(codigo_municipio = paste0(codigo_provincia, codigo_municipio)) %>%
  pull()

# --- Scraping ---

done_set <- load_done()
message(sprintf("Registros ya scrapeados: %d", length(done_set)))

b_init <- ChromoteSession$new()
b_init$default_timeout <- 60

navigate_wait(b_init, URL_MAIN)$then(function(value) {
  html <- extract_html(b_init)
  hrefs <<- get_election_hrefs(html)
  message(sprintf("Encontradas %d elecciones", length(hrefs)))
  b_init$close()
})$then(function(value) {

  election_chunks <- split(seq_along(hrefs), (seq_along(hrefs) - 1L) %% N_WORKERS + 1L)

  worker_promises <- imap(election_chunks, function(indices, worker_id) {
    session <- ChromoteSession$new()
    session$default_timeout <- 60
    worker_id <- as.integer(worker_id)
    message(sprintf("[W%d] Arrancado — %d elecciones asignadas", worker_id, length(indices)))

    reduce(indices, function(p, j) {
      p$then(function(value) {
        scrape_election(session, hrefs[j], codigos_municipios, worker_id, done_set)
      })
    }, .init = promise_resolve(NULL))$finally(function() {
      message(sprintf("[W%d] Cerrado", worker_id))
      session$close()
    })
  })

  promise_all(.list = worker_promises)

})$then(function(value) {
  info <<- read_csv(INFO_FILE, col_types = cols(.default = "c"), show_col_types = FALSE)
  resultados <<- read_csv(RESULTADOS_FILE, col_types = cols(.default = "c"), show_col_types = FALSE)

  message(sprintf(
    "\nFinalizado: %d filas en info, %d filas en resultados",
    nrow(info), nrow(resultados)
  ))
})$catch(function(err) {
  message("Error fatal: ", conditionMessage(err))
})

