# lint.R
# Lint no bloqueante por defecto. Usa LINT_STRICT=true para fallar con lints.

if (!requireNamespace("lintr", quietly = TRUE)) {
    message("[ci] lintr no esta instalado; se omite lint")
    quit(save = "no", status = 0)
}

lints <- lintr::lint_dir(
    "R",
    exclusions = list("R/tmp_append_canarias.R")
)

max_print <- as.integer(Sys.getenv("LINT_MAX_PRINT", unset = "200"))
if (is.na(max_print) || max_print < 0) max_print <- 200

lint_count <- length(lints)
message("[ci] lintr encontro ", lint_count, " avisos")
if (lint_count > 0 && max_print > 0) {
    print(utils::head(lints, max_print))
    if (lint_count > max_print) {
        message(
            "[ci] Salida truncada a ", max_print,
            " avisos. Ajusta LINT_MAX_PRINT para cambiar el limite."
        )
    }
}

strict <- identical(tolower(Sys.getenv("LINT_STRICT", unset = "false")), "true")
if (strict && length(lints) > 0) {
    stop("lintr encontro problemas y LINT_STRICT=true", call. = FALSE)
}
