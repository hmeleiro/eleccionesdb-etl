# Script: generate_data_index.R
# Generates the versioned data-raw manifest from Cloudflare R2.
# Admin-only: requires Cloudflare R2 credentials in .env or the environment.

library(paws.storage)
library(purrr)
library(tibble)
library(dotenv)
library(readr)

dotenv::load_dot_env()

env <- Sys.getenv
access_key <- env("CF_S3_ACCESS_KEY")
secret_key <- env("CF_S3_SECRET_KEY")
endpoint <- env("CF_S3_ENDPOINT")
bucket <- env("CF_S3_BUCKET")
public_base_url <- env("CF_S3_PUBLIC_BASE_URL")

required_env <- c(
    CF_S3_ACCESS_KEY = access_key,
    CF_S3_SECRET_KEY = secret_key,
    CF_S3_ENDPOINT = endpoint,
    CF_S3_BUCKET = bucket,
    CF_S3_PUBLIC_BASE_URL = public_base_url
)
missing_env <- names(required_env)[!nzchar(required_env)]
if (length(missing_env) > 0) {
    stop(
        "Faltan variables de entorno: ",
        paste(missing_env, collapse = ", "),
        call. = FALSE
    )
}

data_prefix <- "eleccionesdb-etl/data-raw/"
manifest_path <- "data-manifest.csv"

s3 <- paws.storage::s3(
    config = list(
        credentials = list(
            creds = list(
                access_key_id = access_key,
                secret_access_key = secret_key
            )
        ),
        endpoint = endpoint,
        region = "auto",
        s3_force_path_style = TRUE
    )
)

all_objs <- list()
marker <- NULL
repeat {
    res <- s3$list_objects_v2(
        Bucket = bucket,
        MaxKeys = 1000,
        ContinuationToken = marker,
        Prefix = data_prefix
    )
    all_objs <- c(all_objs, res$Contents)
    if (!isTRUE(res$IsTruncated)) break
    marker <- res$NextContinuationToken
}

df <- map_dfr(all_objs, function(x) {
    tibble(
        key = x$Key,
        size = x$Size,
        last_modified = as.character(x$LastModified),
        url = paste0(public_base_url, "/", x$Key)
    )
})

if (!nrow(df)) {
    stop("No se encontraron objetos bajo ", data_prefix, call. = FALSE)
}

readr::write_csv(df, manifest_path)

cat("Manifiesto generado: ", manifest_path, " (", nrow(df), " objetos)\n", sep = "")
