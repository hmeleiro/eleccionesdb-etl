library(arrow)
library(dplyr)
library(dotenv)

load_dot_env()

upload_dir_r2 <- function(
  dir,
  prefix = "",
  bucket = Sys.getenv("CF_S3_BUCKET"),
  base_url = Sys.getenv("R2_ENDPOINT")
) {
    stopifnot(dir.exists(dir))
    stopifnot(nzchar(bucket))
    stopifnot(nzchar(base_url))

    files <- list.files(dir, recursive = TRUE, full.names = FALSE)

    for (rel in files) {
        local_file <- file.path(dir, rel)

        key <- file.path(prefix, rel)
        key <- gsub("\\\\", "/", key)

        message("Subiendo: ", local_file, " -> ", key)

        aws.s3::put_object(
            file = local_file,
            object = key,
            bucket = bucket,
            region = "",
            base_url = base_url,
            use_https = TRUE,
            multipart = TRUE
        )
    }
}

upload_dir_r2(
    dir = "data-raw",
    prefix = "eleccionesdb-etl/data-raw"
)
