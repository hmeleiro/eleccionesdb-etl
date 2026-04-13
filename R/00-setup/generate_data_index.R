# Script: generate_data_index.R
# Genera un índice CSV de todos los archivos públicos en el bucket S3 de Cloudflare
# Solo para admins (requiere credenciales en .env)

library(paws.storage)
library(dplyr)
library(purrr)
library(tibble)
library(dotenv)
library(readr)

# Cargar variables de entorno
dotenv::load_dot_env()

# Leer credenciales y endpoint
env <- Sys.getenv
access_key <- env("CF_S3_ACCESS_KEY")
secret_key <- env("CF_S3_SECRET_KEY")
endpoint <- env("CF_S3_ENDPOINT")
bucket <- env("CF_S3_BUCKET")
public_base_url <- env("CF_S3_PUBLIC_BASE_URL") # e.g. https://pub-cdn.domain.com

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

# Listar todos los objetos (manejar paginación si hay muchos)
all_objs <- list()
marker <- NULL
repeat {
    res <- s3$list_objects_v2(
        Bucket = bucket,
        MaxKeys = 1000,
        ContinuationToken = marker, Prefix = "eleccionesdb-etl/"
    )
    all_objs <- c(all_objs, res$Contents)
    if (!isTRUE(res$IsTruncated)) break
    marker <- res$NextContinuationToken
}

# Construir el índice
df <- map_dfr(all_objs, function(x) {
    tibble(
        key = x$Key,
        size = x$Size,
        last_modified = as.character(x$LastModified),
        url = paste0(public_base_url, "/", x$Key)
    )
})

# Guardar CSV (puedes cambiar a JSON si prefieres)
readr::write_csv(df, "docs-site/data_index.csv")

cat("Índice generado: docs-site/data_index.csv\n")
