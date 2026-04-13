
OUTPUT_DIR <- "data-raw/hechos/08-clm/"


url <- "https://datosabiertos.castillalamancha.es/node/73/dataset/download"

tmp <- tempfile()
download.file(url, tmp, mode = "wb")

tmp_dir <- tempfile(pattern = "clm_data")
dir.create(tmp_dir)
unzip(tmp, exdir = tmp_dir)
files <- list.files(tmp_dir, full.names = TRUE)
csv_files <- files[grepl("csv", files)]
csv_files <- csv_files[!grepl("_2\\.", csv_files)]
csv_files

file.copy(csv_files, OUTPUT_DIR, overwrite = T)
