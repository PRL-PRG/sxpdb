#!/usr/bin/env Rscript

library(signatr)
library(record)
library(tictoc)

tic("recording")
print("recording started...")

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  message("Usage: record-vals.R <dir> [... <fileN>]")
  q(status = 1)
}

run_dir <- args[1]
package_name <- args[2]

if (!dir.exists(run_dir)) {
  stop(run_dir, ": no such a directory")
}

val_files <- list.files(path = run_dir, pattern = "values.RDS", recursive = TRUE)

if (is.null(open_db(paste0(package_name, "_db"), create = TRUE))) {
  print(paste0(package_name, "_db", " opened successfully."))
}

for (i in seq_along(val_files)) {
  val_list <- readRDS(paste0(run_dir, "/", val_files[[i]]))
  vals <- lapply(val_list, function(x) x[[3]])

  lapply(vals, add_val)
}

counts <- data.frame(total = count_vals(), unique = size_db(), ints = size_ints())

if (is.null(close_db())) {
  print(paste0(package_name, "_db", " closed successfully."))
}

print("recording done!")
time <- toc()

write.csv(counts, "trace-counts.csv", row.names = FALSE)
write.csv(time, "record-time.csv", row.names = FALSE)
