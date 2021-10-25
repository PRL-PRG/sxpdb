#!/usr/bin/env Rscript

library(signatr)
library(record)

args <- commandArgs(trailingOnly=TRUE)

if (length(args) < 1) {
  message("Usage: merge-dbs.R <dir>")
  q(status=1)
}

run_dir <- args[1]

if (!dir.exists(run_dir)) {
  stop(run_dir, ": no such a directory")
}

open_db("gbov", create=TRUE)

dirs <- list.dirs(path = run_dir, recursive = TRUE)
dbs <- grep("\\/db$", dirs)

lapply(dbs, function(db) merge_db(dirs[[db]]))


num_dbs <- length(dbs)

cat(sprintf("Merging %s dbs:\n\n", num_dbs))

cat(sprintf("gbov has %s values:\n\n", size_db()))
close_db()
