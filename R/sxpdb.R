## VERSION = major.minor.patch
## Until major version is greater than 0, any changes in minor version number
## signifies version breaking change.
VERSION = "0.5.0"

## Primary Functionality

#' @export
# Open database specified by db
# if the database does not exist, it will create
open_db <- function(db = "db") {
	if (!dir.exists(db)) {
    dir.create(db, recursive = TRUE)
	}

  prefix <- file.path(db, "sxpdb.csv")

  .Call(SXPDB_open_db, prefix)
}

#' @export
close_db <- function(db) {
  # Check if it is a EXTPTR and if it has the right tag here maybe
  .Call(SXPDB_close_db, db)
}

#' @export
add_val <- function(db, val) {
  .Call(SXPDB_add_val, db, val)
}

#' @export
add_val_origin <- function(db, val, package, func, argument) {
  stopifnot(is.character(package), is.character(func), is.character(argument))
  .Call(SXPDB_add_val_origin, db, val, package, func, argument)
}

#' @export
sample_val <- function(db) {
  .Call(SXPDB_sample_val, db)
}

#' @export
merge_db <- function(db1, db2) {
  .Call(SXPDB_merge_db, db1, db2)
}

## Testing/Information Gathering Related Functionality

#' @export
have_seen <- function(db, val) {
	.Call(SXPDB_have_seen, db, val)
}

#' @export
get_meta <- function(db, val) {
  .Call(SXPDB_get_meta, db, val)
}

#' @export
get_meta_idx <- function(db, idx) {
  stopifnot(is.numeric(idx))
  .Call(SXPDB_get_meta_idx, db, idx)
}

#' @export
size_db <- function(db) {
  .Call(SXPDB_size_db, db)
}

#' @export
report <- function() {
	.Call(SXPDB_print_report) #TODO
}

#' @export
print_vals <- function (db) {
	if (size_db(db)) {
		for(i in 0:(size_db(db) - 1)) {
			print(.Call(SXPDB_get_val, db, i))
		}
	} else {
		stop("There are no values in the database.")
	}
}

#' @export
view_db <- function(db) {
	if(size_db(db) == 0) {
		viewer <- list()
	} else {
		viewer <- lapply(seq(from=0, to=size_db(db)-1), function(i) .Call(SXPDB_get_val, db, i))
	}
	viewer
}

#' @export
view_meta_db <- function(db) {
  do.call(rbind.data.frame, lapply(seq.int(0, size_db(db) - 1), function(i) as.data.frame(.Call(SXPDB_get_meta_idx, db, i))))
}

#' @export
avg_duration <- function(db) {
    .Call(SXPDB_avg_insertion_duration, db)
}
