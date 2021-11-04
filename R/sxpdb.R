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
    dir.create(db)
	}

  prefix <- file.path(db, "sxpdb.csv")

  .Call(SXPDB_open_db, prefix)
}



close_db <- function(db) {
  # Check if it is a EXTPTR and if it has the right tag here maybe
  .Call(SXPDB_close_db, db)
}

## Testing/Information Gathering Related Functionality

#' @export
have_seen <- function(val) {
	.Call(RCRD_have_seen, val)
}

#' @export
report <- function() {
	.Call(RCRD_print_report)
}

#' @export
print_vals <- function () {
	if (size_db()) {
		for(i in 0:(size_db() - 1)) {
			print(.Call(RCRD_get_val, i))
		}
	} else {
		stop("There are no values in the database.")
	}
}

#' @export
view_db <- function() {
	if(size_db() == 0) {
		viewer <- list()
	} else {
		viewer <- lapply(seq(from=0, to=size_db()-1), function(i) .Call(RCRD_get_val, i))
	}
	viewer
}

#' @export
count_vals <- function() {
	.Call(RCRD_count_vals)
}

#' @export
size_db <- function() {
	.Call(RCRD_size_db)
}

#' @export
size_ints <- function() {
	.Call(RCRD_size_ints)
}
