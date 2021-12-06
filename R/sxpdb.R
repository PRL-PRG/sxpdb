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
  stopifnot(is.character(package) | is.symbol(package), is.character(func) | is.symbol(func), is.character(argument) | is.symbol(argument) | is.na(argument))
  .Call(SXPDB_add_val_origin, db, val, package, func, if(is.na(argument)) NA_character_ else argument)
}

#' export
add_origin <- function(db, hash, package, func, argument) {
  stopifnot(is.raw(hash) && length(hash) == 20, is.character(package) | is.symbol(package), is.character(func) | is.symbol(func), is.character(argument) | is.symbol(argument) | is.na(argument))
  .Call(SXPDB_add_origin, db, hash, package, func, if(is.na(argument)) NA_character_ else argument)
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
get_value_idx <- function(db, idx) {
  .Call(SXPDB_get_val, db, idx)
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
		viewer <- lapply(seq.len(size_db(db)) - 1, function(i) .Call(SXPDB_get_val, db, i))
	}
	viewer
}

#' @export
view_meta_db <- function(db) {
  do.call(rbind.data.frame, lapply(seq.len(size_db(db)) - 1, function(i) as.data.frame(.Call(SXPDB_get_meta_idx, db, i))))
}


#' @export
get_origins <- function(db, hash) {
  stopifnot(is.raw(hash))
  .Call(SXPDB_get_origins, db, hash)
}

#' @export
get_origins_idx <- function(db, i) {
  stopifnot(is.numeric(i))
  .Call(SXPDB_get_origins_idx, db, i)
}

#' @export
view_origins_db <- function(db) {
  lapply(seq.lent(size_db(db)) - 1, function(i) as.data.frame(.Call(SXPDB_get_origins_idx, db, i)))
}

#' @export
path_db <- function(db) {
  .Call(SXPDB_path_db, db)
}

## Utilities

types_map <- c("NULL", "symbol", "pairlist", "closure", "environment", "promise",
               "language", "special", "builtin", "char", "logical", "", "", "integer",
               "real", "complex", "str", "...", "any", "list", "expression",
               "bytecode", "externalpointer", "weakreference", "raw", "S4")

#' @export
string_sexp_type <- function(int_type) {
  if(int_type < 0 || int_type > 25) {
    return("")
  }
  else {
    types_map[[int_type + 1]]
  }
}


