## Primary Functionality

#' @export
# Open database specified by db
# if the database does not exist, it will create
open_db <- function(db = "db", mode = FALSE, quiet = TRUE) {
	if (!dir.exists(db)) {
    dir.create(db, recursive = TRUE)
	}

  prefix <- file.path(db, "sxpdb")

  .Call(SXPDB_open_db, prefix, mode, quiet)
}

#' @export
close_db <- function(db) {
  # Check if it is a EXTPTR and if it has the right tag here maybe
  .Call(SXPDB_close_db, db)
}


#' @export
add_val <- function(db, val) {
  stopifnot(write_mode(db))
  .Call(SXPDB_add_val, db, val)
}

#' @export
add_val_origin <- function(db, val, package, func, argument, call_id = 0) {
  stopifnot(write_mode(db), is.numeric(call_id), is.character(package) | is.symbol(package), is.character(func) | is.symbol(func), is.character(argument) | is.symbol(argument) | is.na(argument))
  .Call(SXPDB_add_val_origin, db, val, package, func, if(is.na(argument)) NA_character_ else argument)
}

#' @export
add_origin <- function(db, hash, package, func, argument) {
  stopifnot(write_mode(db), is.raw(hash) && length(hash) == 20, is.character(package) | is.symbol(package), is.character(func) | is.symbol(func), is.character(argument) | is.symbol(argument) | is.na(argument))
  .Call(SXPDB_add_origin, db, hash, package, func, if(is.na(argument)) NA_character_ else argument)
}

#' @export
sample_val <- function(db, query = NULL) {
  .Call(SXPDB_sample_val, db, query)
}

#' @export
sample_index <- function(db, query = NULL) {
  .Call(SXPDB_sample_index, db, query)
}

#' @export
sample_similar <- function(db, val, relax = "") {
  stopifnot(is.character(relax))
  .Call(SXPDB_sample_similar, db, val, FALSE, relax)
}


#' @export
merge_db <- function(db1, db2) {
  stopifnot(write_mode(db1))
  .Call(SXPDB_merge_db, db1, db2)
}

## Testing/Information Gathering Related Functionality

#' @export
have_seen <- function(db, val) {
	.Call(SXPDB_have_seen, db, val)
}

#' @export
get_value_idx <- function(db, idx) {
  stopifnot(is.numeric(idx), idx >= 0, idx < size_db(db))
  .Call(SXPDB_get_val, db, idx)
}


#' @export
get_meta <- function(db, val) {
  .Call(SXPDB_get_meta, db, val)
}

#' @export
get_meta_idx <- function(db, idx) {
  stopifnot(is.numeric(idx), idx >= 0, idx < size_db(db))
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
view_db <- function(db, query = NULL) {
  .Call(SXPDB_view_db, db, query)
}

#' @export
view_meta_db <- function(db, query = NULL) {
  .Call(SXPDB_view_metadata, db, query)
}


#' @export
get_origins <- function(db, hash) {
  stopifnot(is.raw(hash))
  .Call(SXPDB_get_origins, db, hash)
}

#' @export
get_origins_idx <- function(db, i) {
  stopifnot(is.numeric(i), i >= 0, i < size_db(db))
  .Call(SXPDB_get_origins_idx, db, i)
}

#' @export
view_origins_db <- function(db, query = NULL) {
  .Call(SXPDB_view_origins, db, query)
}

#' @export
path_db <- function(db) {
  .Call(SXPDB_path_db, db)
}

#' @export
check_db <- function(db, slow = FALSE) {
  stopifnot(is.logical(slow))
  .Call(SXPDB_check_db, db, slow)
}

#' @export
map_db <- function(db, fun, query = NULL) {
  stopifnot(is.function(fun))
  .Call(SXPDB_map_db, db, fun, query)
}

#' @export
filter_index_db <- function(db, fun, query = NULL) {
  stopifnot(is.function(fun))
  .Call(SXPDB_filter_index_db, db, fun, query)
}

#' @export
build_indexes <- function(db) {
  .Call(SXPDB_build_indexes, db)
}

#' @export
write_mode <- function(db) {
  .Call(SXPDB_write_mode, db)
}

#' @export
query_from_value <- function(value) {
  .Call(SXPDB_query_from_value, value)
}

#' @export
close_query <- function(query) {
  .Call(SXPDB_close_query, query)
}

#' @export
relax_query <- function(query, relax) {
  stopifnot(is.character(relax))
  .Call(SXPDB_relax_query, query, relax)
}


#' @export
is_query_empty <- function(query) {
  .Call(SXPDB_is_query_empty, query)
}

#' @export
merge_all_dbs <- function(db_paths, output_path) {
  stopifnot(is.character(db_paths), is.character(output_path))
  .Call(SXPDB_merge_all_dbs, db_paths, output_path)
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

#' @export
is_integer_real <- function(v) {
  stopifnot(is.double(v))
  .Call(SXPDB_is_integer_real, v)
}

#' @export
extptr_tag <- function(v) {
  .Call(SXPDB_extptr_tag, v)
}


.onUnload <- function (libpath) {
  library.dynam.unload("sxpdb", libpath)
}

