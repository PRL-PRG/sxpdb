## Primary Functionality

#' @export
# Open database specified by db
# if the database does not exist, it will create
open_db <- function(db = "db", mode = FALSE, quiet = TRUE) {
	if (!dir.exists(db)) {
    dir.create(db, recursive = TRUE)
	}

  prefix <- file.path(db, "sxpdb")

  structure(.Call(SXPDB_open_db, prefix, mode, quiet), class = "sxpdb")
}

#' @export
close_db <- function(db) {
  stopifnot(check_db(db))
  # Check if it is a EXTPTR and if it has the right tag here maybe
  .Call(SXPDB_close_db, db)
}


#' @export
add_val <- function(db, val) {
  stopifnot(check_db(db), write_mode(db))
  .Call(SXPDB_add_val, db, val)
}

#' @export
add_val_origin <- function(db, val, package, func, argument, call_id = 0) {
  stopifnot(check_db(db), write_mode(db), is.numeric(call_id), is.character(package) | is.symbol(package), is.character(func) | is.symbol(func), is.character(argument) | is.symbol(argument) | is.na(argument))
  .Call(SXPDB_add_val_origin, db, val, package, func, if(is.na(argument)) NA_character_ else argument, call_id)
}

#' @export
add_origin <- function(db, hash, package, func, argument) {
  stopifnot(check_db(db), write_mode(db), is.raw(hash) && length(hash) == 20, is.character(package) | is.symbol(package), is.character(func) | is.symbol(func), is.character(argument) | is.symbol(argument) | is.na(argument))
  .Call(SXPDB_add_origin, db, hash, package, func, if(is.na(argument)) NA_character_ else argument)
}

#' @export
sample_val <- function(db, query = NULL) {
  stopifnot(check_db(db))
  .Call(SXPDB_sample_val, db, query)
}

#' @export
sample_index <- function(db, query = NULL) {
  stopifnot(check_db(db))
  .Call(SXPDB_sample_index, db, query)
}

#' @export
sample_similar <- function(db, val, relax = "") {
  stopifnot(check_db(db), is.character(relax))
  .Call(SXPDB_sample_similar, db, val, FALSE, relax)
}


#' @export
merge_db <- function(db1, db2) {
  .Deprecated("merge_into", package = "sxpdb")
  stopifnot(check_db(db1), check_db(db2), write_mode(db1))
  .Call(SXPDB_merge_db, db1, db2)
}

#' @export
merge_into <- function(target, source) {
   stopifnot(check_db(target), check_db(source), write_mode(target))
  .Call(SXPDB_merge_into_db, target, source)
}

## Testing/Information Gathering Related Functionality

#' @export
have_seen <- function(db, val) {
  stopifnot(check_db(db))
	.Call(SXPDB_have_seen, db, val)
}

#' @export
get_value_idx <- function(db, idx) {
  stopifnot(check_db(db), is.numeric(idx), idx >= 0, idx < size_db(db))
  .Call(SXPDB_get_val, db, idx)
}


#' @export
get_meta <- function(db, val) {
  stopifnot(check_db(db))
  .Call(SXPDB_get_meta, db, val)
}

#' @export
get_meta_idx <- function(db, idx) {
  stopifnot(check_db(db), is.numeric(idx), idx >= 0, idx < size_db(db))
  .Call(SXPDB_get_meta_idx, db, idx)
}

#' @export
size_db <- function(db) {
  stopifnot(check_db(db))
  .Call(SXPDB_size_db, db)
}


#' @export
nb_values_db <- function(db, query = NULL) {
  stopifnot(check_db(db))
  .Call(SXPDB_nb_values_db, db, query)
}

#' @export
view_db <- function(db, query = NULL) {
  stopifnot(check_db(db))
  .Call(SXPDB_view_db, db, query)
}

#' @export
view_meta_db <- function(db, query = NULL) {
  stopifnot(check_db(db))
  .Call(SXPDB_view_metadata, db, query)
}

#' @export
view_call_ids<- function(db, query = NULL) {
  stopifnot(check_db(db))
  .Call(SXPDB_view_call_ids, db, query)
}

#' @export
view_db_names <- function(db, query = NULL) {
  stopifnot(check_db(db))
  .Call(SXPDB_view_db_names, db, query)
}


#' @export
get_origins <- function(db, hash) {
  stopifnot(check_db(db), is.raw(hash))
  .Call(SXPDB_get_origins, db, hash)
}

#' @export
get_origins_idx <- function(db, i) {
  stopifnot(check_db(db), is.numeric(i), i >= 0, i < size_db(db))
  .Call(SXPDB_get_origins_idx, db, i)
}

#' @export
view_origins_db <- function(db, query = NULL) {
  stopifnot(check_db(db))
  .Call(SXPDB_view_origins, db, query)
}

#' @export
path_db <- function(db) {
  stopifnot(check_db(db))
  .Call(SXPDB_path_db, db)
}

#' @export
check_all_db <- function(db, slow = FALSE) {
  stopifnot(check_db(db), is.logical(slow))
  .Call(SXPDB_check_db, db, slow)
}

#' @export
map_db <- function(db, fun, query = NULL) {
  stopifnot(check_db(db), is.function(fun))
  .Call(SXPDB_map_db, db, fun, query)
}

#' @export
filter_index_db <- function(db, fun, query = NULL) {
  stopifnot(check_db(db), is.function(fun))
  .Call(SXPDB_filter_index_db, db, fun, query)
}

#' @export
build_indexes <- function(db) {
  stopifnot(check_db(db))
  .Call(SXPDB_build_indexes, db)
}

#' @export
write_mode <- function(db) {
  stopifnot(check_db(db))
  .Call(SXPDB_write_mode, db)
}

#' @export
query_from_value <- function(value) {
  .Call(SXPDB_query_from_value, value)
}

#' @export
query_from_plan <- function(plan) {
  .Call(SXPDB_query_from_plan, plan)
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
merge_all_dbs <- function(db_paths, output_path, parallel = TRUE) {
  stopifnot(is.character(db_paths), is.character(output_path), is.logical(parallel))
  .Call(SXPDB_merge_all_dbs, db_paths, output_path, parallel)
}


#' @export 
values_from_origin <- function(db, pkg_name, fun_name) {
  stopifnot(check_db(db), is.character(pkg_name), is.character(fun_name))
  .Call(SXPDB_values_from_origins, db, pkg_name, fun_name)
}

#' @export 
values_from_calls <- function(db, pkg_name, fun_name) {
  stopifnot(check_db(db), is.character(pkg_name), is.character(fun_name))
  .Call(SXPDB_values_from_calls, db, pkg_name, fun_name)
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
extptr_tag <- function(v) {
  .Call(SXPDB_extptr_tag, v)
}

check_db <- function(v) {
  typeof(v) == "externalptr" && extptr_tag(v) == "sxpdb"
}


.onUnload <- function (libpath) {
  library.dynam.unload("sxpdb", libpath)
}


## S3 API

#' @export
length.sxpdb <- function(x) {
  size_db(x)
}


#' @export
`[.sxpdb` <- function(x, i) {
    get_value_idx(x, i)
}

#' @export
close.sxpdb <- function(con, ...) {
  close_db(con)
}

dir_size <- function(path) {
  sum(file.size(list.files(path, recursive = TRUE, full.names = TRUE)), na.rm = TRUE)  
}

#' @export
summary.sxpdb <- function(object, digits = max(3L, getOption("digits") - 3L), ...) {
  # TODO: more info:
  # - number of packages, functions, parameters
  # - index built or not
  # - if built, some info about the type distribution
  structure(list(
    size = size_db(object),
    byte_size = dir_size(path_db(object)),
    path = path_db(object),
    write_mode = write_mode(object),
    digits = digits), class = "summary.sxpdb")
}

#' @export
print.summary.sxpdb <- function(x, ...) {
  cat("path: ", x$path, "\n")
  cat("size: ", x$byte_size, " bytes\n")
  cat("size: ", x$size, " elements\n")
  cat("write? ", x$write_mode, "\n")
}
