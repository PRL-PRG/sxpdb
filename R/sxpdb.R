#' Open a database
#' 
#' `open_db` opens a database for reading, writing, or merging. Reading does not load as much in memory. 
#' For instance, it will directly read values from the disk. When merging database _source_ into database
#' _target_, _source_ must be open in merge mode, and _target_, in write mode. Writing mode is required
#' when adding any new values to the database or building or updating search indexes.
#' 
#' @param db character vector of the name and path where to create the db. Default to `"db"`
#' @param mode `TRUE` if in write mode, `FALSE` if in read mode (default), `"merge"` if in merge mode
#' @param quiet boolean, whether to print messages or not
#' @returns  `NULL` on error, a external pointer with class tag "sxpdb" and class "sxpdb" to the database on success
#' @seealso [close_db()]
#' @export
open_db <- function(db = "db", mode = FALSE, quiet = TRUE) {
	if (!dir.exists(db)) {
    dir.create(db, recursive = TRUE)
	}

  prefix <- file.path(db, "sxpdb")

  structure(.Call(SXPDB_open_db, prefix, mode, quiet), class = "sxpdb")
}

#' Closes the database
#' 
#' Closes a previously open database. If `db` was uncorrectly opened, or has been already closed,
#' the function will error.
#' 
#' @param db database, sxpdb object
#' @return `NULL`
#' @seealso [open_db()]
#' @export
close_db <- function(db) {
  stopifnot(check_db(db))
  # Check if it is a EXTPTR and if it has the right tag here maybe
  .Call(SXPDB_close_db, db)
}

#' Add a value in the dabatase
#' 
#' `add_val` adds an R value in the database. It does not add origins or call ids. 
#'  This should be used in conjunction with [add_origin()]. 
#'  You should most likely never use it and rather directly use [add_val_origin()].
#' 
#' @param db database, sxpdb object
#' @param val any R value. Currently, environments and closures will be silently ignored
#' @returns integer index of the value if it has been added (now or before), or `NULL` if it has been ignored
#' @seealso [add_val_origin()], [add_origin()]
#' @export
add_val <- function(db, val) {
  stopifnot(check_db(db), write_mode(db))
  .Call(SXPDB_add_val, db, val)
}

#' Add a value with origin and call id
#'
#' `add_val_origin` adds an R value in the database, along with its origin
#' (package, function, argument names), and its call id.
#'
#' @param db database, sxpdb object
#' @param val any R value. Currently, environments and closures will be silently ignored
#' @param package character vector for the package name 
#' @param func character vector for the function name
#' @param argument character vector for the argument name. `""` or `NA` mean that `val` is a return value.
#' @param call_id integer unique id of the call from which the value comes from. Defaults to `0`
#' @returns integer index of the value if it has been added (now or before), or `NULL` if it has been ignored
#' @export
add_val_origin <- function(db, val, package, func, argument, call_id = 0) {
  stopifnot(check_db(db), write_mode(db), is.numeric(call_id), is.character(package) | is.symbol(package), is.character(func) | is.symbol(func), is.character(argument) | is.symbol(argument) | is.na(argument))
  .Call(SXPDB_add_val_origin, db, val, package, func, if(is.na(argument)) NA_character_ else argument, call_id)
}

#' Add origin to an already recorded value
#'
#' `add_origin` adds package, function and argument names for a value already recorded in the database.
#' The value is represented here by its hash. You will not probably use it and rather use [add_val_origin()]
#' directly, as `add_origin` will only save you the hash computation. If performance is an issue, you
#' should not use the R API anyway and directly hook into the C one.
#'
#' @param db database, sxpdb object
#' @param hash hash of an already recorded value
#' @param package character vector for the package name 
#' @param func character vector for the function name
#' @param argument character vector for the argument name. `""` or `NA` mean that `val` is a return value.
#' @returns `NULL`
#' @seealso [add_val_origin()]
#' @export
add_origin <- function(db, hash, package, func, argument) {
  stopifnot(check_db(db), write_mode(db), is.raw(hash) && length(hash) == 20, is.character(package) | is.symbol(package), is.character(func) | is.symbol(func), is.character(argument) | is.symbol(argument) | is.na(argument))
  .Call(SXPDB_add_origin, db, hash, package, func, if(is.na(argument)) NA_character_ else argument)
}

#' Sample randomly a value from the database
#'
#' `sample_val` samples a value from the database and returns the value.
#' The sampling uses an uniform distribution.
#' The value is picked along the whole database or according to a query built from [query_from_plan()]
#' or [query_from_value()].
#'
#' @param db database, sxpdb object
#' @param query query object or `NULL`. If `NULL`, samples from the whole database
#' @returns an R value. If the query is empty, it will return `NULL`. It is ambiguous with sampling
#' a value that  happens to be `NULL` so you should rather use [sample_index()].
#' @seealso [sample_index()]
#' @export
sample_val <- function(db, query = NULL) {
  stopifnot(check_db(db))
  .Call(SXPDB_sample_val, db, query)
}

#' Sample randomly a value from the database
#'
#' `sample_index` samples a value from the database and returns an index to the value.
#' The sampling uses an uniform distribution.
#' The value is picked along the whole database or according to a query built from [query_from_plan()]
#' or [query_from_value()].
#'
#' @inheritParams sample_val
#' @returns integer, an index to a value in the database. You can then access the value 
#'          with [get_value_idx()]. `NULL` if the query is empty.
#' @seealso [sample_val()]
#' @export
sample_index <- function(db, query = NULL) {
  stopifnot(check_db(db))
  .Call(SXPDB_sample_index, db, query)
}

#' Sample randomly a value similar to a given from the database
#'
#' `sample_similar` samples a value from the database, which is similar to `val` along
#' metadata parameters, relaxed according to `relax`.
#' The sampling uses an uniform distribution.
#' 
#'  You should rather use a combination of [query_from_value()] and [sample_index()] for more control.
#'
#' @param db database, sxpdb object
#' @param val any R value
#' @param relax character vector, as in [relax_query()]
#' @returns an R value. If the query is empty, it will return `NULL`. It is ambiguous with sampling
#' a value that  happens to be `NULL` so you should rather use [sample_index()] with [query_from_value()].
#' @seealso [sample_index()], [query_from-value()]
#' @export
sample_similar <- function(db, val, relax = "") {
  stopifnot(check_db(db), is.character(relax))
  .Call(SXPDB_sample_similar, db, val, FALSE, relax)
}

#' Merge a db into another one.
#' 
#' Deprecated. Rather use [merge_into()]
#' @returns `NULL` in case of error, number of values in the _target_ database after merging otherwise
#' @export
merge_db <- function(db1, db2) {
  .Deprecated("merge_into", package = "sxpdb")
  stopifnot(check_db(db1), check_db(db2), write_mode(db1))
  .Call(SXPDB_merge_db, db1, db2)
}

#' Merge a source db into a target db
#'
#' `merge_into` merges db _source_ into db _target_. Db _target_ must be open in write mode and
#' db _source_ must be opened in merge mode.
#'
#' @param target database, sxpdb object
#' @param source database, sxpdb object
#' @returns `NULL`in case of an error, a mapping from old indexes of all the values in the _source_
#'          database to the new indexes for them in the _target_ database.
#'
#' @seealso [merge_all_dbs()]
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
