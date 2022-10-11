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
#' @seealso [add_val_origin()], [add_origin()], [build_indexes()]
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
#' @seealso [build_indexes()]
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
#' @seealso [add_val_origin()], [build_indexes()]
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
#' @seealso [sample_index()], [query_from_value()]
#' @export
sample_similar <- function(db, val, relax = "") {
  stopifnot(check_db(db), is.character(relax))
  .Call(SXPDB_sample_similar, db, val, FALSE, relax)
}

#' Merge a db into another one.
#' 
#' Deprecated. Rather use [merge_into()]
#' 
#' @param db1 database, sxpdb object
#' @param db2 database, sxpdb object
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
#' @seealso [merge_all_dbs()], [build_indexes()]
#' @export
merge_into <- function(target, source) {
   stopifnot(check_db(target), check_db(source), write_mode(target))
  .Call(SXPDB_merge_into_db, target, source)
}

#' Checks if a value is in the database
#' 
#' `have_seen` checks if a value is stored in the database and returns its index in that case. 
#' 
#' @param db database, sxpdb object
#' @param val R value
#' @returns integer index of the value if the value is in the db, `NULL` otherwise
#'
#' @export
have_seen <- function(db, val) {
  stopifnot(check_db(db))
	.Call(SXPDB_have_seen, db, val)
}

#' Get the value in the db at a given index
#'
#' `get_value_idx` returns the value in `db` at index `idx`. You can also use the notation `db[idx]`.
#'
#' @param db database, sxpdb object
#' @param idx integer, index in the database. Indexes start at 0 (not at 1!) so `idx >= 0` and 
#'            `idx < size_db(db)`
#' @returns an R value
#' @seealso [`[.sxpdb`()]
#' @export
get_value_idx <- function(db, idx) {
  stopifnot(check_db(db), is.numeric(idx), idx >= 0, idx < size_db(db))
  .Call(SXPDB_get_val, db, idx)
}

#' Get the metadata associated to a value
#'
#' @description
#' `get_meta` returns the metadata associated to a value. It includes:
#'      * _runtime_ metadata: number of calls the value has been seen in, number of times the value has been seen during merges
#'      * _static_ metadata: type, size in bytes, length (for vector values), number of attributes, number of dimensionsm number of rows (for data frames, matrixes)
#'      * _debug_ metadata: only if the database was created with sxpdb in debug mode, includes how many times `MAYBE_SHARED` has been true on the value, 
#' and how many times we were able to use the SEXP address optimization
#'
#' @param db database, sxpdb object
#' @param val a R value
#' @returns a named list with the metadata, or `NULL` if there is no such value in the database
#'
#' @seealso [get_meta_idx()]
#' @export
get_meta <- function(db, val) {
  stopifnot(check_db(db))
  .Call(SXPDB_get_meta, db, val)
}

#' Get the metadata associated to an index
#'
#' @description
#' `get_meta` returns the metadata associated to a index in the database. It includes:
#'      * _runtime_ metadata: number of calls the value has been seen in, number of times the value has been seen during merges
#'      * _static_ metadata: type, size in bytes, length (for vector values), number of attributes, number of dimensionsm number of rows (for data frames, matrixes)
#'      * _debug_ metadata: only if the database was created with sxpdb in debug mode, includes how many times `MAYBE_SHARED` has been true on the value, 
#' and how many times we were able to use the SEXP address optimization
#'
#' @param db database, sxpdb object
#' @param idx integer index of the value to look for
#' @returns a named list with the metadata, or `NULL` if there is no such value in the database. It will
#'  error if the index is negative or larger than the number of elements in the database.
#'
#' @seealso [get_meta()]
#' @export
get_meta_idx <- function(db, idx) {
  stopifnot(check_db(db), is.numeric(idx), idx >= 0, idx < size_db(db))
  .Call(SXPDB_get_meta_idx, db, idx)
}

#' Number of elements in the database
#'
#' `size_db` returns the number of elements in the database. All elements are unique.
#'
#' @param db database, sxpdb object
#' @returns integer, number of values in the database, or `NULL` in case of error
#'
#' @seealso [nb_values_db()]
#' @export
size_db <- function(db) {
  stopifnot(check_db(db))
  .Call(SXPDB_size_db, db)
}

#' Computes the number of values matching a given query
#'
#' `nb_values_db` returns the number of values matching a given query. This is very cheap to compute
#' as it will just intersect or union some pre-computed arrays, so prefer to using [view_meta_db()]
#' for simple quantitative questions. `nb_values_db` called with a `NULL` query is equivalent to calling
#' `size_db`.
#'
#' @inheritParams sample_val
#' @returns integer, number of values matching the query.
#' @seealso [size_db()]
#' @export
nb_values_db <- function(db, query = NULL) {
  stopifnot(check_db(db))
  .Call(SXPDB_nb_values_db, db, query)
}


#' Fetch values from the database.
#' 
#' `view_db` fetches values from the database, given a query. It will materialize the R values,
#'  and might quickly feel up memory. Rather use metadata (with [view_meta_db()]) if you don't need 
#' to look at the valeus itself. If you need to extract data from the values or transform them, rather use 
#' [map_db()], which will only load one value at a time.
#' 
#' @param db database, sxpdb object
#' @param query query object, typically built from [query_from_plan()] or [query_from_value()].
#' @returns list of values matching the query
#' @seealso [map_db()] [view_meta_db()] [filter_index_db()] [get_value_idx()]
#' @export
view_db <- function(db, query = NULL) {
  stopifnot(check_db(db))
  .Call(SXPDB_view_db, db, query)
}

#' Fetches metadata from the database.
#'
#' @description 
#' `view_meta_db` fetches metadata from the database, given a query.
#' They include:
#'      * _runtime_ metadata: number of calls the value has been seen in, number of times the value has been seen during merges
#'      * _static_ metadata: type, size in bytes, length (for vector values), number of attributes, number of dimensionsm number of rows (for data frames, matrixes)
#'      * _debug_ metadata: only if the database was created with sxpdb in debug mode, includes how many times `MAYBE_SHARED` has been true on the value, 
#' and how many times we were able to use the SEXP address optimization
#'
#' @inheritParams view_db
#' @returns data frame of the metadata for all the values, in the order of their indexes in the database
#' 
#' @seealso [map_db()] [get_meta_idx()] [view_db()] [map_db()] [filter_index_db()]
#' @export
view_meta_db <- function(db, query = NULL) {
  stopifnot(check_db(db))
  .Call(SXPDB_view_metadata, db, query)
}

#' Fetches call ids from the database.
#'
#' `view_call_ids` fetches call ids from the database, according to a given query.
#'
#' @inheritParams view_db
#' @param query not taken into account currently
#' @returns a data frame with column `call_id` where each cell is a list of integers, the call ids, and each row 
#' corresponds to a value. The values are ordered in the same way as their indexes in the database.
#'
#' @seealso [map_db()] [get_meta_idx()] [view_db()] [map_db()] [filter_index_db()] [view_db_names()]
#' @export
view_call_ids<- function(db, query = NULL) {
  stopifnot(check_db(db))
  .Call(SXPDB_view_call_ids, db, query)
}

#' Fetches the origin databases 
#'
#' `view_db_names` fetches the origin databases of each values, i.e. in which database they were written
#' first, if they result from merging in the current database.
#'
#' @inheritParams view_db
#' @param query not taken into account currently
#' @returns a data frame with column `dbname` with the database names, in the same order as the values
#' ordered by indexes in the database.
#'
#' @seealso [map_db()] [get_meta_idx()] [view_db()] [map_db()] [filter_index_db()] [view_call_ids()]
#' @export
view_db_names <- function(db, query = NULL) {
  stopifnot(check_db(db))
  .Call(SXPDB_view_db_names, db, query)
}

#' Get the origins of a value
#'
#' `get_origins` returns the origins (package, function, parameter names) of a function given the hash
#' of the value. It save a hash computation, compared to [get_origins_idx()].
#'
#' @param db database, sxpdb object
#' @param hash RAW, hash of the value to look for
#' @returns data frame with columns `pkg`, `fun` and `param` for  package
#' function and parameter names.
#'
#' @seealso [get_origins_idx()], [view_origins_db()]
#' @export
get_origins <- function(db, hash) {
  stopifnot(check_db(db), is.raw(hash))
  .Call(SXPDB_get_origins, db, hash)
}

#' Get the origins of a value
#'
#' `get_origins` returns the origins (package, function, parameter names) of a function given the index
#' of the value. 
#'
#' @param db database, sxpdb object
#' @param i integer, index of the value in the database
#' @returns data frame with columns `pkg`, `fun` and `param` for  package
#' function and parameter names.
#'
#' @seealso [get_origins()], [view_origins_db()]
#' @export
get_origins_idx <- function(db, i) {
  stopifnot(check_db(db), is.numeric(i), i >= 0, i < size_db(db))
  .Call(SXPDB_get_origins_idx, db, i)
}

#' Fetches origins from the database
#'
#' `view_origins_db` fetches origins from the database, according to a given query.
#'
#' @inheritParams view_db
#' @returns data frame with columns `id`, `pkg`, `fun` and `param` for the index in the database, and package
#' function and parameter names.
#'
#' @seealso [get_origins()], [get_origins_idx()], [view_db()], [map_db()], [view_meta_db()]
#' @export
view_origins_db <- function(db, query = NULL) {
  stopifnot(check_db(db))
  .Call(SXPDB_view_origins, db, query)
}

#'  Path to the database directory
#' 
#' The database is stored as a directory of various configuration files and tables. `path_db`returns 
#' the path to this directory.
#' 
#' @param db database, sxpdb object
#' @returns character path to the database
#' 
#' @export
path_db <- function(db) {
  stopifnot(check_db(db))
  .Call(SXPDB_path_db, db)
}

#' Checks if the db is not corrupted.
#'
#' `check_all_db` checks if the database is not currupted and suggests some fixes if it is.
#'
#' @param db database, sxpdb object
#' @param slow boolean, if `TRUE`, it will unserialize all the values in the database and check 
#'        them all, if `FALSE, it will perform only quick consistency checks.
#' @returns integer vector of indices of values with problems
#'
#' @export
check_all_db <- function(db, slow = FALSE) {
  stopifnot(check_db(db), is.logical(slow))
  .Call(SXPDB_check_db, db, slow)
}

#' Map a function on the values in the database
#'
#' `map_db` runs a function on each of the elements of the database matching a query
#' and return a list of the results.
#'
#' @inheritParams view_db
#' @param fun R function, the function to apply to each value matching the query. It takes on argument, 
#' the value and should return an R value.
#'
#' @seealso [view_db()], [filter_index_db()], [view_meta_db()], [view_origins_db()]
#' @export
map_db <- function(db, fun, query = NULL) {
  stopifnot(check_db(db), is.function(fun))
  .Call(SXPDB_map_db, db, fun, query)
}

#' Filters values from the database according to some predicate
#'
#' Sometimes, the query language is not enough to select some values. You can use `filter_index_db`
#' to refine a query and only keep the values for which the predicate is true.
#'
#' @inheritParams view_db
#' @param fun R function, the predicate, which should take one argument, the value, and return a boolean.
#' ǸA_logical_` is considered as `TRUE`.
#' @returns list of indices of the values matche2d by the query for which `fun` evaluated to `TRUE`
#' or `NA_logical_`.
#' @seealso [map_db()], [view_db()]
#' @export
filter_index_db <- function(db, fun, query = NULL) {
  stopifnot(check_db(db), is.function(fun))
  .Call(SXPDB_filter_index_db, db, fun, query)
}

#' Build search indexes.
#'
#' `build_indexes` explicitly builds search indexes which are used by function with a `query` argument
#' (when it is not `NULL`). You need to use it if you add new values into the database (including merging into it). 
#' However, [merge_all_dbs()] will automatically build the search indexes.
#'
#' @param db database, sxpdb object
#' @returns `NULL`
#'
#' @export
build_indexes <- function(db) {
  stopifnot(check_db(db))
  .Call(SXPDB_build_indexes, db)
}

#' Checks if the database is in write mode
#'
#'
#' The database can be open in read mode, write mode or merge mode. `write_mode` checks if it is open
#' in write mode.
#'
#' @param db database, sxpdb object
#' @returns boolean, `TRUE` if it is in write mode
#'
#' @seealso [open_db()]
#' @export
write_mode <- function(db) {
  stopifnot(check_db(db))
  .Call(SXPDB_write_mode, db)
}

#' Checks if the database has a search index
#'
#'
#' Requests to the database that pass a non-NULL query argument will
#' not work if the search index is not built.
#'
#' @param db database, sxpdb object
#' @returns boolean, `TRUE` if it has a search index
#' @seealso [build_indexes()]
#' @export
has_search_index <- function(db) {
  stopifnot(check_db(db))
  .Call(SXPDB_has_search_index, db)
}

#' Creates a query from an example value.
#'
#' @description
#' `query_from_value` creates a query from an example R value. The query will mimic the value
#' according to the following metadata: type, vector or not, length, class name, `NA` inside or not,
#' number of dimensions, attributes or not. You can then relax on some of those metadata to state
#' that you do not them want to be determined according to the example value, with [relax_query()].
#' The query can then be used with any function taking a `query` argument.
#' 
#' @param value any R value
#' @returns query object
#'
#'
#' @seealso [relax_query()], [query_from_plan()], [close_query()], [view_db()], [map_db()]
#' @export
query_from_value <- function(value) {
  .Call(SXPDB_query_from_value, value)
}

#' Creates a query from a plan.
#'
#' `query_from_plan` creates a query from a plan describing the query.  You can relax later on some
#' parameters of the query with [relax_query()].
#' The query can then be used with any function taking a `query` argument.
#'
#' @param plan named list describing the query. In the absence of a parameter name, the associated metadata
#' will be set to unspecified. The plan parameters are: 
#'   * type: any value of the desired type
#'   * vector: boolean
#'   * length: integer
#'   * class name: character vector of class names, or boolean (to just specify that you want classes, but without specific class names)
#'   * na: boolean
#'   * ndims: integer
#'   * attributes: boolean
#' @returns query object
#' @seealso [query_from_value()], [relax_query()], [close_query()], [view_db()], [map_db()]
#' @export
query_from_plan <- function(plan) {
  .Call(SXPDB_query_from_plan, plan)
}

#' Closes a query object.
#'
#' `close_query` closes a query object. A query object is implemented as an external pointer to a 
#' a C++ object that he dynamically allocated. However, a GC hook for that external pointer is also
#' registered so except if you plan to crate a huge amount of query objects, you should be fine 
#' with not calling `close_query` explicitly.
#'
#' @param query query object
#' @returns `NULL`
#'
#' @seealso [query_from_value()], [query_from_plan()], [relax_query()]
#' @export
close_query <- function(query) {
  .Call(SXPDB_close_query, query)
}

#' Relaxes on some parameters of a query.
#'
#' `relax_query` makes a specified parameter from a query, i.e., with a given target value, into
#' a non-specified parameter. For instance, if the query specifies that it wants values with length
#' 34, relaxing on the length parameter will allow values with any lengths,
#'  not only values with lengths 34.
#'
#' @param query query object
#' @param relax character vector none or several of "na", "length", "attributes", "type", "vector", "ndims", "class". You can
#' also give "keep_type" or "keep_class" to relax on all constraints, except the type, or except the class names.
#'
#' @returns boolean, `TRUE` if the query changed
#' @seealso [query_from_value()], [query_from_plan()], [close_query()], [is_query_empty()]
#' @export
relax_query <- function(query, relax) {
  stopifnot(is.character(relax))
  .Call(SXPDB_relax_query, query, relax)
}

#' Checks if a query object is empty.
#'
#' `is_query_empty` checks if the query refers to some values, or no values at all. You need to call it after at least
#  one call to a function that uses a query or the index cache will not have been initialized.
#' It is typically used when the function using the query returns an ambiguous result (e.g. [sample_val()])
#' and you want to distinguish between `NULL` and an empty query that results also in `NULL`.
#'
#' @param query query object
#' @returns boolean, whether the query is empty or not
#' @seealso [query_from_value()], [query_from_plan()], [relax_query()], [close_query()]
#' @export
is_query_empty <- function(query) {
  .Call(SXPDB_is_query_empty, query)
}


#' Merges several dbs into a new large one.
#'
#' `merge_all_dbs` performs an efficient merge of several databases, to keep only
#' unique values.
#'
#' @param db_paths character vector of paths to the databases to be merged together
#' @param output_path character vector of the path of the resulting database
#' @param parallel boolean, whether the merge should be performed in parallel or not.
#' @returns data frame with information about the merging process. The data frame has the following columns:
#'   * `path`: path of the merged database
#'   * `db_size_before`:  size of the resulting database before merging the db at `path`
#'   * `added_values`: number of new unique values added after merging the db at `path`
#'   * `small_db_size`: number of values in the db at `path`
#'   * `small_db_bytes`: size in bytes of the db at `path`
#'   * `duration`: duration in seconds of merging the db at `path` into the resulting db
#'   * `error`: whether there was an error when merging the db at `path`. 
#' @seealso [merge_into()]
#' @export
merge_all_dbs <- function(db_paths, output_path, parallel = TRUE) {
  stopifnot(is.character(db_paths), is.character(output_path), is.logical(parallel))
  .Call(SXPDB_merge_all_dbs, db_paths, output_path, parallel)
}

#' Fetches all the values corresponding to one origin
#'
#' `values_from_origins` fetches the values that share the given origin, i.e.
#' that are one of the arguments or the return value of `pkg_name::fun_name`.
#'
#' @param db database, sxpdb object
#' @param pkg_name character vector of the name of the package
#' @param fun_name character vector of the name of the function
#' @returns data frame with two columns: 
#'   * `id`: ids of the values
#'   * `param`: parameter names, concatenated and separated with `;`
#'
#' @seealso [view_origins_db()], [values_from_calls()]
#' @export 
values_from_origin <- function(db, pkg_name, fun_name) {
  stopifnot(check_db(db), is.character(pkg_name), is.character(fun_name))
  .Call(SXPDB_values_from_origins, db, pkg_name, fun_name)
}

#' Fetches all the calls corresponding to one origin
#'
#' `values_from_calls`  fetches the values that share the given origin, i.e.
#' that are one of the arguments or the return value of `pkg_name::fun_name`, and
#' identify from which calls they come from.
#'
#' @inheritParams values_from_origin
#' @returns data frame with the following columns: 
#'   * `call_id`: unique id of the call
#'   * `value_id`: unique id of the value
#'   * `param`: parameter name
#'
#' @seealso [values_from_origin()], [view_origins_db()], [view_call_ids()]
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

#'  Get a textual repreentation of an R type
#'
#' `string_sexp_type` shows the character representation of an R type encoded
#' as an integer.
#'
#' @param int_type integer representation of a type
#' @returns character vector 
#' @export
string_sexp_type <- function(int_type) {
  if(int_type < 0 || int_type > 25) {
    return("")
  }
  else {
    types_map[[int_type + 1]]
  }
}


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
