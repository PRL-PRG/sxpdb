#ifndef SXPDB_SXPDB_H
#define SXPDB_SXPDB_H

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include <Rversion.h>
#include <stdint.h>


#ifdef __cplusplus
extern "C" {
#endif


/**
 * This function opens a database
 * This function must be called first.
 * @method open_db
 * @param filename
 * @param mode boolean or "merge" open the database in write (T), read (F) or merge mode. Read and merge mode do not load into memory as much
 * @param quiet boolean print helpful messages or not
 * @return R_NilValue on error, a external pointer to the database on success
 */
SEXP open_db(SEXP filename, SEXP mode, SEXP quiet);

/**
 * This function closes the database, materializes totally on disk.
 * This function must be called last.
 * @method close_db
 * @param db Extenral pointer to the database object
 * @return R_NilValue on succecss
 */
SEXP close_db(SEXP db);

/**
 * This functions adds an R value to the database.
 * @method add_val
 * @param  db       external pointer to the database
 * @param  val      R value in form of SEXP
 * @return          index  of the value if it has been added to the db (now or before),
 *                  else R_NilValue
 */
SEXP add_val(SEXP db, SEXP val);

/**
 * This function asks if the C layer has seen a R value.
 * This function is mainly used for testing.
 * @method have_seen
 * @param  db       external pointer to the database
 * @param  val       R value in form of SEXP
 * @return integer  or R_NilValue index of the value in the db if is inside, NULL otherwise
 */
SEXP have_seen(SEXP db, SEXP val);

/**
 * This function returns a random value from the database
 * @method sample_val
 * @param  db       external pointer to the database
 * @param query external pointer or NULL, sample only among the values matching the query or in the whole databse if NULL
 * @return R value in form of SEXP from the database
 */
SEXP sample_val(SEXP db, SEXP query);

/**
 * This function returns an index to a random value from the database
 * @method sample_val
 * @param  db       external pointer to the database
 * @param query external pointer or NULL, sample only among the values matching the query or in the whole databse if NULL
 * @return integer or NULL
 */
SEXP sample_index(SEXP db, SEXP query);

/**
 * This function returns a random value from the database,
 * but similar to the provided value
 * @method sample_val
 * @param  db       external pointer to the database
 * @param val  a SEXP
 * @param multiple boolean, whether val is actual value or a list of values we will do the union of
 * @param relax character vector none or several of "na", "length", "attributes", "type", "vector", "ndims", "class". You can
 * also give "keep_type" or "keep_class" to relax on all constraints, except the type, or except the class names.
 * It will relax the given constraints inferred from the example value.
 * @return R value in form of SEXP from the database, R_NilValue if no similar value was found
 */
SEXP sample_similar(SEXP db, SEXP val, SEXP multiple, SEXP relax);


/**
 * This function returns a value from the database specified by an order
 * @method get_val
 * @param  db       external pointer to the database
 * @param  i       R value in form of SEXP that is an index,
 *                 it must be non-negative
 * @return R value in form of SEXP from the database at ith position
 */
SEXP get_val(SEXP db, SEXP i);

/**
 * Merges db2 into db1
 * @method merge_db
 * @param  db1       external pointer to the target database
 * @param  db2       external pointer to the source  database
 * @return R_NilValue if error, number of values in the database otherwise
 */
SEXP merge_db(SEXP db1, SEXP db2);

/**
 * Number of values in the database
 * @method size_db
 * @param  db       external pointer to the target database
 * @return R_NilValue if error, number of values in the database otherwise
 */
SEXP size_db(SEXP db);

/**
 * Metadata associated to a value
 * @method get_meta
 * @param  db       external pointer to the target database
 * @param val       value to look for
 * @return R_NilValue if error or no such value in the database, metadata otherwise, as a named list
 */
SEXP get_meta(SEXP db, SEXP val);

/**
 * Metadata associated to a value
 * @method get_meta
 * @param  db       external pointer to the target database
 * @param idx       index of the value to look for
 * @return R_NilValue if error or no such value in the database, metadata otherwise, as a named list
 */
SEXP get_meta_idx(SEXP db, SEXP idx);


/**
 * Add a value along with origin information
 * @method add_val_origin
 * @param db external pointer to the target database
 * @param val value to insert
 * @param package name
 * @param function name
 * @param argument name ; "" or NA mean that it is a return value
 * @param call_id integer or double
 * @return index of the value if it was added, NULL otherwise
 */
SEXP add_val_origin(SEXP sxpdb, SEXP val, SEXP package, SEXP function, SEXP argument, SEXP call_id);

SEXP add_val_origin_(SEXP sxpdb, SEXP val,
                     const char* package_name, const char* function_name, const char* argument_name,
                     uint64_t call_id);

/**
 * Get the origins of a value using its hash
 * @method get_origins
 * @param sxpdb external pointer to the target database
 * @param hash_s hash of a value
 * @return data frame with columns package, function, argument. Argument is NA_String if it is a return value
 */
SEXP get_origins(SEXP sxpdb, SEXP hash_s);

/**
 * Get the origins of a value using its index
 * @method get_origins_idx
 * @param sxpdb external pointer to the target database
 * @param idx index in the database (should be < to the number of values)
 * @return data frame with columns package, function, argument. Argument is NA_String if it is a return value
 */
SEXP get_origins_idx(SEXP sxpdb, SEXP idx);


/**
 * Add origins to a value identified by its hash
 * @method get_origins_idx
 * @param sxpdb external pointer to the target database
 * @param hash pointer to a sexp_hash
 * @param package name
 * @param function name
 * @param argument name ; "" or NA mean that it is a return value
 * @return NULL
 */
SEXP add_origin_(SEXP sxpdb, const void* hash, const char* package_name, const char* function_name, const char* argument_name);

SEXP add_origin(SEXP sxpdb, SEXP hash, SEXP package, SEXP function, SEXP argument);

/**
 * Gives the path to the directory of the database
 * @method path_db
 * @param sxpdb external pointer to the target database
 * @return character path to the database
 */
SEXP path_db(SEXP sxpdb);

/**
 * Check the database
 * @method check_db
 * @param sxpdb external pointer to the target database
 * @param slow boolean enables slow checks
 * @return integer vector of indices to values with problems
 */
SEXP check_db(SEXP sxpdb, SEXP slow);

/**
 * Map over the values of the database
 * @method map_db
 * @param sxpdb external pointer to the target database
 * @param fun function to run on each value in the database
 * @param query externalptr or NULL restrict the values to map on to the ones matching the query
 * @return list of results of applying fun to the values in the database
 */
SEXP map_db(SEXP sxpdb, SEXP fun, SEXP query);

/**
 * Map over the values of the database
 * @method filter_index_db
 * @param sxpdb external pointer to the target database
 * @param fun function to run on each value in the database, should return a boolean
 * @param query externalptr or NULL restrict the values to map on to the ones matching the query
 * @return list of indices of the value for which the function evaluated to true
 */
SEXP filter_index_db(SEXP sxpdb, SEXP fun, SEXP query);

/**
 * @method view_db
 * @param sxpdb external pointer to the target database
 * @param query external pointer or NULL restrict the values to view to the ones matching the query
 * @return a list of all the values in the database
 */
SEXP view_db(SEXP sxpdb, SEXP query);

/**
 * @method view_metadata
 * @param sxpdb external pointer to the target database
 * 2param query external pointer or NULL restrict the metadata to the values matching the query
 * @return a data frame of the metadata for all the values, in the same order as view_db or map_db
 */
SEXP view_metadata(SEXP sxpdb, SEXP query);

/**
 * @method view_origins
 * @param sxpdb external pointer to the target database
 * @param query external pointer or NULL restrict the origins to the ones of the values matching the query
 * @return a data frame of the origins for all the values, in the same order as view_db or map_db. A column id indicates the index of the value
 */
SEXP view_origins(SEXP sxpdb, SEXP query);

/**
 * @method view_call_ids
 * @param sxpdb external pointer to the target database
 * @param query external pointer or NULL restrict the call ids to the ones of the values matching the query.
 *  Query currently is not taken into account for this function
 * @return a data frame of the call ids for all the values, in the same order as view_db or map_db. 
 */
SEXP view_call_ids(SEXP sxpdb, SEXP query);

/**
 * @method view_db_names
 * @param sxpdb external pointer to the target database
 * @param query external pointer or NULL restrict the call ids to the ones of the values matching the query.
 *  Query currently is not taken into account for this function
 * @return a data frame of the db names for all the values, in the same order as view_db or map_db. 
 */
SEXP view_db_names(SEXP sxpdb, SEXP query);

/**
 * @method build_indexes
 * @param sxpdb external pointer to the target database
 * @return R_NilValue
 */
SEXP build_indexes(SEXP sxpdb);

/**
 * @method write_mode
 * @param sxpdb external pointer to the target database
 * @return boolean whether the database is in write mode, or read only mode
 */
SEXP write_mode(SEXP sxpdb);

/**
 * @method query_from_value
 * @param value R value to build the query from
 * @return external pointer to the query object
 */
SEXP query_from_value(SEXP value);

/**
 * @method close_query
 * @param query external pointer to the query
 * @return R_Nil_value
 */
SEXP close_query(SEXP query);

/**
 * @method relax_query
 * It will modify the query in place
 * @param query external pointer to the query object
 * @param relax character vector none or several of "na", "length", "attributes", "type", "vector", "ndims", "class". You can
 * also give "keep_type" or "keep_class" to relax on all constraints, except the type, or except the class names.
 * It will relax the given constraints inferred from the example value.
 * @return boolean, true if the query changed or not
 */
SEXP relax_query(SEXP query, SEXP relax);


/**
 * @method is_query_empty
 * @param query external pointer to the query
 * @return true if the query will have no results, has had no results. You need to call it after at least
 * one call to sample_from_query or the index cache will not have been initialized.
 */
SEXP is_query_empty(SEXP query);

/**
 * @method extptr_tag
 * @param ptr external pointer
 * @return tag of the external pointer
 */
SEXP extptr_tag(SEXP ptr);

/**
 * @method merge_all_dbs
 * 
 * @param db_paths character vector paths of the dbs to be merged
 * @param output_path character path of the resulting db
 * @param parallel logical, whether the merge should be performed in parallel or not
 * @return data frame of the merged dbs
 */
SEXP merge_all_dbs(SEXP db_paths, SEXP output_path, SEXP parallel);

/**
 * @method values_from_origins
 * 
 * @param sxpdb external pointer to the target database
 * @param pkg character name of the package
 * @param fun character name of the function
 * @return data frame with column with the ids of the values corresponding to that origin and with the parameter names
 */
SEXP values_from_origins(SEXP sxpdb, SEXP pkg, SEXP fun);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // SXPDB_SXPDB_H

