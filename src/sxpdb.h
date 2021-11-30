#ifndef SXPDB_SXPDB_H
#define SXPDB_SXPDB_H

#define R_NO_REMAP
#include <Rinternals.h>


#ifdef __cplusplus
extern "C" {
#endif


/**
 * This function opens a database
 * This function must be called first.
 * @method open_db
 * @param filename
 * @return R_NilValue on error, a external pointer to the database on success
 */
SEXP open_db(SEXP filename);

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
 * @return          val if val hasn't been added to database before,
 *                  else R_NilValue
 */
SEXP add_val(SEXP db, SEXP val);

/**
 * This function asks if the C layer has seen a R value.
 * This function is mainly used for testing.
 * @method have_seen
 * @param  db       external pointer to the database
 * @param  val       R value in form of SEXP
 * @return           R value of True or False as a SEXP
 */
SEXP have_seen(SEXP db, SEXP val);

/**
 * This function returns a random value from the database
 * @method sample_val
 * @param  db       external pointer to the database
 * @return R value in form of SEXP from the database
 */
SEXP sample_val(SEXP db);

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
 * @return average duration in milliseconds
 */
SEXP add_val_origin(SEXP sxpdb, SEXP val, SEXP package, SEXP function, SEXP argument);

SEXP add_val_origin_(SEXP sxpdb, SEXP val,
                     const char* package_name, const char* function_name, const char* argument_name);

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
 * @return Boolean TRUE if its a new origin for this hash
 */
SEXP add_origin_(SEXP sxpdb, const void* hash, const char* package_name, const char* function_name, const char* argument_name);

SEXP add_origin(SEXP sxpdb, SEXP hash, SEXP package, SEXP function, SEXP argument);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // SXPDB_SXPDB_H

