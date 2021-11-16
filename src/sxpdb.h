#define R_NO_REMAP
#include <Rinternals.h>

#ifndef SXPDB_SXPDB_H
#define SXPDB_SXPDB_H

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
 * Average duration for insertion of a new value
 * @method avg_insertion_duration
 * @param db external pointer to the target database
 * @return average duration in milliseconds
 */
SEXP avg_insertion_duration(SEXP sxpdb);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // SXPDB_SXPDB_H

