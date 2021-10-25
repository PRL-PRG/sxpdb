#include <R.h>
#include <Rinternals.h>

#ifndef RCRD_SIMPLE_DBLS_STORE_H
#define RCRD_SIMPLE_DBLS_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Load/create a brand new simple double store.
 * @method init_simple_dbl_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_simple_dbl_store(SEXP dbls);

/**
 * Load an existing simple double store.
 * @method load_simple_dbl_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_simple_dbl_store(SEXP dbls);

/**
 * This functions merges another dbl store into the current dbl store.
 * @param  other_dbls is the path to the dbls store on disk of a different db.
 * @method merge_simple_dbl_store
 * @return R_NilValue on success
 */
SEXP merge_simple_dbl_store(SEXP other_dbls);

/**
 * This functions writes simple double R val store to file and closes the file.
 * @method close_simple_dbl_store
 * @return R_NilValue on success
 */
SEXP close_simple_dbl_store();

/**
 * This function assesses if the input is a simple double.
 * @method is_simple_dbl
 * @param  SEXP          Any R value
 * @return               1 if it is a simple dbl, 0 otherwise
 */
int is_simple_dbl(SEXP value);

/**
 * Adds a simple double R value to the simple double store.
 * @method add_simple_dbl
 * @param  val is a simple double R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_simple_dbl(SEXP val);

/**
 * This function asks if the C layer has seen a given simple double value.
 * @method have_seen_simple_dbl
 * @param  val       an simple double R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_simple_dbl(SEXP val);

/**
 * This function gets the simple double at the index'th place in the database.
 * @method get_simple_dbl
 * @return R value
 */
SEXP get_simple_dbl(int index);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_SIMPLE_DBLS_STORE_H
