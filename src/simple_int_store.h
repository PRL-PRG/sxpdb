#include <R.h>
#include <Rinternals.h>

#ifndef RCRD_SIMPLE_INT_STORE_H
#define RCRD_SIMPLE_INT_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Load/create a brand new simple integer store.
 * @param  ints is the path to an empty file.
 * @method init_simple_int_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_simple_int_store(SEXP ints);

/**
 * Load an existing simple integer store.
 * @param  ints is the path to an ints store on disk.
 * @method load_simple_int_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_simple_int_store(SEXP ints);

/**
 * This functions merges another ints store into the current int store.
 * @param  other_ints is the path to the ints store on disk of a different db.
 * @method merge_simple_int_store
 * @return R_NilValue on success
 */
SEXP merge_simple_int_store(SEXP other_ints);

/**
 * This functions writes simple int R val store to file and closes the file.
 * @method close_simple_int_store
 * @return R_NilValue on success
 */
SEXP close_simple_int_store();

/**
 * This function assesses if the input is a simple integer.
 * @method is_simple_int
 * @param  SEXP          Any R value
 * @return               1 if it is a simple integer, 0 otherwise
 */
int is_simple_int(SEXP value);

/**
 * Adds a simple integer R value to the simple integer store.
 * @method add_simple_int
 * @param  val is a simple int R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_simple_int(SEXP val);

/**
 * This function asks if the C layer has seen a given simple integer value.
 * @method have_seen_simple_int
 * @param  val       a simple int R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_simple_int(SEXP val);

/**
 * This function gets the simple integer at the index'th place in the database.
 * @method get_simple_int
 * @return R value
 */
SEXP get_simple_int(int index);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_SIMPLE_INT_STORE_H
