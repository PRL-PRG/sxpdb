#include <R.h>
#include <Rinternals.h>

#ifndef RCRD_INT_STORE_H
#define RCRD_INT_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Load/create a brand new int store.
 * @method create_int_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_int_store(SEXP ints);

/**
 * Load an existing int store.
 * @method load_int_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_int_store(SEXP ints);

/**
 * This functions writes int R val store to file and closes the file.
 * @method close_int_store
 * @return R_NilValue on success
 */
SEXP close_int_store();

/**
 * Load/create a brand new index associated with the ints store.
 * @method create_int_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_int_index(SEXP index);

/**
 * Load an existing index associated with the ints store.
 * @method load_int_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_int_index(SEXP index);

/**
 * This functions merges another str store into the current str store.
 * @param  other_ints is the path to the ints store of a different db.
 * @param  other_index is the path to the index of other_ints on disk.
 * @method merge_int_store
 * @return R_NilValue on success
 */
SEXP merge_int_store(SEXP other_ints, SEXP other_index);

/**
 * This function writes the index associated with the ints store to file
 * and closes the file.
 * @method close_int_index
 * @return R_NilValue
 */
SEXP close_int_index();

/**
 * This function assesses if the input is a int.
 * This function would always return true, therefore no need to implement it.
 * @method is_int
 * @param  SEXP          Any R value
 * @return               1
 */
int is_int(SEXP value);

/**
 * Adds an int R value to the ints store.
 * @method add_int
 * @param  val is a int R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_int(SEXP val);

/**
 * This function asks if the C layer has seen an given int value
 * @method have_seen_int
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_int(SEXP val);

/**
 * This function gets the int value at the index'th place in the database.
 * @method get_int
 * @return R value
 */
SEXP get_int(int index);

/**
 * This function samples from the integers stores in the database
 * @method sample_int
 * @return R value in form of SEXP or throws an error if no integer in database
 */
SEXP sample_int();

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_INT_STORE_H
