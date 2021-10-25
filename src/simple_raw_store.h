#include <R.h>
#include <Rinternals.h>

#ifndef RCRD_SIMPLE_RAW_STORE_H
#define RCRD_SIMPLE_RAW_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Load/create a brand new simple raw store.
 * @method init_simple_raw_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_simple_raw_store(SEXP file);

/**
 * Load an existing simple raw store.
 * @method load_simple_raw_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_simple_raw_store(SEXP raw);

/**
 * This functions merges another raws store into the current raws store.
 * @param  other_raws is the path to the raws store on disk of a different db.
 * @method merge_simple_dbl_store
 * @return R_NilValue on success
 */
SEXP merge_simple_raw_store(SEXP other_raws);

/**
 * This functions writes simple raw R val store to file and closes the file.
 * @method close_simple_raw_store
 * @return R_NilValue on success
 */
SEXP close_simple_raw_store();

/**
 * This function assesses if the input is a simple raw.
 * @method is_simple_raw
 * @param  SEXP          Any R value
 * @return               1 if it is a simple raw, 0 otherwise
 */
int is_simple_raw(SEXP value);

/**
 * Adds a simple integer R value to the simple raw store.
 * @method add_simple_raw
 * @param  val is an R raw value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_simple_raw(SEXP val);

/**
 * This function asks if the C layer has seen a given simple raw value.
 * @method have_seen_simple_raw
 * @param  val       R raw value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_simple_raw(SEXP val);

/**
 * This function gets the simple raw at the index'th place in the database.
 * @method get_simple_raw
 * @return R value
 */
SEXP get_simple_raw(int index);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_SIMPLE_RAW_STORE_H
