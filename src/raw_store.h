#include <R.h>
#include <Rinternals.h>

#ifndef RCRD_RAW_STORE_H
#define RCRD_RAW_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Load/create a brand new raw store.
 * @method create_raw_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_raw_store(SEXP raws);

/**
 * Load an existing raw store.
 * @method load_raw_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_raw_store(SEXP raws);

/**
 * This functions writes raw R val store to file and closes the file.
 * @method close_raw_store
 * @return R_NilValue on success
 */
SEXP close_raw_store();

/**
 * Load/create a brand new index associated with the raws store.
 * @method create_raw_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_raw_index(SEXP index);

/**
 * Load an existing index associated with the raws store.
 * @method load_raw_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_raw_index(SEXP index);

/**
 * This functions merges another str store rawo the current str store.
 * @param  other_raws is the path to the raws store of a different db.
 * @param  other_index is the path to the index of other_raws on disk.
 * @method merge_raw_store
 * @return R_NilValue on success
 */
SEXP merge_raw_store(SEXP other_raws, SEXP other_index);

/**
 * This function writes the index associated with the raws store to file
 * and closes the file.
 * @method close_raw_index
 * @return R_NilValue
 */
SEXP close_raw_index();

/**
 * This function assesses if the input is a raw.
 * This function would always return true, therefore no need to implement it.
 * @method is_raw
 * @param  SEXP          Any R value
 * @return               1
 */
int is_raw(SEXP value);

/**
 * Adds an raw R value to the raws store.
 * @method add_raw
 * @param  val is a raw R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_raw(SEXP val);

/**
 * This function asks if the C layer has seen an given raw value
 * @method have_seen_raw
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_raw(SEXP val);

/**
 * This function gets the raw value at the index'th place in the database.
 * @method get_raw
 * @return R value
 */
SEXP get_raw(int index);

/**
 * This function samples from the rawegers stores in the database
 * @method sample_raw
 * @return R value in form of SEXP or throws an error if no raweger in database
 */
SEXP sample_raw();

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_RAW_STORE_H
