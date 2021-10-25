#include <R.h>
#include <Rinternals.h>

#ifndef RCRD_CMP_STORE_H
#define RCRD_CMP_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Load/create a brand new cmp store.
 * @method create_cmp_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_cmp_store(SEXP cmps);

/**
 * Load an existing cmp store.
 * @method load_cmp_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_cmp_store(SEXP cmps);

/**
 * This functions writes cmp R val store to file and closes the file.
 * @method close_cmp_store
 * @return R_NilValue on success
 */
SEXP close_cmp_store();

/**
 * Load/create a brand new index associated with the cmps store.
 * @method create_cmp_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_cmp_index(SEXP index);

/**
 * Load an existing index associated with the cmps store.
 * @method load_cmp_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_cmp_index(SEXP index);

/**
 * This function writes the index associated with the cmps store to file
 * and closes the file.
 * @method close_cmp_index
 * @return R_NilValue
 */
SEXP close_cmp_index();

/**
 * This functions merges another str store into the current str store.
 * @param  other_cmps is the path to the cmps store of a different db.
 * @param  other_index is the path to the index of other_cmps on disk.
 * @method merge_cmp_store
 * @return R_NilValue on success
 */
SEXP merge_cmp_store(SEXP other_cmps, SEXP other_index);

/**
 * This function assesses if the input is a complex.
 * @method is_cmp
 * @param  SEXP          Any R value
 * @return               1 if the value is a complex, 0 otherwise
 */
int is_cmp(SEXP value);

/**
 * Adds an cmp R value to the cmps store.
 * @method add_cmp
 * @param  val is a cmp R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_cmp(SEXP val);

/**
 * This function asks if the C layer has seen an given cmp value
 * @method have_seen_cmp
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_cmp(SEXP val);

/**
 * This function gets the cmp value at the index'th place in the database.
 * @method get_cmp
 * @return R value
 */
SEXP get_cmp(int index);

/**
 * This function samples from the complexs stores in the database
 * @method sample_str
 * @return R value in form of SEXP or throws an error if no complex in database
 */
SEXP sample_cmp();

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_CMP_STORE_H
