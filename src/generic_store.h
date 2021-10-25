#include <R.h>
#include <Rinternals.h>

#ifndef RCRD_GENERIC_STORE_H
#define RCRD_GENERIC_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Load/create a brand new generic store.
 * @method create_generic_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_generic_store(SEXP generics);

/**
 * Load an existing generic store.
 * @method load_generic_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_generic_store(SEXP generics);

/**
 * This functions writes generic R val store to file and closes the file.
 * @method close_generic_store
 * @return R_NilValue on success
 */
SEXP close_generic_store();

/**
 * Load/create a brand new index associated with the generics store.
 * @method create_generic_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_generic_index(SEXP index);

/**
 * Load an existing index associated with the generics store.
 * @method load_generic_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_generic_index(SEXP index);

/**
 * This function writes the index associated with the generics store to file
 * and closes the file.
 * @method close_generic_index
 * @return R_NilValue
 */
SEXP close_generic_index();

/**
 * This functions merges another str store into the current str store.
 * @param  other_generics is the path to the generics store of a different db.
 * @param  other_index is the path to the index of other_generics on disk.
 * @method merge_generic_store
 * @return R_NilValue on success
 */
SEXP merge_generic_store(SEXP other_generics, SEXP other_index);

/**
 * This function assesses if the input is a generic.
 * This function would always return true, therefore no need to implement it.
 * @method is_generic
 * @param  SEXP          Any R value
 * @return               1
 */
// int is_generic(SEXP value);

/**
 * Adds an generic R value to the generics store.
 * @method add_generic
 * @param  val is a generic R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_generic(SEXP val);

/**
 * This function asks if the C layer has seen an given generic value
 * @method have_seen_generic
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_generic(SEXP val);

/**
 * This function gets the generic value at the index'th place in the database.
 * @method get_generic
 * @return R value
 */
SEXP get_generic(int index);

/**
 * This function samples from the generic stores in the database
 * @method sample_str
 * @return R value in form of SEXP or throws an error if no generic in database
 */
SEXP sample_generic();

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_GENERIC_STORE_H
