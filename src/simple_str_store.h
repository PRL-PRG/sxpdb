#include <R.h>
#include <Rinternals.h>

#ifndef RCRD_SIMPLE_STR_STORE_H
#define RCRD_SIMPLE_STR_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Load/create a brand new simple string store.
 * @method init_simple_str_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_simple_str_store(SEXP generics);

/**
 * Load an existing simple string store.
 * @method load_simple_str_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_simple_str_store(SEXP generics);

/**
 * This functions merges another str store into the current str store.
 * @param  other_strs is the path to the strs store on disk of a different db.
 * @param  other_index is the path to the index of other_strs on disk
 * @method merge_simple_str_store
 * @return R_NilValue on success
 */
SEXP merge_simple_str_store(SEXP other_strs, SEXP other_index);

/**
 * This functions writes simple string store to file and closes the file.
 * @method close_simple_str_store
 * @return R_NilValue on success
 */
SEXP close_simple_str_store();

/**
 * Load/create a brand new index associated with the simple string store.
 * @method init_simple_str_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_simple_str_index(SEXP index);

/**
 * Load an existing index associated with the simple string store.
 * @method load_simple_str_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_simple_str_index(SEXP index);

/**
 * This function writes the index associated with the simple str store to file
 * and closes the file.
 * @method close_simple_str_index
 * @return R_NilValue
 */
SEXP close_simple_str_index();

/**
 * This function assesses if the input is a simple string.
 * This function would always return true, therefore no need to implement it.
 * @method is_simple_str
 * @param  SEXP          Any R value
 * @return               1 if the value is a simple string, else 0
 */
int is_simple_str(SEXP value);

/**
 * Adds an simple string value to the simple string store.
 * @method add_simple_str
 * @param  val is a generic R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_simple_str(SEXP val);

/**
 * This function asks if the C layer has seen an given simple string
 * @method have_seen_simple_str
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_simple_str(SEXP val);

/**
 * This function gets the simple string at the index'th place in the database.
 * @method get_simple_str
 * @return R value
 */
SEXP get_simple_str(int index);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_SIMPLE_STR_STORE_H
