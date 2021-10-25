#include <R.h>
#include <Rinternals.h>

#ifndef RCRD_STR_STORE_H
#define RCRD_STR_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Load/create a brand new str store.
 * @method create_str_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_str_store(SEXP strs);

/**
 * Load an existing str store.
 * @method load_str_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_str_store(SEXP strs);

/**
 * This functions writes str R val store to file and closes the file.
 * @method close_str_store
 * @return R_NilValue on success
 */
SEXP close_str_store();

/**
 * Load/create a brand new index associated with the strs store.
 * @method create_str_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_str_index(SEXP index);

/**
 * Load an existing index associated with the strs store.
 * @method load_str_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_str_index(SEXP index);

/**
 * This functions merges another str store stro the current str store.
 * @param  other_strs is the path to the strs store of a different db.
 * @param  other_index is the path to the index of other_strs on disk.
 * @method merge_str_store
 * @return R_NilValue on success
 */
SEXP merge_str_store(SEXP other_strs, SEXP other_index);

/**
 * This function writes the index associated with the strs store to file
 * and closes the file.
 * @method close_str_index
 * @return R_NilValue
 */
SEXP close_str_index();

/**
 * This function assesses if the input is a str.
 * This function would always return true, therefore no need to implement it.
 * @method is_str
 * @param  SEXP          Any R value
 * @return               1
 */
int is_str(SEXP value);

/**
 * Adds an str R value to the strs store.
 * @method add_str
 * @param  val is a str R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_str(SEXP val);

/**
 * This function asks if the C layer has seen an given str value
 * @method have_seen_str
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_str(SEXP val);

/**
 * This function gets the str value at the index'th place in the database.
 * @method get_str
 * @return R value
 */
SEXP get_str(int index);

/**
 * This function samples from the strings stores in the database
 * @method sample_str
 * @return R value in form of SEXP or throws an error if no streger in database
 */
SEXP sample_str();

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_STR_STORE_H
