#include <R.h>
#include <Rinternals.h>

#ifndef RCRD_FUN_STORE_H
#define RCRD_FUN_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Load/create a brand new fun store.
 * @method create_fun_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_fun_store(SEXP funs);

/**
 * Load an existing fun store.
 * @method load_fun_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_fun_store(SEXP funs);

/**
 * This functions writes fun R val store to file and closes the file.
 * @method close_fun_store
 * @return R_NilValue on success
 */
SEXP close_fun_store();

/**
 * Load/create a brand new index associated with the funs store.
 * @method create_fun_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_fun_index(SEXP index);

/**
 * Load an existing index associated with the funs store.
 * @method load_fun_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_fun_index(SEXP index);

/**
 * This functions merges another str store funo the current str store.
 * @param  other_funs is the path to the funs store of a different db.
 * @param  other_index is the path to the index of other_funs on disk.
 * @method merge_fun_store
 * @return R_NilValue on success
 */
SEXP merge_fun_store(SEXP other_funs, SEXP other_index);

/**
 * This function writes the index associated with the funs store to file
 * and closes the file.
 * @method close_fun_index
 * @return R_NilValue
 */
SEXP close_fun_index();

/**
 * This function assesses if the input is a fun.
 * This function would always return true, therefore no need to implement it.
 * @method is_fun
 * @param  SEXP          Any R value
 * @return               1
 */
int is_fun(SEXP value);

/**
 * Adds an fun R value to the funs store.
 * @method add_fun
 * @param  val is a fun R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_fun(SEXP val);

/**
 * This function asks if the C layer has seen an given fun value
 * @method have_seen_fun
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_fun(SEXP val);

/**
 * This function gets the fun value at the index'th place in the database.
 * @method get_fun
 * @return R value
 */
SEXP get_fun(int index);

/**
 * This function samples from the funegers stores in the database
 * @method sample_fun
 * @return R value in form of SEXP or throws an error if no funeger in database
 */
SEXP sample_fun();

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_FUN_STORE_H
