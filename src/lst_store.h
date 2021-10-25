#include <R.h>
#include <Rinternals.h>

#ifndef RCRD_LST_STORE_H
#define RCRD_LST_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Load/create a brand new lst store.
 * @method create_lst_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_lst_store(SEXP lsts);

/**
 * Load an existing lst store.
 * @method load_lst_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_lst_store(SEXP lsts);

/**
 * This functions writes lst R val store to file and closes the file.
 * @method close_lst_store
 * @return R_NilValue on success
 */
SEXP close_lst_store();

/**
 * Load/create a brand new index associated with the lsts store.
 * @method create_lst_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_lst_index(SEXP index);

/**
 * Load an existing index associated with the lsts store.
 * @method load_lst_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_lst_index(SEXP index);

/**
 * This functions merges another str store lsto the current str store.
 * @param  other_lsts is the path to the lsts store of a different db.
 * @param  other_index is the path to the index of other_lsts on disk.
 * @method merge_lst_store
 * @return R_NilValue on success
 */
SEXP merge_lst_store(SEXP other_lsts, SEXP other_index);

/**
 * This function writes the index associated with the lsts store to file
 * and closes the file.
 * @method close_lst_index
 * @return R_NilValue
 */
SEXP close_lst_index();

/**
 * This function assesses if the input is a lst.
 * This function would always return true, therefore no need to implement it.
 * @method is_lst
 * @param  SEXP          Any R value
 * @return               1
 */
int is_lst(SEXP value);

/**
 * Adds an lst R value to the lsts store.
 * @method add_lst
 * @param  val is a lst R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_lst(SEXP val);

/**
 * This function asks if the C layer has seen an given lst value
 * @method have_seen_lst
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_lst(SEXP val);

/**
 * This function gets the lst value at the index'th place in the database.
 * @method get_lst
 * @return R value
 */
SEXP get_lst(int index);

/**
 * This function samples from the lstegers stores in the database
 * @method sample_lst
 * @return R value in form of SEXP or throws an error if no lsteger in database
 */
SEXP sample_lst();

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_LST_STORE_H
