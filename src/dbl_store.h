#include <R.h>
#include <Rinternals.h>

#ifndef RCRD_DBL_STORE_H
#define RCRD_DBL_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Load/create a brand new dbl store.
 * @method create_dbl_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_dbl_store(SEXP dbls);

/**
 * Load an existing dbl store.
 * @method load_dbl_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_dbl_store(SEXP dbls);

/**
 * This functions writes dbl R val store to file and closes the file.
 * @method close_dbl_store
 * @return R_NilValue on success
 */
SEXP close_dbl_store();

/**
 * Load/create a brand new index associated with the dbls store.
 * @method create_dbl_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_dbl_index(SEXP index);

/**
 * Load an existing index associated with the dbls store.
 * @method load_dbl_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_dbl_index(SEXP index);

/**
 * This functions merges another str store dblo the current str store.
 * @param  other_dbls is the path to the dbls store of a different db.
 * @param  other_index is the path to the index of other_dbls on disk.
 * @method merge_dbl_store
 * @return R_NilValue on success
 */
SEXP merge_dbl_store(SEXP other_dbls, SEXP other_index);

/**
 * This function writes the index associated with the dbls store to file
 * and closes the file.
 * @method close_dbl_index
 * @return R_NilValue
 */
SEXP close_dbl_index();

/**
 * This function assesses if the input is a dbl.
 * This function would always return true, therefore no need to implement it.
 * @method is_dbl
 * @param  SEXP          Any R value
 * @return               1
 */
int is_dbl(SEXP value);

/**
 * Adds an dbl R value to the dbls store.
 * @method add_dbl
 * @param  val is a dbl R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_dbl(SEXP val);

/**
 * This function asks if the C layer has seen an given dbl value
 * @method have_seen_dbl
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_dbl(SEXP val);

/**
 * This function gets the dbl value at the index'th place in the database.
 * @method get_dbl
 * @return R value
 */
SEXP get_dbl(int index);

/**
 * This function samples from the dblegers stores in the database
 * @method sample_dbl
 * @return R value in form of SEXP or throws an error if no dbleger in database
 */
SEXP sample_dbl();

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_DBL_STORE_H
