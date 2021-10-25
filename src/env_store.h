#include <R.h>
#include <Rinternals.h>

#ifndef RCRD_ENV_STORE_H
#define RCRD_ENV_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Load/create a brand new env store.
 * @method create_env_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_env_store(SEXP envs);

/**
 * Load an existing env store.
 * @method load_env_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_env_store(SEXP envs);

/**
 * This functions writes env R val store to file and closes the file.
 * @method close_env_store
 * @return R_NilValue on success
 */
SEXP close_env_store();

/**
 * Load/create a brand new index associated with the envs store.
 * @method create_env_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_env_index(SEXP index);

/**
 * Load an existing index associated with the envs store.
 * @method load_env_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_env_index(SEXP index);

/**
 * This functions merges another str store envo the current str store.
 * @param  other_envs is the path to the envs store of a different db.
 * @param  other_index is the path to the index of other_envs on disk.
 * @method merge_env_store
 * @return R_NilValue on success
 */
SEXP merge_env_store(SEXP other_envs, SEXP other_index);

/**
 * This function writes the index associated with the envs store to file
 * and closes the file.
 * @method close_env_index
 * @return R_NilValue
 */
SEXP close_env_index();

/**
 * This function assesses if the input is a env.
 * This function would always return true, therefore no need to implement it.
 * @method is_env
 * @param  SEXP          Any R value
 * @return               1
 */
int is_env(SEXP value);

/**
 * Adds an env R value to the envs store.
 * @method add_env
 * @param  val is a env R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_env(SEXP val);

/**
 * This function asks if the C layer has seen an given env value
 * @method have_seen_env
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_env(SEXP val);

/**
 * This function gets the env value at the index'th place in the database.
 * @method get_env
 * @return R value
 */
SEXP get_env(int index);

/**
 * This function samples from the envegers stores in the database
 * @method sample_env
 * @return R value in form of SEXP or throws an error if no enveger in database
 */
SEXP sample_env();

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_ENV_STORE_H
