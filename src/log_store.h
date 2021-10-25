#include <R.h>
#include <Rinternals.h>

#ifndef RCRD_LOG_STORE_H
#define RCRD_LOG_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Load/create a brand new log store.
 * @method create_log_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_log_store(SEXP logs);

/**
 * Load an existing log store.
 * @method load_log_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_log_store(SEXP logs);

/**
 * This functions writes log R val store to file and closes the file.
 * @method close_log_store
 * @return R_NilValue on success
 */
SEXP close_log_store();

/**
 * Load/create a brand new index associated with the logs store.
 * @method create_log_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_log_index(SEXP index);

/**
 * Load an existing index associated with the logs store.
 * @method load_log_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_log_index(SEXP index);

/**
 * This function writes the index associated with the logs store to file
 * and closes the file.
 * @method close_log_index
 * @return R_NilValue
 */
SEXP close_log_index();

/**
 * This functions merges another str store into the current str store.
 * @param  other_logs is the path to the logs store of a different db.
 * @param  other_index is the path to the index of other_logs on disk.
 * @method merge_log_store
 * @return R_NilValue on success
 */
SEXP merge_log_store(SEXP other_logs, SEXP other_index);

/**
 * This function assesses if the input is a logical.
 * @method is_log
 * @param  SEXP          Any R value
 * @return               1 if the value is a logical, 0 otherwise
 */
int is_log(SEXP value);

/**
 * Adds an log R value to the logs store.
 * @method add_log
 * @param  val is a log R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_log(SEXP val);

/**
 * This function asks if the C layer has seen an given log value
 * @method have_seen_log
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_log(SEXP val);

/**
 * This function gets the log value at the index'th place in the database.
 * @method get_log
 * @return R value
 */
SEXP get_log(int index);

/**
 * This function samples from the logicals stores in the database
 * @method sample_str
 * @return R value in form of SEXP or throws an error if no logical in database
 */
SEXP sample_log();

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_LOG_STORE_H
