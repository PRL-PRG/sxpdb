#include <R.h>
#include <Rinternals.h>

#ifndef RCRD_STATS_STORE_H
#define RCRD_STATS_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Load/create a brand new stats store.
 * @method init_stats_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_stats_store(SEXP stats);

/**
 * Load an existing stats store.
 * @method load_stats_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_stats_store(SEXP stats);

/**
 * This functions adjust information in the stats store based on the merge.
 * @method merge_stats_store
 * @return R_NilValue on success
 */
SEXP merge_stats_store(SEXP other_stats);

/**
 * This functions writes database statistics to file and closes the file.
 * @method close_stats_store
 * @return R_NilValue on success
 */
SEXP close_stats_store();

/**
 * This function samples from the "null store" in the database
 * @method sample_null
 * @return R value in form of SEXP or throws an error if no generic in database
 */
SEXP sample_null();

/**
 * Report database statistics
 * @method print_report
 */
SEXP print_report();

/**
 * This function asks for how many R values the C add_val has seen.
 * @method count_vals
 * @return number of times add_val was called
 */
SEXP count_vals();

/**
 * This function asks for how many values are stored in the database
 * @method size_db
 * @return Non-zero numeric R value in form of a SEXP
 */
SEXP size_db();

/**
 * This function asks for how many simple integer values stored in the database
 * @method size_ints
 * @return Non-zero numeric R value in form of a SEXP
 */
SEXP size_ints();

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_STATS_STORE_H
