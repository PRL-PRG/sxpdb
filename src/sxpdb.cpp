#include "sxpdb.h"

#include "byte_vector.h"
#include "helper.h" // rans_d_size_t

// Include all stores
#include "stats_store.h"
#include "generic_store.h"

#include "int_store.h"
#include "simple_int_store.h"

#include "dbl_store.h"
#include "simple_dbl_store.h"

#include "raw_store.h"
#include "simple_raw_store.h"

#include "str_store.h"
#include "simple_str_store.h"

#include "log_store.h"

#include "cmp_store.h"

#include "lst_store.h"

#include "env_store.h"

#include "fun_store.h"

// Reusable buffer for everything
byte_vector_t vector = NULL;

// Pulled in from stats_store.cpp
extern size_t count;
extern size_t size;

extern size_t i_size;
extern size_t s_i_size;

extern size_t d_size;
extern size_t s_d_size;

extern size_t r_size;
extern size_t s_r_size;

extern size_t s_size;
extern size_t s_s_size;

extern size_t o_size;

extern size_t c_size;

extern size_t l_size;

extern size_t e_size;

extern size_t u_size;

extern size_t n_size;
extern size_t n_count;

extern size_t g_size;

/**
 * This function sets up the initial state of the database.
 * This function must be called first.
 * @method setup
 * @export
 * @return R_NilValue on success
 */
SEXP setup() {
	vector = make_vector(1 << 30);

	return R_NilValue;
}

/**
 * This function tears down all traces of the database after running.
 * This function must be called last.
 * @method setup
 * @return R_NilValue on success
 */
SEXP teardown() {
	if (vector) {
		free_vector(vector);
		vector = NULL;
	}

	return R_NilValue;
}

/**
 * This functions adds an R value to the database.
 * @method add_val
 * @param  val      R value in form of SEXP
 * @return          val if val hasn't been added to database before,
 *                  else R_NilValue
 */
SEXP add_val(SEXP val) {
	count += 1;

	if (TYPEOF(val) == FREESXP) { // This should never happen
		return R_NilValue;
	} else if (TYPEOF(val) == NILSXP) {
		if (n_count == 0) {
			size++;
			n_size++;
		}
		n_count++;
		return R_NilValue;
	} else if (is_int(val)) {
		return add_int(val);
	} else if (is_dbl(val)) {
		return add_dbl(val);
	} else if (is_raw(val)) {
		return add_raw(val);
	} else if (is_str(val)) {
		return add_str(val);
	} else if (is_log(val)) {
		return add_log(val);
	} else if (is_cmp(val)) {
		return add_cmp(val);
	} else if (is_lst(val)) {
		return add_lst(val);
	} else if (is_env(val)) {
		return add_env(val);
	} else if (is_fun(val)) {
		return add_fun(val);
	} else {
		return add_generic(val);
	}
}

/**
 * This function asks if the C layer has seen a R value.
 * This function is mainly used for testing.
 * @method have_seen
 * @param  val       R value in form of SEXP
 * @return           R value of True or False as a SEXP
 */
SEXP have_seen(SEXP val) {
	SEXP res = PROTECT(allocVector(LGLSXP, 1));
	int *res_ptr = LOGICAL(res);

	// TODO: Add have_seen interface for the various databases
	if (TYPEOF(val) == NILSXP) {
		res_ptr[0] = n_size;
	} else if (is_int(val)) {
		res_ptr[0] = have_seen_int(val);
	} else if (is_dbl(val)) {
		res_ptr[0] = have_seen_dbl(val);
	} else if (is_raw(val)) {
		res_ptr[0] = have_seen_raw(val);
	} else if (is_str(val)) {
		res_ptr[0] = have_seen_str(val);
	} else if (is_log(val)) {
		res_ptr[0] = have_seen_log(val);
	} else if (is_cmp(val)) {
		res_ptr[0] = have_seen_cmp(val);
	} else if (is_lst(val)) {
		res_ptr[0] = have_seen_lst(val);
	} else if (is_env(val)) {
		res_ptr[0] = have_seen_env(val);
	} else if (is_fun(val)) {
		res_ptr[0] = have_seen_fun(val);
	} else {
		res_ptr[0] = have_seen_generic(val);
	}

	UNPROTECT(1);
	return res;
}

/**
 * This function returns a random value from the database
 * @method sample_val
 * @return R value in form of SEXP from the database
 */
SEXP sample_val() {
	// TODO: Add error checking (e.g. size == 0)
	size_t random_index = rand_size_t() % size;

	if (random_index < n_size) {
		return R_NilValue;
	} else {
		random_index -= n_size;
	}

	if (random_index < i_size) {
		return get_int(random_index);
	} else {
		random_index -= i_size;
	}

	if (random_index < d_size) {
		return get_dbl(random_index);
	} else {
		random_index -= d_size;
	}

	if (random_index < r_size) {
		return get_raw(random_index);
	} else {
		random_index -= r_size;
	}

	if (random_index < s_size) {
		return get_str(random_index);
	} else {
		random_index -= s_size;
	}

	if (random_index < o_size) {
		return get_log(random_index);
	} else {
		random_index -= o_size;
	}

	if (random_index < c_size) {
		return get_cmp(random_index);
	} else {
		random_index -= c_size;
	}

	if (random_index < l_size) {
		return get_lst(random_index);
	} else {
		random_index -= l_size;
	}

	if (random_index < e_size) {
		return get_env(random_index);
	} else {
		random_index -= e_size;
	}

	if (random_index < u_size) {
		return get_env(random_index);
	} else {
		random_index -= u_size;
	}

	return get_generic(random_index);
}

/**
 * This function returns a value from the database specified by an order
 * @method get_val
 * @param  i       R value in form of SEXP that is an index,
 *                 it must be non-negative
 * @return R value in form of SEXP from the database at ith position
 */
SEXP get_val(SEXP i) {
	// TODO: Add error checking (e.g. size == 0)
	int index = asInteger(i);

	if (index < n_size) {
		return R_NilValue;
	} else {
		index -= n_size;
	}

	if (index < i_size) {
		return get_int(index);
	} else {
		index -= i_size;
	}

	if (index < d_size) {
		return get_dbl(index);
	} else {
		index -= d_size;
	}

	if (index < r_size) {
		return get_raw(index);
	} else {
		index -= r_size;
	}

	if (index < s_size) {
		return get_str(index);
	} else {
		index -= s_size;
	}

	if (index < o_size) {
		return get_log(index);
	} else {
		index -= o_size;
	}

	if (index < c_size) {
		return get_cmp(index);
	} else {
		index -= c_size;
	}

	if (index < l_size) {
		return get_lst(index);
	} else {
		index -= l_size;
	}

	if (index < e_size) {
		return get_env(index);
	} else {
		index -= e_size;
	}

	if (index < u_size) {
		return get_env(index);
	} else {
		index -= u_size;
	}

	return get_generic(index);
}
