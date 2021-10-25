#include "simple_int_store.h"

#include "helper.h"

FILE *s_ints_file = NULL;

// Small scalar int storage
int INT_STORE_MAX = 5000;
int INT_STORE_MIN = -5000;
int INT_STORE_SIZE = INT_STORE_MAX - INT_STORE_MIN + 1;
size_t int_db[10001] = { 0 };    // hard wired to accommodate -5000 to 5000

extern size_t size;
extern size_t i_size;
extern size_t s_i_size;

/**
 * Load/create a brand new simple integer store.
 * @method init_simple_int_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_simple_int_store(SEXP ints) {
	s_ints_file = open_file(ints);

	for (int i = 0; i < INT_STORE_SIZE; ++i) {
		int_db[i] = 0;
	}

	return R_NilValue;
}

/**
 * Load an existing simple integer store.
 * @method load_simple_int_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_simple_int_store(SEXP ints) {
	s_ints_file = open_file(ints);

	for (size_t i = 0; i < INT_STORE_SIZE; ++i) {
		read_n(s_ints_file, int_db + i, sizeof(size_t));
	}

	return R_NilValue;
}

/**
 * This functions merges another ints store into the current int store.
 * @param  other_ints is the path to the ints store on disk of a different db.
 * @method merge_simple_int_store
 * @return R_NilValue on success
 */
SEXP merge_simple_int_store(SEXP other_ints) {
	FILE *other_ints_file = open_file(other_ints);
	size_t other_int_count;

	for (size_t i = 0; i < INT_STORE_SIZE; ++i) {
		read_n(other_ints_file, &other_int_count, sizeof(size_t));

		if (int_db[i] == 0 && other_int_count > 0) {
			size += 1;
			s_i_size += 1;
		}

		int_db[i] += other_int_count;
	}

	close_file(&other_ints_file);

	return R_NilValue;
}

/**
 * This functions writes simple int R val store to file and closes the file.
 * @method close_simple_int_store
 * @return R_NilValue on success
 */
SEXP close_simple_int_store() {
	if (s_ints_file) {
		fseek(s_ints_file, 0, SEEK_SET);

		for(int i = 0; i < INT_STORE_SIZE; ++i) {
			write_n(s_ints_file, &(int_db[i]), sizeof(size_t));
		}

		close_file(&s_ints_file);

		memset(int_db, 0, INT_STORE_SIZE * sizeof(size_t));
	}

	return R_NilValue;
}

/**
 * This function assesses if the input is a simple integer.
 * @method is_simple_int
 * @param  SEXP          Any R value
 * @return               1 if it is a simple integer, 0 otherwise
 */
int is_simple_int(SEXP value) {
	if (IS_SIMPLE_SCALAR(value, INTSXP)) {
		if (asInteger(value) <= INT_STORE_MAX &&
			asInteger(value) >= INT_STORE_MIN) {
			return 1;
		}
	}

	return 0;
}

/**
 * Adds a simple integer R value to the simple integer store.
 * @method add_simple_int
 * @param  val is a simple int R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_simple_int(SEXP val) {
	// int_db[0] represents -5000L
	int int_val = asInteger(val) - INT_STORE_MIN;
	if(int_db[int_val] == 0) {
		int_db[int_val] += 1;
		s_i_size += 1;
		i_size += 1;
		size += 1;
		return val;
	} else {
		int_db[int_val] += 1;
		return R_NilValue;
	}
}

/**
 * This function asks if the C layer has seen a given simple integer value.
 * @method have_seen_simple_int
 * @param  val       a simple int R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_simple_int(SEXP val) {
	// int_db[0] represents -5000L
	int index = Rf_asInteger(val) - INT_STORE_MIN;
	return int_db[index];
}

/**
 * This function gets the simple integer at the index'th place in the database.
 * @method get_simple_int
 * @return R value
 */
SEXP get_simple_int(int index) {
	if (s_i_size < INT_STORE_SIZE) {
		int values_passed = 0;
		for (int i = 0; i < INT_STORE_SIZE; ++i) {
			if (int_db[i] > 0) {
				++values_passed;
			}

			if (values_passed == index + 1) {
				SEXP res;
				R_xlen_t n = 1;
				PROTECT(res = allocVector(INTSXP, n));
				int *res_ptr = INTEGER(res);
				res_ptr[0] = i - INT_STORE_MAX;
				UNPROTECT(1);
				return res;
			}
		}
	} else {
		SEXP res;
		R_xlen_t n = 1;
		PROTECT(res = allocVector(INTSXP, n));
		int *res_ptr = INTEGER(res);
		res_ptr[0] = index - INT_STORE_MAX;
		UNPROTECT(1);
		return res;
	}

	return R_NilValue;
}
