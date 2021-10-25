#include "int_store.h"

#include "helper.h"

#include <map> // std::map
#include <iterator> //advance
#include <string> // std::string, strlen

#include "byte_vector.h"
#include "sha1.h"

#include "simple_int_store.h"

FILE *ints_file = NULL;
FILE *ints_index_file = NULL;
std::map<std::string, size_t> *int_index = NULL;


extern size_t size;
extern size_t i_size;
extern size_t i_offset;
extern size_t s_i_size;

extern byte_vector_t vector;

/**
 * Load/create a brand new int store.
 * @method create_int_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_int_store(SEXP ints) {
	ints_file = open_file(ints);
	fseek(ints_file, i_offset, SEEK_SET);
	return R_NilValue;
}

/**
 * Load an existing int store.
 * @method load_int_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_int_store(SEXP ints) {
	return init_int_store(ints);
}

/**
 * This functions writes int R val store to file and closes the file.
 * @method close_int_store
 * @return R_NilValue on success
 */
SEXP close_int_store() {
	if (ints_file) {
		close_file(&ints_file);
	}

	return R_NilValue;
}

/**
 * Load/create a brand new index associated with the ints store.
 * @method create_int_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_int_index(SEXP index) {
	ints_index_file = open_file(index);
	int_index = new std::map<std::string, size_t>;
	return R_NilValue;
}

/**
 * Load an existing index associated with the ints store.
 * @method load_int_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_int_index(SEXP index) {
	init_int_index(index);

	size_t start = 0;
	char hash[20];
	for (size_t i = 0; i < (i_size - s_i_size); ++i) {
		read_n(ints_index_file, hash, 20);
		read_n(ints_index_file, &start, sizeof(size_t));
		(*int_index)[std::string(hash, 20)] = start;
	}

	return R_NilValue;
}

/**
 * This function writes the index associated with the ints store to file
 * and closes the file.
 * @method close_int_index
 * @return R_NilValue
 */
SEXP close_int_index() {
	if (ints_index_file) {
		// TODO: Think about ways to reuse rather than overwrite each time
		fseek(ints_index_file, 0, SEEK_SET);

		std::map<std::string, size_t>::iterator it;
		for(it = int_index->begin(); it != int_index->end(); it++) {
			write_n(ints_index_file, (void *) it->first.c_str(), 20);
			write_n(ints_index_file, &(it->second), sizeof(size_t));
		}

		close_file(&ints_index_file);
	}

	if (int_index) {
		delete int_index;
		int_index = NULL;
	}

	return R_NilValue;
}

/**
 * This functions merges another str store into the current str store.
 * @param  other_ints is the path to the ints store of a different db.
 * @param  other_index is the path to the index of other_ints on disk.
 * @method merge_int_store
 * @return R_NilValue on success
 */
SEXP merge_int_store(SEXP other_ints, SEXP other_index) {
	FILE *other_ints_file = open_file(other_ints);
	FILE *other_int_index_file = open_file(other_index);

	fseek(other_int_index_file, 0, SEEK_END);
	long int sz = ftell(other_int_index_file) / (20 + sizeof(size_t));
	fseek(other_int_index_file, 0, SEEK_SET);

	unsigned char other_sha1sum[20] = { 0 };
	size_t other_offset = 0;
	for (long int i = 0; i < sz; ++i) {
		read_n(other_int_index_file, other_sha1sum, 20);
		read_n(other_int_index_file, &other_offset, sizeof(size_t));

		std::string key((char *) other_sha1sum, 20);
		std::map<std::string, size_t>::iterator it = int_index->find(key);
		if (it == int_index->end()) { // TODO: Deal with collision
			(*int_index)[key] = i_offset;
			i_size++;
			size++;

			size_t new_int_size = 0;
			fseek(other_ints_file, other_offset, SEEK_SET);
			read_n(other_ints_file, &new_int_size, sizeof(size_t));

			free_content(vector);
			read_n(other_ints_file, vector->buf, new_int_size);
			vector->capacity = new_int_size;

			// Write the blob
			write_n(ints_file, &(vector->size), sizeof(size_t));
			write_n(ints_file, vector->buf, vector->size);

			// Acting as NULL for linked-list next pointer
			write_n(ints_file, &(vector->size), sizeof(size_t));

			// Modify i_offset here
			i_offset += vector->size + sizeof(size_t) + sizeof(size_t);

			vector->capacity = 1 << 30;
		}
	}

	fseek(other_ints_file, -1, SEEK_END);
	fseek(other_int_index_file, -1, SEEK_END);

	close_file(&other_ints_file);
	close_file(&other_int_index_file);

	return R_NilValue;
}

/**
 * This function assesses if the input is a int.
 * This function returns true whenver a value is of an integer type.
 * @method is_int
 * @param  SEXP          Any R value
 * @return               1
 */
int is_int(SEXP value) {
	return (TYPEOF(value) == INTSXP);
}

/**
 * Adds an int R value to the ints store.
 * @method add_int
 * @param  val is a int R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_int(SEXP val) {
	if (is_simple_int(val)) {
		return add_simple_int(val);
	}

	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = int_index->find(key);
	if (it == int_index->end()) { // TODO: Deal with collision
		(*int_index)[key] = i_offset;
		i_size++;
		size++;

		// Write the blob
		write_n(ints_file, &(vector->size), sizeof(size_t));
		write_n(ints_file, vector->buf, vector->size);

		// Acting as NULL for linked-list next pointer
		write_n(ints_file, &(vector->size), sizeof(size_t));

		// Modify i_offset here
		i_offset += vector->size + sizeof(size_t) + sizeof(size_t);

		track_type(val);

		return val;
	} else {
		return R_NilValue;
	}
}

/**
 * This function asks if the C layer has seen an given int value
 * @method have_seen_int
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_int(SEXP val) {
	if (is_simple_int(val)) {
		return have_seen_simple_int(val);
	}

	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = int_index->find(key);

	if (it == int_index->end()) {
		return 0;
	} else {
		return 1;
	}
}

/**
 * This function gets the int value at the index'th place in the database.
 * @method get_dbl
 * @return R value
 */
SEXP get_int(int index) {
	if (index < s_i_size) {
		return get_simple_int(index);
	} else {
		index -= s_i_size;
	}

	std::map<std::string, size_t>::iterator it = int_index->begin();
	std::advance(it, index);

	// Get the specified value
	size_t obj_size;
	free_content(vector);
	fseek(ints_file, it->second, SEEK_SET);
	read_n(ints_file, &obj_size, sizeof(size_t));
	read_n(ints_file, vector->buf, obj_size);
	fseek(ints_file, i_offset, SEEK_SET);
	vector->capacity = obj_size;

	SEXP res = unserialize_val(vector);

	// Restore vector
	vector->capacity = 1 << 30;

	return res;
}

/**
 * This function samples from the integers stores in the database
 * @method sample_int
 * @return R value in form of SEXP or throws an error if no integer in database
 */
SEXP sample_int() {
	if (s_i_size || i_size) {
		size_t random_index = rand_size_t() % (s_i_size + i_size);

		if (random_index < s_i_size) {
			return get_simple_int(random_index);
		} else {
			return get_int(random_index - s_i_size);
		}
	}

	Rf_error("No integers in this database.");
}
