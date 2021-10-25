#include "str_store.h"

#include "helper.h"

#include <map> // std::map
#include <iterator> //advance
#include <string> // std::string, strlen

#include "byte_vector.h"
#include "sha1.h"

#include "simple_str_store.h"

FILE *strs_file = NULL;
FILE *strs_index_file = NULL;
std::map<std::string, size_t> *str_index = NULL;


extern size_t size;
extern size_t s_size;
extern size_t s_offset;
extern size_t s_s_size;

extern byte_vector_t vector;

/**
 * Load/create a brand new str store.
 * @method create_str_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_str_store(SEXP strs) {
	strs_file = open_file(strs);
	fseek(strs_file, s_offset, SEEK_SET);
	return R_NilValue;
}

/**
 * Load an existing str store.
 * @method load_str_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_str_store(SEXP strs) {
	return init_str_store(strs);
}

/**
 * This functions writes str R val store to file and closes the file.
 * @method close_str_store
 * @return R_NilValue on success
 */
SEXP close_str_store() {
	if (strs_file) {
		close_file(&strs_file);
	}

	return R_NilValue;
}

/**
 * Load/create a brand new index associated with the strs store.
 * @method create_str_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_str_index(SEXP index) {
	strs_index_file = open_file(index);
	str_index = new std::map<std::string, size_t>;
	return R_NilValue;
}

/**
 * Load an existing index associated with the strs store.
 * @method load_str_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_str_index(SEXP index) {
	init_str_index(index);

	size_t start = 0;
	char hash[20];
	for (size_t i = 0; i < s_size - s_s_size; ++i) {
		read_n(strs_index_file, hash, 20);
		read_n(strs_index_file, &start, sizeof(size_t));
		(*str_index)[std::string(hash, 20)] = start;
	}

	return R_NilValue;
}

/**
 * This function writes the index associated with the strs store to file
 * and closes the file.
 * @method close_str_index
 * @return R_NilValue
 */
SEXP close_str_index() {
	if (strs_index_file) {
		// TODO: Think about ways to reuse rather than overwrite each time
		fseek(strs_index_file, 0, SEEK_SET);

		std::map<std::string, size_t>::iterator it;
		for(it = str_index->begin(); it != str_index->end(); it++) {
			write_n(strs_index_file, (void *) it->first.c_str(), 20);
			write_n(strs_index_file, &(it->second), sizeof(size_t));
		}

		close_file(&strs_index_file);
	}

	if (str_index) {
		delete str_index;
		str_index = NULL;
	}

	return R_NilValue;
}

/**
 * This functions merges another str store stro the current str store.
 * @param  other_strs is the path to the strs store of a different db.
 * @param  other_index is the path to the index of other_strs on disk.
 * @method merge_str_store
 * @return R_NilValue on success
 */
SEXP merge_str_store(SEXP other_strs, SEXP other_index) {
	FILE *other_strs_file = open_file(other_strs);
	FILE *other_str_index_file = open_file(other_index);

	fseek(other_str_index_file, 0, SEEK_END);
	long int sz = ftell(other_str_index_file) / (20 + sizeof(size_t));
	fseek(other_str_index_file, 0, SEEK_SET);

	unsigned char other_sha1sum[20] = { 0 };
	size_t other_offset = 0;
	for (long int i = 0; i < sz; ++i) {
		read_n(other_str_index_file, other_sha1sum, 20);
		read_n(other_str_index_file, &other_offset, sizeof(size_t));

		std::string key((char *) other_sha1sum, 20);
		std::map<std::string, size_t>::iterator it = str_index->find(key);
		if (it == str_index->end()) { // TODO: Deal with collision
			(*str_index)[key] = s_offset;
			s_size++;
			size++;

			size_t new_str_size = 0;
			fseek(other_strs_file, other_offset, SEEK_SET);
			read_n(other_strs_file, &new_str_size, sizeof(size_t));

			free_content(vector);
			read_n(other_strs_file, vector->buf, new_str_size);
			vector->capacity = new_str_size;

			// Write the blob
			write_n(strs_file, &(vector->size), sizeof(size_t));
			write_n(strs_file, vector->buf, vector->size);

			// Acting as NULL for linked-list next postrer
			write_n(strs_file, &(vector->size), sizeof(size_t));

			// Modify s_offset here
			s_offset += vector->size + sizeof(size_t) + sizeof(size_t);

			vector->capacity = 1 << 30;
		}
	}

	fseek(other_strs_file, -1, SEEK_END);
	fseek(other_str_index_file, -1, SEEK_END);

	close_file(&other_strs_file);
	close_file(&other_str_index_file);

	return R_NilValue;
}

/**
 * This function assesses if the input is a str.
 * This function returns true whenver a value is of an streger type.
 * @method is_str
 * @param  SEXP          Any R value
 * @return               1
 */
int is_str(SEXP value) {
	return (TYPEOF(value) == STRSXP);
}

/**
 * Adds an str R value to the strs store.
 * @method add_str
 * @param  val is a str R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_str(SEXP val) {
	if (is_simple_str(val)) {
		return add_simple_str(val);
	}

	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = str_index->find(key);
	if (it == str_index->end()) { // TODO: Deal with collision
		(*str_index)[key] = s_offset;
		s_size++;
		size++;

		// Write the blob
		write_n(strs_file, &(vector->size), sizeof(size_t));
		write_n(strs_file, vector->buf, vector->size);

		// Acting as NULL for linked-list next postrer
		write_n(strs_file, &(vector->size), sizeof(size_t));

		// Modify s_offset here
		s_offset += vector->size + sizeof(size_t) + sizeof(size_t);

		track_type(val);

		return val;
	} else {
		return R_NilValue;
	}
}

/**
 * This function asks if the C layer has seen an given str value
 * @method have_seen_str
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_str(SEXP val) {
	if (is_simple_str(val)) {
		return have_seen_simple_str(val);
	}

	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = str_index->find(key);

	if (it == str_index->end()) {
		return 0;
	} else {
		return 1;
	}
}

/**
 * This function gets the str value at the index'th place in the database.
 * @method get_str
 * @return R value
 */
SEXP get_str(int index) {
	if (index < s_s_size) {
		return get_simple_str(index);
	} else {
		index -= s_s_size;
	}

	std::map<std::string, size_t>::iterator it = str_index->begin();
	std::advance(it, index);

	// Get the specified value
	size_t obj_size;
	free_content(vector);
	fseek(strs_file, it->second, SEEK_SET);
	read_n(strs_file, &obj_size, sizeof(size_t));
	read_n(strs_file, vector->buf, obj_size);
	fseek(strs_file, s_offset, SEEK_SET);
	vector->capacity = obj_size;

	SEXP res = unserialize_val(vector);

	// Restore vector
	vector->capacity = 1 << 30;

	return res;
}

/**
 * This function samples from the strings stores in the database
 * @method sample_str
 * @return R value in form of SEXP or throws an error if no string in database
 */
SEXP sample_str() {
	if (s_s_size || s_size) {
		size_t random_index = rand_size_t() % (s_s_size + s_size);

		if (random_index < s_s_size) {
			return get_simple_str(random_index);
		} else {
			return get_str(random_index - s_s_size);
		}
	}

	Rf_error("No strings in this database.");
}
