#include "raw_store.h"

#include "helper.h"

#include <map> // std::map
#include <iterator> //advance
#include <string> // std::string, strlen

#include "byte_vector.h"
#include "sha1.h"

#include "simple_raw_store.h"

FILE *raws_file = NULL;
FILE *raws_index_file = NULL;
std::map<std::string, size_t> *raw_index = NULL;


extern size_t size;
extern size_t r_size;
extern size_t r_offset;
extern size_t s_r_size;

extern byte_vector_t vector;

/**
 * Load/create a brand new raw store.
 * @method create_raw_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_raw_store(SEXP raws) {
	raws_file = open_file(raws);
	fseek(raws_file, r_offset, SEEK_SET);
	return R_NilValue;
}

/**
 * Load an existing raw store.
 * @method load_raw_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_raw_store(SEXP raws) {
	return init_raw_store(raws);
}

/**
 * This functions writes raw R val store to file and closes the file.
 * @method close_raw_store
 * @return R_NilValue on success
 */
SEXP close_raw_store() {
	if (raws_file) {
		close_file(&raws_file);
	}

	return R_NilValue;
}

/**
 * Load/create a brand new index associated with the raws store.
 * @method create_raw_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_raw_index(SEXP index) {
	raws_index_file = open_file(index);
	raw_index = new std::map<std::string, size_t>;
	return R_NilValue;
}

/**
 * Load an existing index associated with the raws store.
 * @method load_raw_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_raw_index(SEXP index) {
	init_raw_index(index);

	size_t start = 0;
	char hash[20];
	for (size_t i = 0; i < r_size - s_r_size; ++i) {
		read_n(raws_index_file, hash, 20);
		read_n(raws_index_file, &start, sizeof(size_t));
		(*raw_index)[std::string(hash, 20)] = start;
	}

	return R_NilValue;
}

/**
 * This function writes the index associated with the raws store to file
 * and closes the file.
 * @method close_raw_index
 * @return R_NilValue
 */
SEXP close_raw_index() {
	if (raws_index_file) {
		// TODO: Think about ways to reuse rather than overwrite each time
		fseek(raws_index_file, 0, SEEK_SET);

		std::map<std::string, size_t>::iterator it;
		for(it = raw_index->begin(); it != raw_index->end(); it++) {
			write_n(raws_index_file, (void *) it->first.c_str(), 20);
			write_n(raws_index_file, &(it->second), sizeof(size_t));
		}

		close_file(&raws_index_file);
	}

	if (raw_index) {
		delete raw_index;
		raw_index = NULL;
	}

	return R_NilValue;
}

/**
 * This functions merges another str store rawo the current str store.
 * @param  other_raws is the path to the raws store of a different db.
 * @param  other_index is the path to the index of other_raws on disk.
 * @method merge_raw_store
 * @return R_NilValue on success
 */
SEXP merge_raw_store(SEXP other_raws, SEXP other_index) {
	FILE *other_raws_file = open_file(other_raws);
	FILE *other_raw_index_file = open_file(other_index);

	fseek(other_raw_index_file, 0, SEEK_END);
	long int sz = ftell(other_raw_index_file) / (20 + sizeof(size_t));
	fseek(other_raw_index_file, 0, SEEK_SET);

	unsigned char other_sha1sum[20] = { 0 };
	size_t other_offset = 0;
	for (long int i = 0; i < sz; ++i) {
		read_n(other_raw_index_file, other_sha1sum, 20);
		read_n(other_raw_index_file, &other_offset, sizeof(size_t));

		std::string key((char *) other_sha1sum, 20);
		std::map<std::string, size_t>::iterator it = raw_index->find(key);
		if (it == raw_index->end()) { // TODO: Deal with collision
			(*raw_index)[key] = r_offset;
			r_size++;
			size++;

			size_t new_raw_size = 0;
			fseek(other_raws_file, other_offset, SEEK_SET);
			read_n(other_raws_file, &new_raw_size, sizeof(size_t));

			free_content(vector);
			read_n(other_raws_file, vector->buf, new_raw_size);
			vector->capacity = new_raw_size;

			// Write the blob
			write_n(raws_file, &(vector->size), sizeof(size_t));
			write_n(raws_file, vector->buf, vector->size);

			// Acting as NULL for linked-list next porawer
			write_n(raws_file, &(vector->size), sizeof(size_t));

			// Modify r_offset here
			r_offset += vector->size + sizeof(size_t) + sizeof(size_t);

			vector->capacity = 1 << 30;
		}
	}

	fseek(other_raws_file, -1, SEEK_END);
	fseek(other_raw_index_file, -1, SEEK_END);

	close_file(&other_raws_file);
	close_file(&other_raw_index_file);

	return R_NilValue;
}

/**
 * This function assesses if the input is a raw.
 * This function returns true whenver a value is of an raweger type.
 * @method is_raw
 * @param  SEXP          Any R value
 * @return               1
 */
int is_raw(SEXP value) {
	return (TYPEOF(value) == RAWSXP);
}

/**
 * Adds an raw R value to the raws store.
 * @method add_raw
 * @param  val is a raw R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_raw(SEXP val) {
	if (is_simple_raw(val)) {
		return add_simple_raw(val);
	}

	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = raw_index->find(key);
	if (it == raw_index->end()) { // TODO: Deal with collision
		(*raw_index)[key] = r_offset;
		r_size++;
		size++;

		// Write the blob
		write_n(raws_file, &(vector->size), sizeof(size_t));
		write_n(raws_file, vector->buf, vector->size);

		// Acting as NULL for linked-list next porawer
		write_n(raws_file, &(vector->size), sizeof(size_t));

		// Modify r_offset here
		r_offset += vector->size + sizeof(size_t) + sizeof(size_t);

		track_type(val);

		return val;
	} else {
		return R_NilValue;
	}
}

/**
 * This function asks if the C layer has seen an given raw value
 * @method have_seen_raw
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_raw(SEXP val) {
	if (is_simple_raw(val)) {
		return have_seen_simple_raw(val);
	}

	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = raw_index->find(key);

	if (it == raw_index->end()) {
		return 0;
	} else {
		return 1;
	}
}

/**
 * This function gets the raw value at the index'th place in the database.
 * @method get_raw
 * @return R value
 */
SEXP get_raw(int index) {
	if (index < s_r_size) {
		return get_simple_raw(index);
	} else {
		index -= s_r_size;
	}

	std::map<std::string, size_t>::iterator it = raw_index->begin();
	std::advance(it, index);

	// Get the specified value
	size_t obj_size;
	free_content(vector);
	fseek(raws_file, it->second, SEEK_SET);
	read_n(raws_file, &obj_size, sizeof(size_t));
	read_n(raws_file, vector->buf, obj_size);
	fseek(raws_file, r_offset, SEEK_SET);
	vector->capacity = obj_size;

	SEXP res = unserialize_val(vector);

	// Restore vector
	vector->capacity = 1 << 30;

	return res;
}

/**
 * This function samples from the rawegers stores in the database
 * @method sample_raw
 * @return R value in form of SEXP or throws an error if no raweger in database
 */
SEXP sample_raw() {
	if (s_r_size || r_size) {
		size_t random_index = rand_size_t() % (s_r_size + r_size);

		if (random_index < s_r_size) {
			return get_simple_raw(random_index);
		} else {
			return get_raw(random_index - s_r_size);
		}
	}

	Rf_error("No raws in this database.");
}
