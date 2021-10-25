#include "cmp_store.h"

#include "helper.h"

#include <map> // std::map
#include <iterator> //advance
#include <string> // std::string, strlen

#include "byte_vector.h"
#include "sha1.h"

FILE *cmps_file = NULL;
FILE *cmps_index_file = NULL;
std::map<std::string, size_t> *cmp_index = NULL;


extern size_t size;
extern size_t c_size;
extern size_t c_offset;

extern byte_vector_t vector;

/**
 * Load/create a brand new cmp store.
 * @method create_cmp_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_cmp_store(SEXP cmps) {
	cmps_file = open_file(cmps);
	fseek(cmps_file, c_offset, SEEK_SET);
	return R_NilValue;
}

/**
 * Load an existing cmp store.
 * @method load_cmp_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_cmp_store(SEXP cmps) {
	return init_cmp_store(cmps);
}

/**
 * This functions writes cmp R val store to file and closes the file.
 * @method close_cmp_store
 * @return R_NilValue on success
 */
SEXP close_cmp_store() {
	if (cmps_file) {
		close_file(&cmps_file);
	}

	return R_NilValue;
}

/**
 * Load/create a brand new index associated with the cmps store.
 * @method create_cmp_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_cmp_index(SEXP index) {
	cmps_index_file = open_file(index);
	cmp_index = new std::map<std::string, size_t>;
	return R_NilValue;
}

/**
 * Load an existing index associated with the cmps store.
 * @method load_cmp_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_cmp_index(SEXP index) {
	init_cmp_index(index);

	size_t start = 0;
	char hash[20];
	for (size_t i = 0; i < c_size; ++i) {
		read_n(cmps_index_file, hash, 20);
		read_n(cmps_index_file, &start, sizeof(size_t));
		(*cmp_index)[std::string(hash, 20)] = start;
	}

	return R_NilValue;
}

/**
 * This function writes the index associated with the cmps store to file
 * and closes the file.
 * @method close_cmp_index
 * @return R_NilValue
 */
SEXP close_cmp_index() {
	if (cmps_index_file) {
		// TODO: Think about ways to reuse rather than overwrite each time
		fseek(cmps_index_file, 0, SEEK_SET);

		std::map<std::string, size_t>::iterator it;
		for(it = cmp_index->begin(); it != cmp_index->end(); it++) {
			write_n(cmps_index_file, (void *) it->first.c_str(), 20);
			write_n(cmps_index_file, &(it->second), sizeof(size_t));
		}

		close_file(&cmps_index_file);
	}

	if (cmp_index) {
		delete cmp_index;
		cmp_index = NULL;
	}

	return R_NilValue;
}

/**
 * This functions merges another str store into the current str store.
 * @param  other_cmps is the path to the cmps store of a different db.
 * @param  other_index is the path to the index of other_cmps on disk.
 * @method merge_cmp_store
 * @return R_NilValue on success
 */
SEXP merge_cmp_store(SEXP other_cmps, SEXP other_index) {
	FILE *other_cmps_file = open_file(other_cmps);
	FILE *other_cmp_index_file = open_file(other_index);

	fseek(other_cmp_index_file, 0, SEEK_END);
	long int sz = ftell(other_cmp_index_file) / (20 + sizeof(size_t));
	fseek(other_cmp_index_file, 0, SEEK_SET);

	unsigned char other_sha1sum[20] = { 0 };
	size_t other_offset = 0;
	for (long int i = 0; i < sz; ++i) {
		read_n(other_cmp_index_file, other_sha1sum, 20);
		read_n(other_cmp_index_file, &other_offset, sizeof(size_t));

		std::string key((char *) other_sha1sum, 20);
		std::map<std::string, size_t>::iterator it = cmp_index->find(key);
		if (it == cmp_index->end()) { // TODO: Deal with collision
			(*cmp_index)[key] = c_offset;
			c_size++;
			size++;

			size_t new_cmp_size = 0;
			fseek(other_cmps_file, other_offset, SEEK_SET);
			read_n(other_cmps_file, &new_cmp_size, sizeof(size_t));

			free_content(vector);
			read_n(other_cmps_file, vector->buf, new_cmp_size);
			vector->capacity = new_cmp_size;

			// Write the blob
			write_n(cmps_file, &(vector->size), sizeof(size_t));
			write_n(cmps_file, vector->buf, vector->size);

			// Acting as NULL for linked-list next pointer
			write_n(cmps_file, &(vector->size), sizeof(size_t));

			// Modify c_offset here
			c_offset += vector->size + sizeof(size_t) + sizeof(size_t);

			vector->capacity = 1 << 30;
		}
	}

	fseek(other_cmps_file, -1, SEEK_END);
	fseek(other_cmp_index_file, -1, SEEK_END);

	close_file(&other_cmps_file);
	close_file(&other_cmp_index_file);

	return R_NilValue;
}

/**
 * This function assesses if the input is a cmp.
 * @method is_cmp
 * @param  SEXP          Any R value
 * @return               1 if the value is a complex, 0 otherwise
 */
int is_cmp(SEXP value) {
	return (TYPEOF(value) == CPLXSXP);
}

/**
 * Adds an cmp R value to the cmps store.
 * @method add_cmp
 * @param  val is a cmp R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_cmp(SEXP val) {
	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = cmp_index->find(key);
	if (it == cmp_index->end()) { // TODO: Deal with collision
		(*cmp_index)[key] = c_offset;
		c_size++;
		size++;

		// Write the blob
		write_n(cmps_file, &(vector->size), sizeof(size_t));
		write_n(cmps_file, vector->buf, vector->size);

		// Acting as NULL for linked-list next pointer
		write_n(cmps_file, &(vector->size), sizeof(size_t));

		// Modify c_offset here
		c_offset += vector->size + sizeof(size_t) + sizeof(size_t);

		track_type(val);

		return val;
	} else {
		return R_NilValue;
	}
}

/**
 * This function asks if the C layer has seen an given cmp value
 * @method have_seen_cmp
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_cmp(SEXP val) {
	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = cmp_index->find(key);

	if (it == cmp_index->end()) {
		return 0;
	} else {
		return 1;
	}
}

/**
 * This function gets the cmp value at the index'th place in the database.
 * @method get_dbl
 * @return R value
 */
SEXP get_cmp(int index) {
	std::map<std::string, size_t>::iterator it = cmp_index->begin();
	std::advance(it, index);

	// Get the specified value
	size_t obj_size;
	free_content(vector);
	fseek(cmps_file, it->second, SEEK_SET);
	read_n(cmps_file, &obj_size, sizeof(size_t));
	read_n(cmps_file, vector->buf, obj_size);
	fseek(cmps_file, c_offset, SEEK_SET);
	vector->capacity = obj_size;

	SEXP res = unserialize_val(vector);

	// Restore vector
	vector->capacity = 1 << 30;

	return res;
}

/**
 * This function samples from the complexs stores in the database
 * @method sample_str
 * @return R value in form of SEXP or throws an error if no complex in database
 */
SEXP sample_cmp() {
	if (c_size) {
		size_t random_index = rand_size_t() % c_size;
		return get_cmp(random_index);
	}

	Rf_error("No complex values in this database.");
}
