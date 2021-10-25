#include "dbl_store.h"

#include "helper.h"

#include <map> // std::map
#include <iterator> //advance
#include <string> // std::string, strlen

#include "byte_vector.h"
#include "sha1.h"

#include "simple_dbl_store.h"

FILE *dbls_file = NULL;
FILE *dbls_index_file = NULL;
std::map<std::string, size_t> *dbl_index = NULL;


extern size_t size;
extern size_t d_size;
extern size_t d_offset;
extern size_t s_d_size;

extern byte_vector_t vector;

/**
 * Load/create a brand new dbl store.
 * @method create_dbl_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_dbl_store(SEXP dbls) {
	dbls_file = open_file(dbls);
	fseek(dbls_file, d_offset, SEEK_SET);
	return R_NilValue;
}

/**
 * Load an existing dbl store.
 * @method load_dbl_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_dbl_store(SEXP dbls) {
	return init_dbl_store(dbls);
}

/**
 * This functions writes dbl R val store to file and closes the file.
 * @method close_dbl_store
 * @return R_NilValue on success
 */
SEXP close_dbl_store() {
	if (dbls_file) {
		close_file(&dbls_file);
	}

	return R_NilValue;
}

/**
 * Load/create a brand new index associated with the dbls store.
 * @method create_dbl_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_dbl_index(SEXP index) {
	dbls_index_file = open_file(index);
	dbl_index = new std::map<std::string, size_t>;
	return R_NilValue;
}

/**
 * Load an existing index associated with the dbls store.
 * @method load_dbl_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_dbl_index(SEXP index) {
	init_dbl_index(index);

	size_t start = 0;
	char hash[20];
	for (size_t i = 0; i < (d_size - s_d_size); ++i) {
		read_n(dbls_index_file, hash, 20);
		read_n(dbls_index_file, &start, sizeof(size_t));
		(*dbl_index)[std::string(hash, 20)] = start;
	}

	return R_NilValue;
}

/**
 * This function writes the index associated with the dbls store to file
 * and closes the file.
 * @method close_dbl_index
 * @return R_NilValue
 */
SEXP close_dbl_index() {
	if (dbls_index_file) {
		// TODO: Think about ways to reuse rather than overwrite each time
		fseek(dbls_index_file, 0, SEEK_SET);

		std::map<std::string, size_t>::iterator it;
		for(it = dbl_index->begin(); it != dbl_index->end(); it++) {
			write_n(dbls_index_file, (void *) it->first.c_str(), 20);
			write_n(dbls_index_file, &(it->second), sizeof(size_t));
		}

		close_file(&dbls_index_file);
	}

	if (dbl_index) {
		delete dbl_index;
		dbl_index = NULL;
	}

	return R_NilValue;
}

/**
 * This functions merges another str store dblo the current str store.
 * @param  other_dbls is the path to the dbls store of a different db.
 * @param  other_index is the path to the index of other_dbls on disk.
 * @method merge_dbl_store
 * @return R_NilValue on success
 */
SEXP merge_dbl_store(SEXP other_dbls, SEXP other_index) {
	FILE *other_dbls_file = open_file(other_dbls);
	FILE *other_dbl_index_file = open_file(other_index);

	fseek(other_dbl_index_file, 0, SEEK_END);
	long int sz = ftell(other_dbl_index_file) / (20 + sizeof(size_t));
	fseek(other_dbl_index_file, 0, SEEK_SET);

	unsigned char other_sha1sum[20] = { 0 };
	size_t other_offset = 0;
	for (long int i = 0; i < sz; ++i) {
		read_n(other_dbl_index_file, other_sha1sum, 20);
		read_n(other_dbl_index_file, &other_offset, sizeof(size_t));

		std::string key((char *) other_sha1sum, 20);
		std::map<std::string, size_t>::iterator it = dbl_index->find(key);
		if (it == dbl_index->end()) { // TODO: Deal with collision
			(*dbl_index)[key] = d_offset;
			d_size++;
			size++;

			size_t new_dbl_size = 0;
			fseek(other_dbls_file, other_offset, SEEK_SET);
			read_n(other_dbls_file, &new_dbl_size, sizeof(size_t));

			free_content(vector);
			read_n(other_dbls_file, vector->buf, new_dbl_size);
			vector->capacity = new_dbl_size;

			// Write the blob
			write_n(dbls_file, &(vector->size), sizeof(size_t));
			write_n(dbls_file, vector->buf, vector->size);

			// Acting as NULL for linked-list next pointer
			write_n(dbls_file, &(vector->size), sizeof(size_t));

			// Modify d_offset here
			d_offset += vector->size + sizeof(size_t) + sizeof(size_t);

			vector->capacity = 1 << 30;
		}
	}

	fseek(other_dbls_file, -1, SEEK_END);
	fseek(other_dbl_index_file, -1, SEEK_END);

	close_file(&other_dbls_file);
	close_file(&other_dbl_index_file);

	return R_NilValue;
}

/**
 * This function assesses if the input is a dbl.
 * This function returns true whenver a value is of an dbleger type.
 * @method is_dbl
 * @param  SEXP          Any R value
 * @return               1
 */
int is_dbl(SEXP value) {
	return (TYPEOF(value) == REALSXP);
}

/**
 * Adds an dbl R value to the dbls store.
 * @method add_dbl
 * @param  val is a dbl R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_dbl(SEXP val) {
	if (is_simple_dbl(val)) {
		return add_simple_dbl(val);
	}

	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = dbl_index->find(key);
	if (it == dbl_index->end()) { // TODO: Deal with collision
		(*dbl_index)[key] = d_offset;
		d_size++;
		size++;

		// Write the blob
		write_n(dbls_file, &(vector->size), sizeof(size_t));
		write_n(dbls_file, vector->buf, vector->size);

		// Acting as NULL for linked-list next podbler
		write_n(dbls_file, &(vector->size), sizeof(size_t));

		// Modify d_offset here
		d_offset += vector->size + sizeof(size_t) + sizeof(size_t);

		track_type(val);

		return val;
	} else {
		return R_NilValue;
	}
}

/**
 * This function asks if the C layer has seen an given dbl value
 * @method have_seen_dbl
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_dbl(SEXP val) {
	if (is_simple_dbl(val)) {
		return have_seen_simple_dbl(val);
	}

	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = dbl_index->find(key);

	if (it == dbl_index->end()) {
		return 0;
	} else {
		return 1;
	}
}

/**
 * This function gets the dbl value at the index'th place in the database.
 * @method get_dbl
 * @return R value
 */
SEXP get_dbl(int index) {
	if (index < s_d_size) {
		return get_simple_dbl(index);
	} else {
		index -= s_d_size;
	}

	std::map<std::string, size_t>::iterator it = dbl_index->begin();
	std::advance(it, index);

	// Get the specified value
	size_t obj_size;
	free_content(vector);
	fseek(dbls_file, it->second, SEEK_SET);
	read_n(dbls_file, &obj_size, sizeof(size_t));
	read_n(dbls_file, vector->buf, obj_size);
	fseek(dbls_file, d_offset, SEEK_SET);
	vector->capacity = obj_size;

	SEXP res = unserialize_val(vector);

	// Restore vector
	vector->capacity = 1 << 30;

	return res;
}

/**
 * This function samples from the dblegers stores in the database
 * @method sample_dbl
 * @return R value in form of SEXP or throws an error if no dbleger in database
 */
SEXP sample_dbl() {
	if (s_d_size || d_size) {
		size_t random_index = rand_size_t() % (s_d_size + d_size);

		if (random_index < s_d_size) {
			return get_simple_dbl(random_index);
		} else {
			return get_dbl(random_index - s_d_size);
		}
	}

	Rf_error("No doubles in this database.");
}
