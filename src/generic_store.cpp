#include "generic_store.h"

#include "helper.h"

#include <map> // std::map
#include <iterator> //advance
#include <string> // std::string, strlen

#include "byte_vector.h"
#include "sha1.h"

FILE *generics_file = NULL;
FILE *generics_index_file = NULL;
std::map<std::string, size_t> *generic_index = NULL;


extern size_t size;
extern size_t g_size;
extern size_t g_offset;

extern byte_vector_t vector;

/**
 * Load/create a brand new generic store.
 * @method create_generic_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_generic_store(SEXP generics) {
	generics_file = open_file(generics);
	fseek(generics_file, g_offset, SEEK_SET);
	return R_NilValue;
}

/**
 * Load an existing generic store.
 * @method load_generic_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_generic_store(SEXP generics) {
	return init_generic_store(generics);
}

/**
 * This functions writes generic R val store to file and closes the file.
 * @method close_generic_store
 * @return R_NilValue on success
 */
SEXP close_generic_store() {
	if (generics_file) {
		close_file(&generics_file);
	}

	return R_NilValue;
}

/**
 * Load/create a brand new index associated with the generics store.
 * @method create_generic_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_generic_index(SEXP index) {
	generics_index_file = open_file(index);
	generic_index = new std::map<std::string, size_t>;
	return R_NilValue;
}

/**
 * Load an existing index associated with the generics store.
 * @method load_generic_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_generic_index(SEXP index) {
	init_generic_index(index);

	size_t start = 0;
	char hash[20];
	for (size_t i = 0; i < g_size; ++i) {
		read_n(generics_index_file, hash, 20);
		read_n(generics_index_file, &start, sizeof(size_t));
		(*generic_index)[std::string(hash, 20)] = start;
	}

	return R_NilValue;
}

/**
 * This function writes the index associated with the generics store to file
 * and closes the file.
 * @method close_generic_index
 * @return R_NilValue
 */
SEXP close_generic_index() {
	if (generics_index_file) {
		// TODO: Think about ways to reuse rather than overwrite each time
		fseek(generics_index_file, 0, SEEK_SET);

		std::map<std::string, size_t>::iterator it;
		for(it = generic_index->begin(); it != generic_index->end(); it++) {
			write_n(generics_index_file, (void *) it->first.c_str(), 20);
			write_n(generics_index_file, &(it->second), sizeof(size_t));
		}

		close_file(&generics_index_file);
	}

	if (generic_index) {
		delete generic_index;
		generic_index = NULL;
	}

	return R_NilValue;
}

/**
 * This functions merges another str store into the current str store.
 * @param  other_generics is the path to the generics store of a different db.
 * @param  other_index is the path to the index of other_generics on disk.
 * @method merge_generic_store
 * @return R_NilValue on success
 */
SEXP merge_generic_store(SEXP other_generics, SEXP other_index) {
	FILE *other_generics_file = open_file(other_generics);
	FILE *other_generic_index_file = open_file(other_index);

	fseek(other_generic_index_file, 0, SEEK_END);
	long int sz = ftell(other_generic_index_file) / (20 + sizeof(size_t));
	fseek(other_generic_index_file, 0, SEEK_SET);

	unsigned char other_sha1sum[20] = { 0 };
	size_t other_offset = 0;
	for (long int i = 0; i < sz; ++i) {
		read_n(other_generic_index_file, other_sha1sum, 20);
		read_n(other_generic_index_file, &other_offset, sizeof(size_t));

		std::string key((char *) other_sha1sum, 20);
		std::map<std::string, size_t>::iterator it = generic_index->find(key);
		if (it == generic_index->end()) { // TODO: Deal with collision
			(*generic_index)[key] = g_offset;
			g_size++;
			size++;

			size_t new_generic_size = 0;
			fseek(other_generics_file, other_offset, SEEK_SET);
			read_n(other_generics_file, &new_generic_size, sizeof(size_t));

			free_content(vector);
			read_n(other_generics_file, vector->buf, new_generic_size);
			vector->capacity = new_generic_size;

			// Write the blob
			write_n(generics_file, &(vector->size), sizeof(size_t));
			write_n(generics_file, vector->buf, vector->size);

			// Acting as NULL for linked-list next pointer
			write_n(generics_file, &(vector->size), sizeof(size_t));

			// Modify g_offset here
			g_offset += vector->size + sizeof(size_t) + sizeof(size_t);

			vector->capacity = 1 << 30;
		}
	}

	fseek(other_generics_file, -1, SEEK_END);
	fseek(other_generic_index_file, -1, SEEK_END);

	close_file(&other_generics_file);
	close_file(&other_generic_index_file);

	return R_NilValue;
}

/**
 * This function assesses if the input is a generic.
 * This function would always return true, therefore no need to implement it.
 * @method is_generic
 * @param  SEXP          Any R value
 * @return               1
 */
// int is_generic(SEXP value) {
// 	return 1;
// }

/**
 * Adds an generic R value to the generics store.
 * @method add_generic
 * @param  val is a generic R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_generic(SEXP val) {
	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = generic_index->find(key);
	if (it == generic_index->end()) { // TODO: Deal with collision
		(*generic_index)[key] = g_offset;
		g_size++;
		size++;

		// Write the blob
		write_n(generics_file, &(vector->size), sizeof(size_t));
		write_n(generics_file, vector->buf, vector->size);

		// Acting as NULL for linked-list next pointer
		write_n(generics_file, &(vector->size), sizeof(size_t));

		// Modify g_offset here
		g_offset += vector->size + sizeof(size_t) + sizeof(size_t);

		track_type(val);

		return val;
	} else {
		return R_NilValue;
	}
}

/**
 * This function asks if the C layer has seen an given generic value
 * @method have_seen_generic
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_generic(SEXP val) {
	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = generic_index->find(key);

	if (it == generic_index->end()) {
		return 0;
	} else {
		return 1;
	}
}

/**
 * This function gets the generic value at the index'th place in the database.
 * @method get_dbl
 * @return R value
 */
SEXP get_generic(int index) {
	std::map<std::string, size_t>::iterator it = generic_index->begin();
	std::advance(it, index);

	// Get the specified value
	size_t obj_size;
	free_content(vector);
	fseek(generics_file, it->second, SEEK_SET);
	read_n(generics_file, &obj_size, sizeof(size_t));
	read_n(generics_file, vector->buf, obj_size);
	fseek(generics_file, g_offset, SEEK_SET);
	vector->capacity = obj_size;

	SEXP res = unserialize_val(vector);

	// Restore vector
	vector->capacity = 1 << 30;

	return res;
}

/**
 * This function samples from the generic stores in the database
 * @method sample_generic
 * @return R value in form of SEXP or throws an error if no generic in database
 */
SEXP sample_generic() {
	if (g_size) {
		size_t random_index = rand_size_t() % g_size;
		return get_generic(random_index);
	}

	Rf_error("No generic values in this database.");
}
