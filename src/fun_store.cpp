#include "fun_store.h"

#include "helper.h"

#include <map> // std::map
#include <iterator> //advance
#include <string> // std::string, strlen

#include "byte_vector.h"
#include "sha1.h"

FILE *funs_file = NULL;
FILE *funs_index_file = NULL;
std::map<std::string, size_t> *fun_index = NULL;


extern size_t size;
extern size_t u_size;
extern size_t u_offset;

extern byte_vector_t vector;

/**
 * Load/create a brand new fun store.
 * @method create_fun_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_fun_store(SEXP funs) {
	funs_file = open_file(funs);
	fseek(funs_file, u_offset, SEEK_SET);
	return R_NilValue;
}

/**
 * Load an existing fun store.
 * @method load_fun_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_fun_store(SEXP funs) {
	return init_fun_store(funs);
}

/**
 * This functions writes fun R val store to file and closes the file.
 * @method close_fun_store
 * @return R_NilValue on success
 */
SEXP close_fun_store() {
	if (funs_file) {
		close_file(&funs_file);
	}

	return R_NilValue;
}

/**
 * Load/create a brand new index associated with the funs store.
 * @method create_fun_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_fun_index(SEXP index) {
	funs_index_file = open_file(index);
	fun_index = new std::map<std::string, size_t>;
	return R_NilValue;
}

/**
 * Load an existing index associated with the funs store.
 * @method load_fun_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_fun_index(SEXP index) {
	init_fun_index(index);

	size_t start = 0;
	char hash[20];
	for (size_t i = 0; i < u_size; ++i) {
		read_n(funs_index_file, hash, 20);
		read_n(funs_index_file, &start, sizeof(size_t));
		(*fun_index)[std::string(hash, 20)] = start;
	}

	return R_NilValue;
}

/**
 * This function writes the index associated with the funs store to file
 * and closes the file.
 * @method close_fun_index
 * @return R_NilValue
 */
SEXP close_fun_index() {
	if (funs_index_file) {
		// TODO: Think about ways to reuse rather than overwrite each time
		fseek(funs_index_file, 0, SEEK_SET);

		std::map<std::string, size_t>::iterator it;
		for(it = fun_index->begin(); it != fun_index->end(); it++) {
			write_n(funs_index_file, (void *) it->first.c_str(), 20);
			write_n(funs_index_file, &(it->second), sizeof(size_t));
		}

		close_file(&funs_index_file);
	}

	if (fun_index) {
		delete fun_index;
		fun_index = NULL;
	}

	return R_NilValue;
}

/**
 * This functions merges another str store funo the current str store.
 * @param  other_funs is the path to the funs store of a different db.
 * @param  other_index is the path to the index of other_funs on disk.
 * @method merge_fun_store
 * @return R_NilValue on success
 */
SEXP merge_fun_store(SEXP other_funs, SEXP other_index) {
	FILE *other_funs_file = open_file(other_funs);
	FILE *other_fun_index_file = open_file(other_index);

	fseek(other_fun_index_file, 0, SEEK_END);
	long int sz = ftell(other_fun_index_file) / (20 + sizeof(size_t));
	fseek(other_fun_index_file, 0, SEEK_SET);

	unsigned char other_sha1sum[20] = { 0 };
	size_t other_offset = 0;
	for (long int i = 0; i < sz; ++i) {
		read_n(other_fun_index_file, other_sha1sum, 20);
		read_n(other_fun_index_file, &other_offset, sizeof(size_t));

		std::string key((char *) other_sha1sum, 20);
		std::map<std::string, size_t>::iterator it = fun_index->find(key);
		if (it == fun_index->end()) { // TODO: Deal with collision
			(*fun_index)[key] = u_offset;
			u_size++;
			size++;

			size_t new_fun_size = 0;
			fseek(other_funs_file, other_offset, SEEK_SET);
			read_n(other_funs_file, &new_fun_size, sizeof(size_t));

			free_content(vector);
			read_n(other_funs_file, vector->buf, new_fun_size);
			vector->capacity = new_fun_size;

			// Write the blob
			write_n(funs_file, &(vector->size), sizeof(size_t));
			write_n(funs_file, vector->buf, vector->size);

			// Acting as NULL for linked-list next pointer
			write_n(funs_file, &(vector->size), sizeof(size_t));

			// Modify u_offset here
			u_offset += vector->size + sizeof(size_t) + sizeof(size_t);

			vector->capacity = 1 << 30;
		}
	}

	fseek(other_funs_file, -1, SEEK_END);
	fseek(other_fun_index_file, -1, SEEK_END);

	close_file(&other_funs_file);
	close_file(&other_fun_index_file);

	return R_NilValue;
}

/**
 * This function assesses if the input is a fun.
 * This function returns true whfuner a value is of an funeger type.
 * @method is_fun
 * @param  SEXP          Any R value
 * @return               1
 */
int is_fun(SEXP value) {
	return (TYPEOF(value) == CLOSXP);
}

/**
 * Adds an fun R value to the funs store.
 * @method add_fun
 * @param  val is a fun R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_fun(SEXP val) {
	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = fun_index->find(key);
	if (it == fun_index->end()) { // TODO: Deal with collision
		(*fun_index)[key] = u_offset;
		u_size++;
		size++;

		// Write the blob
		write_n(funs_file, &(vector->size), sizeof(size_t));
		write_n(funs_file, vector->buf, vector->size);

		// Acting as NULL for linked-list next pofuner
		write_n(funs_file, &(vector->size), sizeof(size_t));

		// Modify u_offset here
		u_offset += vector->size + sizeof(size_t) + sizeof(size_t);

		track_type(val);

		return val;
	} else {
		return R_NilValue;
	}
}

/**
 * This function asks if the C layer has seen an given fun value
 * @method have_seen_fun
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_fun(SEXP val) {
	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = fun_index->find(key);

	if (it == fun_index->end()) {
		return 0;
	} else {
		return 1;
	}
}

/**
 * This function gets the fun value at the index'th place in the database.
 * @method get_fun
 * @return R value
 */
SEXP get_fun(int index) {
	std::map<std::string, size_t>::iterator it = fun_index->begin();
	std::advance(it, index);

	// Get the specified value
	size_t obj_size;
	free_content(vector);
	fseek(funs_file, it->second, SEEK_SET);
	read_n(funs_file, &obj_size, sizeof(size_t));
	read_n(funs_file, vector->buf, obj_size);
	fseek(funs_file, u_offset, SEEK_SET);
	vector->capacity = obj_size;

	SEXP res = unserialize_val(vector);

	// Restore vector
	vector->capacity = 1 << 30;

	return res;
}

/**
 * This function samples from the function stores in the database
 * @method sample_fun
 * @return R value in form of SEXP or throws an error if no function in database
 */
SEXP sample_fun() {
	if (u_size) {
		size_t random_index = rand_size_t() % u_size;
		return get_fun(random_index);
	}

	Rf_error("No funironment values in this database.");
}
