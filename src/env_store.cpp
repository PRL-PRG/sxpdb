#include "env_store.h"

#include "helper.h"

#include <map> // std::map
#include <iterator> //advance
#include <string> // std::string, strlen

#include "byte_vector.h"
#include "sha1.h"

FILE *envs_file = NULL;
FILE *envs_index_file = NULL;
std::map<std::string, size_t> *env_index = NULL;


extern size_t size;
extern size_t e_size;
extern size_t e_offset;

extern byte_vector_t vector;

/**
 * Load/create a brand new env store.
 * @method create_env_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_env_store(SEXP envs) {
	envs_file = open_file(envs);
	fseek(envs_file, e_offset, SEEK_SET);
	return R_NilValue;
}

/**
 * Load an existing env store.
 * @method load_env_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_env_store(SEXP envs) {
	return init_env_store(envs);
}

/**
 * This functions writes env R val store to file and closes the file.
 * @method close_env_store
 * @return R_NilValue on success
 */
SEXP close_env_store() {
	if (envs_file) {
		close_file(&envs_file);
	}

	return R_NilValue;
}

/**
 * Load/create a brand new index associated with the envs store.
 * @method create_env_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_env_index(SEXP index) {
	envs_index_file = open_file(index);
	env_index = new std::map<std::string, size_t>;
	return R_NilValue;
}

/**
 * Load an existing index associated with the envs store.
 * @method load_env_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_env_index(SEXP index) {
	init_env_index(index);

	size_t start = 0;
	char hash[20];
	for (size_t i = 0; i < e_size; ++i) {
		read_n(envs_index_file, hash, 20);
		read_n(envs_index_file, &start, sizeof(size_t));
		(*env_index)[std::string(hash, 20)] = start;
	}

	return R_NilValue;
}

/**
 * This function writes the index associated with the envs store to file
 * and closes the file.
 * @method close_env_index
 * @return R_NilValue
 */
SEXP close_env_index() {
	if (envs_index_file) {
		// TODO: Think about ways to reuse rather than overwrite each time
		fseek(envs_index_file, 0, SEEK_SET);

		std::map<std::string, size_t>::iterator it;
		for(it = env_index->begin(); it != env_index->end(); it++) {
			write_n(envs_index_file, (void *) it->first.c_str(), 20);
			write_n(envs_index_file, &(it->second), sizeof(size_t));
		}

		close_file(&envs_index_file);
	}

	if (env_index) {
		delete env_index;
		env_index = NULL;
	}

	return R_NilValue;
}

/**
 * This functions merges another str store envo the current str store.
 * @param  other_envs is the path to the envs store of a different db.
 * @param  other_index is the path to the index of other_envs on disk.
 * @method merge_env_store
 * @return R_NilValue on success
 */
SEXP merge_env_store(SEXP other_envs, SEXP other_index) {
	FILE *other_envs_file = open_file(other_envs);
	FILE *other_env_index_file = open_file(other_index);

	fseek(other_env_index_file, 0, SEEK_END);
	long int sz = ftell(other_env_index_file) / (20 + sizeof(size_t));
	fseek(other_env_index_file, 0, SEEK_SET);

	unsigned char other_sha1sum[20] = { 0 };
	size_t other_offset = 0;
	for (long int i = 0; i < sz; ++i) {
		read_n(other_env_index_file, other_sha1sum, 20);
		read_n(other_env_index_file, &other_offset, sizeof(size_t));

		std::string key((char *) other_sha1sum, 20);
		std::map<std::string, size_t>::iterator it = env_index->find(key);
		if (it == env_index->end()) { // TODO: Deal with collision
			(*env_index)[key] = e_offset;
			e_size++;
			size++;

			size_t new_env_size = 0;
			fseek(other_envs_file, other_offset, SEEK_SET);
			read_n(other_envs_file, &new_env_size, sizeof(size_t));

			free_content(vector);
			read_n(other_envs_file, vector->buf, new_env_size);
			vector->capacity = new_env_size;

			// Write the blob
			write_n(envs_file, &(vector->size), sizeof(size_t));
			write_n(envs_file, vector->buf, vector->size);

			// Acting as NULL for linked-list next pointer
			write_n(envs_file, &(vector->size), sizeof(size_t));

			// Modify e_offset here
			e_offset += vector->size + sizeof(size_t) + sizeof(size_t);

			vector->capacity = 1 << 30;
		}
	}

	fseek(other_envs_file, -1, SEEK_END);
	fseek(other_env_index_file, -1, SEEK_END);

	close_file(&other_envs_file);
	close_file(&other_env_index_file);

	return R_NilValue;
}

/**
 * This function assesses if the input is a env.
 * This function returns true whenver a value is of an enveger type.
 * @method is_env
 * @param  SEXP          Any R value
 * @return               1
 */
int is_env(SEXP value) {
	return (TYPEOF(value) == ENVSXP);
}

/**
 * Adds an env R value to the envs store.
 * @method add_env
 * @param  val is a env R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_env(SEXP val) {
	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = env_index->find(key);
	if (it == env_index->end()) { // TODO: Deal with collision
		(*env_index)[key] = e_offset;
		e_size++;
		size++;

		// Write the blob
		write_n(envs_file, &(vector->size), sizeof(size_t));
		write_n(envs_file, vector->buf, vector->size);

		// Acting as NULL for linked-list next poenver
		write_n(envs_file, &(vector->size), sizeof(size_t));

		// Modify e_offset here
		e_offset += vector->size + sizeof(size_t) + sizeof(size_t);

		track_type(val);

		return val;
	} else {
		return R_NilValue;
	}
}

/**
 * This function asks if the C layer has seen an given env value
 * @method have_seen_env
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_env(SEXP val) {
	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = env_index->find(key);

	if (it == env_index->end()) {
		return 0;
	} else {
		return 1;
	}
}

/**
 * This function gets the env value at the index'th place in the database.
 * @method get_env
 * @return R value
 */
SEXP get_env(int index) {
	std::map<std::string, size_t>::iterator it = env_index->begin();
	std::advance(it, index);

	// Get the specified value
	size_t obj_size;
	free_content(vector);
	fseek(envs_file, it->second, SEEK_SET);
	read_n(envs_file, &obj_size, sizeof(size_t));
	read_n(envs_file, vector->buf, obj_size);
	fseek(envs_file, e_offset, SEEK_SET);
	vector->capacity = obj_size;

	SEXP res = unserialize_val(vector);

	// Restore vector
	vector->capacity = 1 << 30;

	return res;
}

/**
 * This function samples from the envegers stores in the database
 * @method sample_env
 * @return R value in form of SEXP or throws an error if no enveger in database
 */
SEXP sample_env() {
	if (e_size) {
		size_t random_index = rand_size_t() % e_size;
		return get_env(random_index);
	}

	Rf_error("No environment values in this database.");
}
