#include "log_store.h"

#include "helper.h"

#include <map> // std::map
#include <iterator> //advance
#include <string> // std::string, strlen

#include "byte_vector.h"
#include "sha1.h"

FILE *logs_file = NULL;
FILE *logs_index_file = NULL;
std::map<std::string, size_t> *log_index = NULL;


extern size_t size;
extern size_t o_size;
extern size_t o_offset;

extern byte_vector_t vector;

/**
 * Load/create a brand new log store.
 * @method create_log_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_log_store(SEXP logs) {
	logs_file = open_file(logs);
	fseek(logs_file, o_offset, SEEK_SET);
	return R_NilValue;
}

/**
 * Load an existing log store.
 * @method load_log_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_log_store(SEXP logs) {
	return init_log_store(logs);
}

/**
 * This functions writes log R val store to file and closes the file.
 * @method close_log_store
 * @return R_NilValue on success
 */
SEXP close_log_store() {
	if (logs_file) {
		close_file(&logs_file);
	}

	return R_NilValue;
}

/**
 * Load/create a brand new index associated with the logs store.
 * @method create_log_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_log_index(SEXP index) {
	logs_index_file = open_file(index);
	log_index = new std::map<std::string, size_t>;
	return R_NilValue;
}

/**
 * Load an existing index associated with the logs store.
 * @method load_log_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_log_index(SEXP index) {
	init_log_index(index);

	size_t start = 0;
	char hash[20];
	for (size_t i = 0; i < o_size; ++i) {
		read_n(logs_index_file, hash, 20);
		read_n(logs_index_file, &start, sizeof(size_t));
		(*log_index)[std::string(hash, 20)] = start;
	}

	return R_NilValue;
}

/**
 * This function writes the index associated with the logs store to file
 * and closes the file.
 * @method close_log_index
 * @return R_NilValue
 */
SEXP close_log_index() {
	if (logs_index_file) {
		// TODO: Think about ways to reuse rather than overwrite each time
		fseek(logs_index_file, 0, SEEK_SET);

		std::map<std::string, size_t>::iterator it;
		for(it = log_index->begin(); it != log_index->end(); it++) {
			write_n(logs_index_file, (void *) it->first.c_str(), 20);
			write_n(logs_index_file, &(it->second), sizeof(size_t));
		}

		close_file(&logs_index_file);
	}

	if (log_index) {
		delete log_index;
		log_index = NULL;
	}

	return R_NilValue;
}

/**
 * This functions merges another str store into the current str store.
 * @param  other_logs is the path to the logs store of a different db.
 * @param  other_index is the path to the index of other_logs on disk.
 * @method merge_log_store
 * @return R_NilValue on success
 */
SEXP merge_log_store(SEXP other_logs, SEXP other_index) {
	FILE *other_logs_file = open_file(other_logs);
	FILE *other_log_index_file = open_file(other_index);

	fseek(other_log_index_file, 0, SEEK_END);
	long int sz = ftell(other_log_index_file) / (20 + sizeof(size_t));
	fseek(other_log_index_file, 0, SEEK_SET);

	unsigned char other_sha1sum[20] = { 0 };
	size_t other_offset = 0;
	for (long int i = 0; i < sz; ++i) {
		read_n(other_log_index_file, other_sha1sum, 20);
		read_n(other_log_index_file, &other_offset, sizeof(size_t));

		std::string key((char *) other_sha1sum, 20);
		std::map<std::string, size_t>::iterator it = log_index->find(key);
		if (it == log_index->end()) { // TODO: Deal with collision
			(*log_index)[key] = o_offset;
			o_size++;
			size++;

			size_t new_log_size = 0;
			fseek(other_logs_file, other_offset, SEEK_SET);
			read_n(other_logs_file, &new_log_size, sizeof(size_t));

			free_content(vector);
			read_n(other_logs_file, vector->buf, new_log_size);
			vector->capacity = new_log_size;

			// Write the blob
			write_n(logs_file, &(vector->size), sizeof(size_t));
			write_n(logs_file, vector->buf, vector->size);

			// Acting as NULL for linked-list next pologer
			write_n(logs_file, &(vector->size), sizeof(size_t));

			// Modify o_offset here
			o_offset += vector->size + sizeof(size_t) + sizeof(size_t);

			vector->capacity = 1 << 30;
		}
	}

	fseek(other_logs_file, -1, SEEK_END);
	fseek(other_log_index_file, -1, SEEK_END);

	close_file(&other_logs_file);
	close_file(&other_log_index_file);

	return R_NilValue;
}

/**
 * This function assesses if the input is a log.
 * @method is_log
 * @param  SEXP          Any R value
 * @return               1 if the value is a logical, 0 otherwise
 */
int is_log(SEXP value) {
	return (TYPEOF(value) == LGLSXP);
}

/**
 * Adds an log R value to the logs store.
 * @method add_log
 * @param  val is a log R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_log(SEXP val) {
	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = log_index->find(key);
	if (it == log_index->end()) { // TODO: Deal with collision
		(*log_index)[key] = o_offset;
		o_size++;
		size++;

		// Write the blob
		write_n(logs_file, &(vector->size), sizeof(size_t));
		write_n(logs_file, vector->buf, vector->size);

		// Acting as NULL for linked-list next pointer
		write_n(logs_file, &(vector->size), sizeof(size_t));

		// Modify o_offset here
		o_offset += vector->size + sizeof(size_t) + sizeof(size_t);

		track_type(val);

		return val;
	} else {
		return R_NilValue;
	}
}

/**
 * This function asks if the C layer has seen an given log value
 * @method have_seen_log
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_log(SEXP val) {
	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = log_index->find(key);

	if (it == log_index->end()) {
		return 0;
	} else {
		return 1;
	}
}

/**
 * This function gets the log value at the index'th place in the database.
 * @method get_dbl
 * @return R value
 */
SEXP get_log(int index) {
	std::map<std::string, size_t>::iterator it = log_index->begin();
	std::advance(it, index);

	// Get the specified value
	size_t obj_size;
	free_content(vector);
	fseek(logs_file, it->second, SEEK_SET);
	read_n(logs_file, &obj_size, sizeof(size_t));
	read_n(logs_file, vector->buf, obj_size);
	fseek(logs_file, o_offset, SEEK_SET);
	vector->capacity = obj_size;

	SEXP res = unserialize_val(vector);

	// Restore vector
	vector->capacity = 1 << 30;

	return res;
}

/**
 * This function samples from the logicals stores in the database
 * @method sample_str
 * @return R value in form of SEXP or throws an error if no logical in database
 */
SEXP sample_log() {
	if (o_size) {
		size_t random_index = rand_size_t() % o_size;
		return get_log(random_index);
	}

	Rf_error("No logicals in this database.");
}
