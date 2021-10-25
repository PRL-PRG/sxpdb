#include "lst_store.h"

#include "helper.h"

#include <map> // std::map
#include <iterator> //advance
#include <string> // std::string, strlen

#include "byte_vector.h"
#include "sha1.h"

FILE *lsts_file = NULL;
FILE *lsts_index_file = NULL;
std::map<std::string, size_t> *lst_index = NULL;


extern size_t size;
extern size_t l_size;
extern size_t l_offset;

extern byte_vector_t vector;

/**
 * Load/create a brand new lst store.
 * @method create_lst_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_lst_store(SEXP lsts) {
	lsts_file = open_file(lsts);
	fseek(lsts_file, l_offset, SEEK_SET);
	return R_NilValue;
}

/**
 * Load an existing lst store.
 * @method load_lst_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_lst_store(SEXP lsts) {
	return init_lst_store(lsts);
}

/**
 * This functions writes lst R val store to file and closes the file.
 * @method close_lst_store
 * @return R_NilValue on success
 */
SEXP close_lst_store() {
	if (lsts_file) {
		close_file(&lsts_file);
	}

	return R_NilValue;
}

/**
 * Load/create a brand new index associated with the lsts store.
 * @method create_lst_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_lst_index(SEXP index) {
	lsts_index_file = open_file(index);
	lst_index = new std::map<std::string, size_t>;
	return R_NilValue;
}

/**
 * Load an existing index associated with the lsts store.
 * @method load_lst_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_lst_index(SEXP index) {
	init_lst_index(index);

	size_t start = 0;
	char hash[20];
	for (size_t i = 0; i < l_size; ++i) {
		read_n(lsts_index_file, hash, 20);
		read_n(lsts_index_file, &start, sizeof(size_t));
		(*lst_index)[std::string(hash, 20)] = start;
	}

	return R_NilValue;
}

/**
 * This function writes the index associated with the lsts store to file
 * and closes the file.
 * @method close_lst_index
 * @return R_NilValue
 */
SEXP close_lst_index() {
	if (lsts_index_file) {
		// TODO: Think about ways to reuse rather than overwrite each time
		fseek(lsts_index_file, 0, SEEK_SET);

		std::map<std::string, size_t>::iterator it;
		for(it = lst_index->begin(); it != lst_index->end(); it++) {
			write_n(lsts_index_file, (void *) it->first.c_str(), 20);
			write_n(lsts_index_file, &(it->second), sizeof(size_t));
		}

		close_file(&lsts_index_file);
	}

	if (lst_index) {
		delete lst_index;
		lst_index = NULL;
	}

	return R_NilValue;
}

/**
 * This functions merges another str store lsto the current str store.
 * @param  other_lsts is the path to the lsts store of a different db.
 * @param  other_index is the path to the index of other_lsts on disk.
 * @method merge_lst_store
 * @return R_NilValue on success
 */
SEXP merge_lst_store(SEXP other_lsts, SEXP other_index) {
	FILE *other_lsts_file = open_file(other_lsts);
	FILE *other_lst_index_file = open_file(other_index);

	fseek(other_lst_index_file, 0, SEEK_END);
	long int sz = ftell(other_lst_index_file) / (20 + sizeof(size_t));
	fseek(other_lst_index_file, 0, SEEK_SET);

	unsigned char other_sha1sum[20] = { 0 };
	size_t other_offset = 0;
	for (long int i = 0; i < sz; ++i) {
		read_n(other_lst_index_file, other_sha1sum, 20);
		read_n(other_lst_index_file, &other_offset, sizeof(size_t));

		std::string key((char *) other_sha1sum, 20);
		std::map<std::string, size_t>::iterator it = lst_index->find(key);
		if (it == lst_index->end()) { // TODO: Deal with collision
			(*lst_index)[key] = l_offset;
			l_size++;
			size++;

			size_t new_lst_size = 0;
			fseek(other_lsts_file, other_offset, SEEK_SET);
			read_n(other_lsts_file, &new_lst_size, sizeof(size_t));

			free_content(vector);
			read_n(other_lsts_file, vector->buf, new_lst_size);
			vector->capacity = new_lst_size;

			// Write the blob
			write_n(lsts_file, &(vector->size), sizeof(size_t));
			write_n(lsts_file, vector->buf, vector->size);

			// Acting as NULL for linked-list next polster
			write_n(lsts_file, &(vector->size), sizeof(size_t));

			// Modify l_offset here
			l_offset += vector->size + sizeof(size_t) + sizeof(size_t);

			vector->capacity = 1 << 30;
		}
	}

	fseek(other_lsts_file, -1, SEEK_END);
	fseek(other_lst_index_file, -1, SEEK_END);

	close_file(&other_lsts_file);
	close_file(&other_lst_index_file);

	return R_NilValue;
}

/**
 * This function assesses if the input is a lst.
 * This function returns true whenver a value is of an lsteger type.
 * @method is_lst
 * @param  SEXP          Any R value
 * @return               1
 */
int is_lst(SEXP value) {
	return (TYPEOF(value) == VECSXP);
}

/**
 * Adds an lst R value to the lsts store.
 * @method add_lst
 * @param  val is a lst R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_lst(SEXP val) {
	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = lst_index->find(key);
	if (it == lst_index->end()) { // TODO: Deal with collision
		(*lst_index)[key] = l_offset;
		l_size++;
		size++;

		// Write the blob
		write_n(lsts_file, &(vector->size), sizeof(size_t));
		write_n(lsts_file, vector->buf, vector->size);

		// Acting as NULL for linked-list next polster
		write_n(lsts_file, &(vector->size), sizeof(size_t));

		// Modify l_offset here
		l_offset += vector->size + sizeof(size_t) + sizeof(size_t);

		track_type(val);

		return val;
	} else {
		return R_NilValue;
	}
}

/**
 * This function asks if the C layer has seen an given lst value
 * @method have_seen_lst
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_lst(SEXP val) {
	serialize_val(vector, val);

	// Get the sha1 hash of the serialized value
	sha1_context ctx;
	unsigned char sha1sum[20];
	sha1_starts(&ctx);
	sha1_update(&ctx, (uint8 *)vector->buf, vector->size);
	sha1_finish(&ctx, sha1sum);

	std::string key((char *) sha1sum, 20);
	std::map<std::string, size_t>::iterator it = lst_index->find(key);

	if (it == lst_index->end()) {
		return 0;
	} else {
		return 1;
	}
}

/**
 * This function gets the lst value at the index'th place in the database.
 * @method get_lst
 * @return R value
 */
SEXP get_lst(int index) {
	std::map<std::string, size_t>::iterator it = lst_index->begin();
	std::advance(it, index);

	// Get the specified value
	size_t obj_size;
	free_content(vector);
	fseek(lsts_file, it->second, SEEK_SET);
	read_n(lsts_file, &obj_size, sizeof(size_t));
	read_n(lsts_file, vector->buf, obj_size);
	fseek(lsts_file, l_offset, SEEK_SET);
	vector->capacity = obj_size;

	SEXP res = unserialize_val(vector);

	// Restore vector
	vector->capacity = 1 << 30;

	return res;
}

/**
 * This function samples from the lstegers stores in the database
 * @method sample_lst
 * @return R value in form of SEXP or throws an error if no lsteger in database
 */
SEXP sample_lst() {
	if (l_size) {
		size_t random_index = rand_size_t() % l_size;
		return get_lst(random_index);
	}

	Rf_error("No list values in this database.");
}
