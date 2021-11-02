#include "simple_str_store.h"

#include "helper.h"

#include "crc.h" // CRC32

#include <map> // std::map
#include <iterator> //advance
#include <string> // strlen
#include <cstdint> // uint32_t

FILE *s_strs_file = NULL;
FILE *s_str_index_file = NULL;
std::map<uint32_t, size_t> *s_str_index = NULL;
size_t MAX_LENGTH = 8;

extern size_t size;
extern size_t s_size;
extern size_t s_s_size;
extern size_t s_s_offset;

/**
 * Load/create a brand new simple string store.
 * @method init_simple_str_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_simple_str_store(SEXP strs) {
	s_strs_file = open_file(strs);
	fseek(s_strs_file, s_s_offset, SEEK_SET);
	return R_NilValue;
}

/**
 * Load an existing simple string store.
 * @method load_simple_str_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_simple_str_store(SEXP strs) {
	return init_simple_str_store(strs);
}

/**
 * This functions writes simple string store to file and closes the file.
 * @method close_simple_str_store
 * @return R_NilValue on success
 */
SEXP close_simple_str_store() {
	if (s_strs_file) {
		close_file(&s_strs_file);
	}

	return R_NilValue;
}

/**
 * Load/create a brand new index associated with the simple string store.
 * @method init_simple_str_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_simple_str_index(SEXP index) {
	s_str_index_file = open_file(index);
	s_str_index = new std::map<uint32_t, size_t>;
	return R_NilValue;
}

/**
 * Load an existing index associated with the simple string store.
 * @method load_simple_str_index
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_simple_str_index(SEXP index) {
	init_simple_str_index(index);

	uint32_t hash = 0;
	size_t idx = 0;
	for (size_t i = 0; i < s_s_size; ++i) {
		read_n(s_str_index_file, &hash, sizeof(uint32_t));
		read_n(s_str_index_file, &idx, sizeof(size_t));
		(*s_str_index)[hash] = idx;
	}

	return R_NilValue;
}

/**
 * This functions merges another str store into the current str store.
 * @param  other_strs is the path to the strs store on disk of a different db.
 * @param  other_index is the path to the index of other_strs on disk
 * @method merge_simple_str_store
 * @return R_NilValue on success
 */
SEXP merge_simple_str_store(SEXP other_strs, SEXP other_index) {
	FILE *other_s_strs_file = open_file(other_strs);
	FILE *other_s_str_index_file = open_file(other_index);

	fseek(other_s_strs_file, 0, SEEK_END);
	long int sz = ftell(other_s_strs_file) / 8;

	uint32_t hash = 0;
	size_t idx = 0;
	for (long int i = 0; i < sz; ++i) {
		read_n(other_s_str_index_file, &hash, sizeof(uint32_t));
		read_n(other_s_str_index_file, &idx, sizeof(size_t));

		std::map<uint32_t, size_t>::iterator it = s_str_index->find(hash);
		if (it == s_str_index->end()) { // TODO: Deal with collisions
			(*s_str_index)[hash] = s_s_size;
			s_s_size++;
			s_size += 1;
			size++;

			char buf [9] = { 0 };
			fseek(other_s_strs_file, idx * 8, SEEK_SET);
			read_n(other_s_strs_file, buf, 8);

			size_t len = strlen(buf);
			write_n(s_strs_file, buf, len);
			if (len < MAX_LENGTH) {
				char empty [MAX_LENGTH] = { 0 };
				write_n(s_strs_file, empty, MAX_LENGTH - len);
			}

			s_s_offset += MAX_LENGTH;
		}
	}

	fseek(other_s_strs_file, -1, SEEK_END);
	fseek(other_s_str_index_file, -1, SEEK_END);

	close_file(&other_s_strs_file);
	close_file(&other_s_str_index_file);

	return R_NilValue;
}

/**
 * This function writes the index associated with the simple str store to file
 * and closes the file.
 * @method close_simple_str_index
 * @return R_NilValue
 */
SEXP close_simple_str_index() {
	if (s_str_index_file) {
		// TODO: Think about ways to reuse rather than overwrite each time
		fseek(s_str_index_file, 0, SEEK_SET);

		std::map<uint32_t, size_t>::iterator it;
		for(it = s_str_index->begin(); it != s_str_index->end(); it++) {
			write_n(s_str_index_file, (void *) &(it->first), sizeof(uint32_t));
			write_n(s_str_index_file, &(it->second), sizeof(size_t));
		}

		close_file(&s_str_index_file);
	}

	if (s_str_index) {
		delete s_str_index;
		s_str_index = NULL;
	}

	return R_NilValue;
}

/**
 * This function assesses if the input is a simple string.
 * This function would always return true, therefore no need to implement it.
 * @method is_simple_str
 * @param  SEXP          Any R value
 * @return               1 if the value is a simple string, else 0
 */
int is_simple_str(SEXP value) {
	if (IS_SIMPLE_SCALAR(value, STRSXP)
		&& strlen(CHAR(STRING_ELT(value, 0))) <= MAX_LENGTH) {
		return 1;
	}
	return 0;
}

/**
 * Adds an simple string value to the simple string store.
 * @method add_simple_str
 * @param  val is a generic R value
 * @return val if val hasn't been added to store before, else R_NilValue
 */
SEXP add_simple_str(SEXP val) {
	const char* c_val = CHAR(STRING_ELT(val, 0));
	uint32_t hash = CRC32(c_val);
	std::map<uint32_t, size_t>::iterator it = s_str_index->find(hash);
	if (it == s_str_index->end()) { // TODO: Deal with collisions
		(*s_str_index)[hash] = s_s_size;
		s_s_size++;
		s_size++;
		size++;

		size_t len = strlen(c_val);
		write_n(s_strs_file, (void *) c_val, len);
		if (len < MAX_LENGTH) {
			char buf [MAX_LENGTH] = { 0 };
			write_n(s_strs_file, buf, MAX_LENGTH - len);
		}

		s_s_offset += MAX_LENGTH;

		return val;
	}

	return R_NilValue;
}

/**
 * This function asks if the C layer has seen an given simple string
 * @method have_seen_simple_str
 * @param  val       R value in form of SEXP
 * @return           1 if the value has been encountered before, else 0
 */
int have_seen_simple_str(SEXP val) {
	const char* c_val = CHAR(STRING_ELT(val, 0));
	uint32_t hash = CRC32(c_val);

	std::map<uint32_t, size_t>::iterator it = s_str_index->find(hash);

	if (it == s_str_index->end()) {
		return 0;
	} else {
		return 1;
	}
}

/**
 * This function gets the simple string at the index'th place in the database.
 * @method get_simple_str
 * @return R value
 */
SEXP get_simple_str(int index) {
	char buf [9] = { 0 };

	fseek(s_strs_file, index * 8, SEEK_SET);
	read_n(s_strs_file, buf, 8);
	fseek(s_strs_file, s_s_offset, SEEK_SET);

	SEXP r_res = PROTECT(allocVector(STRSXP, 1));
	SET_STRING_ELT(r_res, 0, mkChar(buf));
	UNPROTECT(1);

	return r_res;
}
