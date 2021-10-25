#include "stats_store.h"

#include "helper.h"

FILE *stats_file = NULL;

// Database Statistics
size_t count = 0; // TODO: Consider: Maybe better to make this a double
size_t size = 0; // TODO: Consider: Maybe better to make this a double

size_t i_size = 0; // number of unique ints encountered
size_t i_offset = 0; // number of bytes in the int store
size_t s_i_size = 0; // number of unique simple ints encountered

size_t d_size = 0; // number of unique dbls encountered
size_t d_offset = 0; // number of bytes in the dbl store
size_t s_d_size = 0; // number of unique simple dbls encountered

size_t r_size = 0; // number of unique raws encountered
size_t r_offset = 0;  // number of bytes in raw store
size_t s_r_size = 0; // number of unique simple raws encountered

size_t s_size = 0; // number of unique strs encountered
size_t s_offset = 0; // number of bytes in str store
size_t s_s_size = 0; // number of unique simple strs encountered
size_t s_s_offset = 0; // number of bytes in simple str store

size_t o_size = 0; // number of unique logicals encountered
size_t o_offset = 0; // number of bytes in logicals store

size_t c_size = 0; // number of unique complexes encountered
size_t c_offset = 0; // number of bytes in complexes store

size_t l_size = 0; // number of unique list encountered
size_t l_offset = 0; // number of bytes in list store

size_t e_size = 0; // number of unique environments encountered
size_t e_offset = 0; // number of bytes in environments store

size_t u_size = 0; // number of unique closure encountered
size_t u_offset = 0; // number of bytes in closure store

size_t n_size = 0; // number of unique nulls encountered
size_t n_count = 0; // number of nulls encountered

size_t g_size = 0; // number of unique generic values encountered
size_t g_offset = 0; // number of bytes in generic store

size_t m_count = 0; // number of times this database has merged another one

// Useful session counters
size_t bytes_read_session = 0;
size_t bytes_written_session = 0;

size_t bytes_serialized_session = 0;
size_t bytes_unserialized_session = 0;

// Useful lifetime counters
size_t bytes_read = 0;
size_t bytes_written = 0;

size_t bytes_serialized = 0;
size_t bytes_unserialized = 0;

/**
 * Load/create a brand new stats store.
 * @method init_stats_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP init_stats_store(SEXP stats) {
	stats_file = open_file(stats);

	// Database Information
	size = 0;
	count = 0;

	i_size = 0;
	i_offset = 0;
	s_i_size = 0;

	d_size = 0;
	d_offset = 0;
	s_d_size = 0;

	r_size = 0;
	r_offset = 0;
	s_r_size = 0;

	s_size = 0;
	s_offset = 0;
	s_s_size = 0;
	s_s_offset = 0;

	o_size = 0;
	o_offset = 0;

	c_size = 0;
	c_offset = 0;

	l_size = 0;
	l_offset = 0;

	e_size = 0;
	e_offset = 0;

	u_size = 0;
	u_offset = 0;

	n_size = 0;
	n_count = 0;

	g_size = 0;
	g_offset = 0;

	m_count = 0;

	// Performance Information
	bytes_read_session = 0;
	bytes_written_session = 0;

	bytes_serialized_session = 0;
	bytes_unserialized_session = 0;

	return R_NilValue;
}

/**
 * Load an existing stats store.
 * @method load_stats_store
 * @return R_NilValue on success, throw and error otherwise
 */
SEXP load_stats_store(SEXP stats) {
	stats_file = open_file(stats);

	// Database Information
	read_n(stats_file, &size, sizeof(size_t));
	read_n(stats_file, &count, sizeof(size_t));

	read_n(stats_file, &i_size, sizeof(size_t));
	read_n(stats_file, &i_offset, sizeof(size_t));
	read_n(stats_file, &s_i_size, sizeof(size_t));

	read_n(stats_file, &d_size, sizeof(size_t));
	read_n(stats_file, &d_offset, sizeof(size_t));
	read_n(stats_file, &s_d_size, sizeof(size_t));

	read_n(stats_file, &r_size, sizeof(size_t));
	read_n(stats_file, &r_offset, sizeof(size_t));
	read_n(stats_file, &s_r_size, sizeof(size_t));

	read_n(stats_file, &s_size, sizeof(size_t));
	read_n(stats_file, &s_offset, sizeof(size_t));
	read_n(stats_file, &s_s_size, sizeof(size_t));
	read_n(stats_file, &s_s_offset, sizeof(size_t));

	read_n(stats_file, &o_size, sizeof(size_t));
	read_n(stats_file, &o_offset, sizeof(size_t));

	read_n(stats_file, &c_size, sizeof(size_t));
	read_n(stats_file, &c_offset, sizeof(size_t));

	read_n(stats_file, &l_size, sizeof(size_t));
	read_n(stats_file, &l_offset, sizeof(size_t));

	read_n(stats_file, &e_size, sizeof(size_t));
	read_n(stats_file, &e_offset, sizeof(size_t));

	read_n(stats_file, &u_size, sizeof(size_t));
	read_n(stats_file, &u_offset, sizeof(size_t));

	read_n(stats_file, &n_size, sizeof(size_t));
	read_n(stats_file, &n_count, sizeof(size_t));

	read_n(stats_file, &g_size, sizeof(size_t));
	read_n(stats_file, &g_offset, sizeof(size_t));

	read_n(stats_file, &m_count, sizeof(size_t));

	// Performance Information
	read_n(stats_file, &bytes_serialized, sizeof(size_t));
	read_n(stats_file, &bytes_unserialized, sizeof(size_t));

	read_n(stats_file, &bytes_read, sizeof(size_t));
	bytes_read += bytes_read_session - sizeof(size_t);

	read_n(stats_file, &bytes_written, sizeof(size_t));

	return R_NilValue;
}

/**
 * This functions adjust information in the stats store based on the merge.
 * @method merge_stats_store
 * @return R_NilValue on success
 */
SEXP merge_stats_store(SEXP other_stats) {
	size_t statistics [34];

	FILE *other_stats_file = open_file(other_stats);
	read_n(other_stats_file, statistics, 34 * sizeof(size_t));
	close_file(&other_stats_file);

	n_size = n_size > statistics[25] ? n_size : statistics[25];
	n_count += statistics[26];

	m_count += 1;

	return R_NilValue;
}

/**
 * This functions writes database statistics to file and closes the file.
 * @method close_stats_store
 * @return R_NilValue on success
 */
SEXP close_stats_store() {
	if (stats_file) {
		fseek(stats_file, 0, SEEK_SET);

		// Database Information
		write_n(stats_file, &size, sizeof(size_t));
		size = 0;

		write_n(stats_file, &count, sizeof(size_t));
		count = 0;

		write_n(stats_file, &i_size, sizeof(size_t));
		i_size = 0;

		write_n(stats_file, &i_offset, sizeof(size_t));
		i_offset = 0;

		write_n(stats_file, &s_i_size, sizeof(size_t));
		s_i_size = 0;

		write_n(stats_file, &d_size, sizeof(size_t));
		d_size = 0;

		write_n(stats_file, &d_offset, sizeof(size_t));
		d_offset = 0;

		write_n(stats_file, &s_d_size, sizeof(size_t));
		s_d_size = 0;

		write_n(stats_file, &r_size, sizeof(size_t));
		r_size = 0;

		write_n(stats_file, &r_offset, sizeof(size_t));
		r_offset = 0;

		write_n(stats_file, &s_r_size, sizeof(size_t));
		s_r_size = 0;

		write_n(stats_file, &s_size, sizeof(size_t));
		s_size = 0;

		write_n(stats_file, &s_offset, sizeof(size_t));
		s_offset = 0;

		write_n(stats_file, &s_s_size, sizeof(size_t));
		s_s_size = 0;

		write_n(stats_file, &s_s_offset, sizeof(size_t));
		s_s_offset = 0;

		write_n(stats_file, &o_size, sizeof(size_t));
		o_size = 0;

		write_n(stats_file, &o_offset, sizeof(size_t));
		o_offset = 0;

		write_n(stats_file, &c_size, sizeof(size_t));
		c_size = 0;

		write_n(stats_file, &c_offset, sizeof(size_t));
		c_offset = 0;

		write_n(stats_file, &l_size, sizeof(size_t));
		l_size = 0;

		write_n(stats_file, &l_offset, sizeof(size_t));
		l_offset = 0;

		write_n(stats_file, &e_size, sizeof(size_t));
		e_size = 0;

		write_n(stats_file, &e_offset, sizeof(size_t));
		e_offset = 0;

		write_n(stats_file, &u_size, sizeof(size_t));
		u_size = 0;

		write_n(stats_file, &u_offset, sizeof(size_t));
		u_offset = 0;

		write_n(stats_file, &n_size, sizeof(size_t));
		n_size = 0;

		write_n(stats_file, &n_count, sizeof(size_t));
		n_count = 0;

		write_n(stats_file, &g_size, sizeof(size_t));
		g_size = 0;

		write_n(stats_file, &g_offset, sizeof(size_t));
		g_offset = 0;

		write_n(stats_file, &m_count, sizeof(size_t));
		m_count = 0;

		// Performance Information
		write_n(stats_file, &bytes_serialized, sizeof(size_t));
		bytes_serialized = 0;

		write_n(stats_file, &bytes_unserialized, sizeof(size_t));
		bytes_unserialized = 0;

		write_n(stats_file, &bytes_read, sizeof(size_t));
		bytes_read = 0;

		// Record the bytes written for write_n and close_file
		bytes_written += sizeof(size_t) + 1;
		write_n(stats_file, &bytes_written, sizeof(size_t));
		// bytes_written = 0; // Must be zeroed out later

		close_file(&stats_file);
		bytes_written = 0; // close_file cause 1 bytes to be written

		// Zero out session information
		bytes_read_session = 0;
		bytes_written_session = 0;

		bytes_serialized_session = 0;
		bytes_unserialized_session = 0;
	}

	return R_NilValue;
}

/**
 * This function samples from the "null store" in the database
 * @method sample_null
 * @return R value in form of SEXP or throws an error if no generic in database
 */
SEXP sample_null() {
	if (n_size) {
		return R_NilValue;
	}

	Rf_error("No generic values in this database.");
}

/**
 * Report database statistics
 * @method print_report
 */
SEXP print_report() {
	// Session
	fprintf(stderr, "Session Information:\n");
	fprintf(stderr, "  bytes read: %lu\n", bytes_read_session);
	fprintf(stderr, "  bytes written: %lu\n", bytes_written_session);
	fprintf(stderr, "  bytes serialized: %lu\n", bytes_serialized_session);
	fprintf(stderr, "  bytes unserialized: %lu\n", bytes_unserialized_session);
	fprintf(stderr, "\n");

	// Lifetime
	fprintf(stderr, "Database Lifetime Information:\n");
	fprintf(stderr, "  bytes read: %lu\n", bytes_read);
	fprintf(stderr, "  bytes written: %lu\n", bytes_written);
	fprintf(stderr, "  bytes serialized: %lu\n", bytes_serialized);
	fprintf(stderr, "  bytes unserialized: %lu\n", bytes_unserialized);
	fprintf(stderr, "\n");

	// Database Statistics
	fprintf(stderr, "Database Statistics\n");
	fprintf(stderr, "  Unique elements in the database: %lu\n", size);
	fprintf(stderr, "  Number of times add_val was called: %lu\n", count);
	fprintf(stderr, "  Number of times this database has merged another: %lu\n", m_count);
	fprintf(stderr, "\n");

	fprintf(stderr, "Database Stores Statistics\n");
	fprintf(stderr, "  Database Specialty Stores Statistics\n");
	fprintf(stderr, "    Elements in simple integer store: %lu\n", s_i_size);
	fprintf(stderr, "    Elements in integer store: %lu\n", i_size);
	fprintf(stderr, "    Elements in simple double store: %lu\n", s_d_size);
	fprintf(stderr, "    Elements in double store: %lu\n", d_size);
	fprintf(stderr, "    Elements in simple raw store: %lu\n", s_r_size);
	fprintf(stderr, "    Elements in raw store: %lu\n", r_size);
	fprintf(stderr, "    Elements in string store: %lu\n", s_size);
	fprintf(stderr, "    Elements in simple string store: %lu\n", s_s_size);
	fprintf(stderr, "    Elements in logical store: %lu\n", o_size);
	fprintf(stderr, "    Elements in complex store: %lu\n", c_size);
	fprintf(stderr, "    Elements in list store: %lu\n", l_size);
	fprintf(stderr, "    Elements in environment store: %lu\n", e_size);
	fprintf(stderr, "    Elements in closure store: %lu\n", u_size);
	fprintf(stderr, "    Elements in null store: %lu\n", n_size);
	fprintf(stderr, "  Database Generic Store Statistics\n");
	fprintf(stderr, "    Elements in generic store: %lu\n", g_size);
	fprintf(stderr, "    Bytes in the generic database: %lu\n", g_offset);
	fprintf(stderr, "\n");

	return R_NilValue;
}

/**
 * This function asks for how many times C add_val has been called.
 * @method count_vals
 * @return number of times add_val was called
 */
SEXP count_vals() {
	SEXP ret = PROTECT(allocVector(INTSXP, 1));
	INTEGER(ret)[0] = count;
	UNPROTECT(1);

	return ret;
}

/**
 * This function asks for how many values are stored in the database
 * @method size_db
 * @return Non-zero numeric R value in form of a SEXP
 */
SEXP size_db() {
	SEXP ret = PROTECT(allocVector(INTSXP, 1));
	INTEGER(ret)[0] = size;
	UNPROTECT(1);

	return ret;
}

/**
 * This function asks for how many simple integer values stored in the database
 * @method size_ints
 * @return Non-zero numeric R value in form of a SEXP
 */
SEXP size_ints() {
	SEXP ret = PROTECT(allocVector(INTSXP, 1));
	INTEGER(ret)[0] = s_i_size;
	UNPROTECT(1);

	return ret;
}
