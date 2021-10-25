#define R_NO_REMAP

#include <R.h>
#include <R_ext/Rdynload.h>
#include <Rinternals.h>
#include <stdlib.h> // for NULL // Based on evil suggestion


#include "stats_store.h"
#include "generic_store.h"

#include "int_store.h"
#include "simple_int_store.h"

#include "dbl_store.h"
#include "simple_dbl_store.h"

#include "raw_store.h"
#include "simple_raw_store.h"

#include "str_store.h"
#include "simple_str_store.h"

#include "log_store.h"

#include "cmp_store.h"

#include "lst_store.h"

#include "env_store.h"

#include "fun_store.h"

#include "recorder.h"

static const R_CallMethodDef callMethods[] = {
	/* name						casted ptr to function			# of args */
	// Generic record related
	{"setup",					(DL_FUNC) &setup,					0},
	{"teardown",				(DL_FUNC) &teardown,				0},
	{"add_val",					(DL_FUNC) &add_val,					1},
	{"have_seen",				(DL_FUNC) &have_seen,				1},
	{"sample_val",				(DL_FUNC) &sample_val,				0},
	{"get_val",					(DL_FUNC) &get_val,					1},

	// stats store related
	{"init_stats_store",		(DL_FUNC) &init_stats_store,		1},
	{"load_stats_store",		(DL_FUNC) &load_stats_store,		1},
	{"merge_stats_store",		(DL_FUNC) &merge_stats_store,		1},
	{"close_stats_store",		(DL_FUNC) &close_stats_store,		0},
	{"sample_null",				(DL_FUNC) &sample_null,				0},
	{"print_report",			(DL_FUNC) &print_report,			0},
	{"size_db",					(DL_FUNC) &size_db,					0},
	{"size_ints",				(DL_FUNC) &size_ints,				0},
	{"count_vals",				(DL_FUNC) &count_vals,				0},

	// int value store related
	{"init_int_index",			(DL_FUNC) &init_int_index,			1},
	{"init_int_store",			(DL_FUNC) &init_int_store,			1},
	{"load_int_index",			(DL_FUNC) &load_int_index,			1},
	{"load_int_store",			(DL_FUNC) &load_int_store,			1},
	{"merge_int_store",			(DL_FUNC) &merge_int_store,			2},
	{"close_int_index",			(DL_FUNC) &close_int_index,			0},
	{"close_int_store",			(DL_FUNC) &close_int_store,			0},
	{"sample_int",				(DL_FUNC) &sample_int,				0},

	// simple ints store related
	{"init_simple_int_store",	(DL_FUNC) &init_simple_int_store,	1},
	{"load_simple_int_store",	(DL_FUNC) &load_simple_int_store,	1},
	{"merge_simple_int_store",	(DL_FUNC) &merge_simple_int_store,	1},	{"close_simple_int_store",	(DL_FUNC) &close_simple_int_store,	0},

	// dbl value store related
	{"init_dbl_index",			(DL_FUNC) &init_dbl_index,			1},
	{"init_dbl_store",			(DL_FUNC) &init_dbl_store,			1},
	{"load_dbl_index",			(DL_FUNC) &load_dbl_index,			1},
	{"load_dbl_store",			(DL_FUNC) &load_dbl_store,			1},
	{"merge_dbl_store",			(DL_FUNC) &merge_dbl_store,			2},
	{"close_dbl_index",			(DL_FUNC) &close_dbl_index,			0},
	{"close_dbl_store",			(DL_FUNC) &close_dbl_store,			0},
	{"sample_dbl",				(DL_FUNC) &sample_dbl,				0},

	// simple doubles store related
	{"init_simple_dbl_store",	(DL_FUNC) &init_simple_dbl_store,	1},
	{"load_simple_dbl_store",	(DL_FUNC) &load_simple_dbl_store,	1},
	{"merge_simple_dbl_store",	(DL_FUNC) &merge_simple_dbl_store,	1},		{"close_simple_dbl_store",	(DL_FUNC) &close_simple_dbl_store,	0},

	// raw value store related
	{"init_raw_index",			(DL_FUNC) &init_raw_index,			1},
	{"init_raw_store",			(DL_FUNC) &init_raw_store,			1},
	{"load_raw_index",			(DL_FUNC) &load_raw_index,			1},
	{"load_raw_store",			(DL_FUNC) &load_raw_store,			1},
	{"merge_raw_store",			(DL_FUNC) &merge_raw_store,			2},
	{"close_raw_index",			(DL_FUNC) &close_raw_index,			0},
	{"close_raw_store",			(DL_FUNC) &close_raw_store,			0},
	{"sample_raw",				(DL_FUNC) &sample_raw,				0},

	// simple raw store related
	{"init_simple_raw_store",	(DL_FUNC) &init_simple_raw_store,	1},
	{"load_simple_raw_store",	(DL_FUNC) &load_simple_raw_store,	1},	{"merge_simple_raw_store",	(DL_FUNC) &merge_simple_raw_store,	1},
	{"close_simple_raw_store",	(DL_FUNC) &close_simple_raw_store,	0},

	// str value store related
	{"init_str_index",			(DL_FUNC) &init_str_index,			1},
	{"init_str_store",			(DL_FUNC) &init_str_store,			1},
	{"load_str_index",			(DL_FUNC) &load_str_index,			1},
	{"load_str_store",			(DL_FUNC) &load_str_store,			1},
	{"merge_str_store",			(DL_FUNC) &merge_str_store,			2},
	{"close_str_index",			(DL_FUNC) &close_str_index,			0},
	{"close_str_store",			(DL_FUNC) &close_str_store,			0},
	{"sample_str",				(DL_FUNC) &sample_str,				0},

	// simple string store related
	{"init_simple_str_index",	(DL_FUNC) &init_simple_str_index,	1},
	{"init_simple_str_store",	(DL_FUNC) &init_simple_str_store,	1},
	{"load_simple_str_index",	(DL_FUNC) &load_simple_str_index,	1},
	{"load_simple_str_store",	(DL_FUNC) &load_simple_str_store,	1},
	{"merge_simple_str_store",	(DL_FUNC) &merge_simple_str_store,	2},
	{"close_simple_str_index",	(DL_FUNC) &close_simple_str_index,	0},
	{"close_simple_str_store",	(DL_FUNC) &close_simple_str_store,	0},

	// logical R value store related
	{"init_log_index",			(DL_FUNC) &init_log_index,			1},
	{"init_log_store",			(DL_FUNC) &init_log_store,			1},
	{"load_log_index",			(DL_FUNC) &load_log_index,			1},
	{"load_log_store",			(DL_FUNC) &load_log_store,			1},
	{"merge_log_store",			(DL_FUNC) &merge_log_store,			2},
	{"close_log_index",			(DL_FUNC) &close_log_index,			0},
	{"close_log_store",			(DL_FUNC) &close_log_store,			0},
	{"sample_log",				(DL_FUNC) &sample_log,				0},

	// complex R value store related
	{"init_cmp_index",			(DL_FUNC) &init_cmp_index,			1},
	{"init_cmp_store",			(DL_FUNC) &init_cmp_store,			1},
	{"load_cmp_index",			(DL_FUNC) &load_cmp_index,			1},
	{"load_cmp_store",			(DL_FUNC) &load_cmp_store,			1},
	{"merge_cmp_store",			(DL_FUNC) &merge_cmp_store,			2},
	{"close_cmp_index",			(DL_FUNC) &close_cmp_index,			0},
	{"close_cmp_store",			(DL_FUNC) &close_cmp_store,			0},
	{"sample_cmp",				(DL_FUNC) &sample_cmp,				0},

	// list R value store related
	{"init_lst_index",			(DL_FUNC) &init_lst_index,			1},
	{"init_lst_store",			(DL_FUNC) &init_lst_store,			1},
	{"load_lst_index",			(DL_FUNC) &load_lst_index,			1},
	{"load_lst_store",			(DL_FUNC) &load_lst_store,			1},
	{"merge_lst_store",			(DL_FUNC) &merge_lst_store,			2},
	{"close_lst_index",			(DL_FUNC) &close_lst_index,			0},
	{"close_lst_store",			(DL_FUNC) &close_lst_store,			0},
	{"sample_lst",				(DL_FUNC) &sample_lst,				0},

	// environment R value store related
	{"init_env_index",			(DL_FUNC) &init_env_index,			1},
	{"init_env_store",			(DL_FUNC) &init_env_store,			1},
	{"load_env_index",			(DL_FUNC) &load_env_index,			1},
	{"load_env_store",			(DL_FUNC) &load_env_store,			1},
	{"merge_env_store",			(DL_FUNC) &merge_env_store,			2},
	{"close_env_index",			(DL_FUNC) &close_env_index,			0},
	{"close_env_store",			(DL_FUNC) &close_env_store,			0},
	{"sample_env",				(DL_FUNC) &sample_env,				0},

	// function R value store related
	{"init_fun_index",			(DL_FUNC) &init_fun_index,			1},
	{"init_fun_store",			(DL_FUNC) &init_fun_store,			1},
	{"load_fun_index",			(DL_FUNC) &load_fun_index,			1},
	{"load_fun_store",			(DL_FUNC) &load_fun_store,			1},
	{"merge_fun_store",			(DL_FUNC) &merge_fun_store,			2},
	{"close_fun_index",			(DL_FUNC) &close_fun_index,			0},
	{"close_fun_store",			(DL_FUNC) &close_fun_store,			0},
	{"sample_fun",				(DL_FUNC) &sample_fun,				0},

	// generic R value store related
	{"init_generic_index",		(DL_FUNC) &init_generic_index,		1},
	{"init_generic_store",		(DL_FUNC) &init_generic_store,		1},
	{"load_generic_index",		(DL_FUNC) &load_generic_index,		1},
	{"load_generic_store",		(DL_FUNC) &load_generic_store,		1},
	{"merge_generic_store",		(DL_FUNC) &merge_generic_store,		2},
	{"close_generic_index",		(DL_FUNC) &close_generic_index,		0},
	{"close_generic_store",		(DL_FUNC) &close_generic_store,		0},
	{"sample_generic",			(DL_FUNC) &sample_generic,			0},

	// Must have at the end
	{NULL,						NULL,								0}
};

void R_init_record(DllInfo* dll) {
	R_registerRoutines(dll, NULL, callMethods, NULL, NULL);
	R_RegisterCCallable("record", "add_val", (DL_FUNC) &add_val);
}

