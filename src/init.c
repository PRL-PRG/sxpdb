#define R_NO_REMAP

#include <R.h>
#include <R_ext/Rdynload.h>
#include <Rinternals.h>
#include <stdlib.h> // for NULL // Based on evil suggestion


#include "sxpdb.h"

static const R_CallMethodDef callMethods[] = {
	/* name						casted ptr to function			# of args */
	// Generic record related
	{"open_db",					(DL_FUNC) &open_db,					3},
	{"close_db",				(DL_FUNC) &close_db,				1},
	{"add_val",					(DL_FUNC) &add_val,					2},
	{"add_val_origin",  (DL_FUNC) &add_val_origin,	5},
	{"add_origin",      (DL_FUNC) &add_origin,      5},
	{"have_seen",				(DL_FUNC) &have_seen,				2},
	{"sample_val",				(DL_FUNC) &sample_val,		2},
	{"sample_similar",   (DL_FUNC) &sample_similar, 4},
	{"sample_index",    (DL_FUNC) &sample_index,    2},
	{"get_val",					(DL_FUNC) &get_val,					2},
	{"merge_db",					(DL_FUNC) &merge_db,			2},
	{"size_db",           (DL_FUNC) &size_db,			  1},
	{"get_meta",        (DL_FUNC) &get_meta,        2},
	{"get_meta_idx",   (DL_FUNC) &get_meta_idx,     2},
	{"get_origins",    (DL_FUNC) &get_origins,      2},
	{"get_origins_idx", (DL_FUNC) &get_origins_idx, 2},
	{"path_db",         (DL_FUNC) &path_db,         1},
	{"check_db",        (DL_FUNC) &check_db,        2},
	{"map_db",          (DL_FUNC) &map_db,          3},
	{"filter_index_db",   (DL_FUNC) &filter_index_db, 3},
	{"view_db",         (DL_FUNC) &view_db,         2},
	{"view_metadata",   (DL_FUNC) &view_metadata,   2},
	{"view_origins",    (DL_FUNC) &view_origins,    2},
	{"build_indexes",  (DL_FUNC) &build_indexes,    1},
	{"write_mode",     (DL_FUNC) &write_mode,       1},
	{"query_from_value", (DL_FUNC) &query_from_value, 1},
	{"close_query", (DL_FUNC) &close_query,         1},
	{"relax_query", (DL_FUNC) &relax_query,         2},
	{"is_query_empty", (DL_FUNC) &is_query_empty,   1},
	{"extptr_tag",   (DL_FUNC) &extptr_tag,         1},
	{"merge_all_dbs", (DL_FUNC) &merge_all_dbs,     2},


	// Must have at the end
	{NULL,						NULL,								0}
};

void R_init_sxpdb(DllInfo* dll) {
	R_registerRoutines(dll, NULL, callMethods, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
	R_RegisterCCallable("sxpdb", "add_val", (DL_FUNC) &add_val);
	R_RegisterCCallable("sxpdb", "add_val_origin_", (DL_FUNC) &add_val_origin_);
	R_RegisterCCallable("sxpdb", "add_origin_", (DL_FUNC) &add_origin_);
}

