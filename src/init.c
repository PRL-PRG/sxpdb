#define R_NO_REMAP

#include <R.h>
#include <R_ext/Rdynload.h>
#include <Rinternals.h>
#include <stdlib.h> // for NULL // Based on evil suggestion


#include "sxpdb.h"

static const R_CallMethodDef callMethods[] = {
	/* name						casted ptr to function			# of args */
	// Generic record related
	{"open_db",					(DL_FUNC) &open_db,					1},
	{"close_db",				(DL_FUNC) &close_db,				1},
	{"add_val",					(DL_FUNC) &add_val,					2},
	{"have_seen",				(DL_FUNC) &have_seen,				2},
	{"sample_val",				(DL_FUNC) &sample_val,				1},
	{"get_val",					(DL_FUNC) &get_val,					2},
	{"merge_db",					(DL_FUNC) &merge_db,					2},
	{"size_db",           (DL_FUNC) &size_db,					1},
	{"get_meta",        (DL_FUNC) &get_meta,        2},
	{"get_meta_idx",   (DL_FUNC) &get_meta_idx,        2},

	// Must have at the end
	{NULL,						NULL,								0}
};

void R_init_sxpdb(DllInfo* dll) {
	R_registerRoutines(dll, NULL, callMethods, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
	R_RegisterCCallable("sxpdb", "add_val", (DL_FUNC) &add_val);
}

