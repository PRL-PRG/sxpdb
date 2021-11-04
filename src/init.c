#define R_NO_REMAP

#include <R.h>
#include <R_ext/Rdynload.h>
#include <Rinternals.h>
#include <stdlib.h> // for NULL // Based on evil suggestion


#include "sxpdb.h"

static const R_CallMethodDef callMethods[] = {
	/* name						casted ptr to function			# of args */
	// Generic record related
	{"setup",					(DL_FUNC) &setup,					0},
	{"teardown",				(DL_FUNC) &teardown,				0},
	{"add_val",					(DL_FUNC) &add_val,					1},
	{"have_seen",				(DL_FUNC) &have_seen,				1},
	{"sample_val",				(DL_FUNC) &sample_val,				0},
	{"get_val",					(DL_FUNC) &get_val,					1},

	// Must have at the end
	{NULL,						NULL,								0}
};

void R_init_sxpdb(DllInfo* dll) {
	R_registerRoutines(dll, NULL, callMethods, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
	R_RegisterCCallable("sxpdb", "add_val", (DL_FUNC) &add_val);
}

