#ifndef SXPDB_R_COMPAT_H
#define SXPDB_R_COMPAT_H

// Compatibility shims for changes in R's C API.
//
// R 4.6.0 removed several accessor functions from the public headers
// (Rinternals.h). They are still present and exported by libR, so we simply
// re-declare the ones this package relies on. On R < 4.6.0 the headers still
// declare them, so nothing needs to change there.
//
// The tracing bit (RTRACE / SET_RTRACE), which we used as a "have we already
// cached this SEXP?" flag, was dropped from the C API after R 3.x -- it is not
// even exported by libR on R 4.5. On modern R we instead repurpose an unused
// bit of the general-purpose (`gp`) field, reached through LEVELS/SETLEVELS.

#include <Rversion.h>
#include <Rinternals.h>

#if R_VERSION >= R_Version(4, 6, 0)
// Still exported by libR, just no longer declared in the public headers.
extern "C" {
SEXP     ATTRIB(SEXP x);
void     SET_ATTRIB(SEXP x, SEXP v);
int      REFCNT(SEXP x);
int      LEVELS(SEXP x);
int      SETLEVELS(SEXP x, int v);
Rboolean Rf_isFrame(SEXP x);
}
#endif

// --- "already cached" flag on a SEXP ---------------------------------------
#if R_VERSION >= R_Version(4, 0, 0)
// RTRACE/SET_RTRACE are unavailable. We use an otherwise-unused general-purpose
// bit instead. Bits 7-10 of `gp` are not claimed by R for any SEXPTYPE we
// store: environments, closures (filtered out in add_value) and promises
// (arguments are forced before reaching us) are the only types that use the
// whole `gp` field, so bit 10 is safe to repurpose here.
#define SXPDB_CACHED_GP_BIT (1 << 10)
static inline int sxpdb_is_cached(SEXP x) {
  return (LEVELS(x) & SXPDB_CACHED_GP_BIT) != 0;
}
static inline void sxpdb_set_cached(SEXP x) {
  SETLEVELS(x, LEVELS(x) | SXPDB_CACHED_GP_BIT);
}
#else
// R 3.x: the dedicated tracing bit is available.
static inline int sxpdb_is_cached(SEXP x) { return RTRACE(x); }
static inline void sxpdb_set_cached(SEXP x) { SET_RTRACE(x, 1); }
#endif

#endif
