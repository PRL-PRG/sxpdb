#ifndef SXPDB_R_ROARING_SHIM_H
#define SXPDB_R_ROARING_SHIM_H

/* Included at the top of the vendored CRoaring amalgamation (roaring.c).
 *
 * It is kept out of Makevars' PKG_CFLAGS on purpose: a global -include would
 * also hit init.c, whose R headers declare __attribute__((format(printf,...)))
 * — and "#define printf Rprintf" there turns that into an unrecognized
 * format archetype and warns. roaring.c pulls in no R headers, so including
 * the shim only there is clean.
 *
 * The vendored CRoaring amalgamation (roaring.c) prints debug output and
 * deserialization error messages straight to stdout/stderr via printf and
 * fprintf(stderr, ...). R packages must not write to stdout/stderr directly:
 * R CMD check flags it under "checking compiled code" (found printf/putchar/
 * stderr). Rather than editing the auto-generated roaring.c, we transparently
 * redirect those calls to R's console I/O (Rprintf/REprintf).
 *
 * roaring.c does no real file I/O (no fopen/fwrite/FILE*), and every fprintf
 * targets stderr, so ignoring the stream argument and routing to REprintf is
 * safe here.
 */

/* Pull in the real declarations first so the macros below do not collide with
 * stdio.h's own (possibly _FORTIFY_SOURCE-wrapped) definitions of these names. */
#include <stdio.h>
#include <R_ext/Print.h>

#undef printf
#undef fprintf
#undef putchar
#define printf Rprintf
#define fprintf(stream, ...) REprintf(__VA_ARGS__)
#define putchar(c) Rprintf("%c", (int)(c))

#endif /* SXPDB_R_ROARING_SHIM_H */
