/*
 * This file uses the Catch unit testing library, alongside
 * testthat's simple bindings, to test a C++ function.
 *
 * For your own packages, ensure that your test files are
 * placed within the `src/` folder, and that you include
 * `LinkingTo: testthat` within your DESCRIPTION file.
 */

// All test files should include the <testthat.h>
// header file.
#include <testthat.h>
#include <algorithm>

#include "search_index.h"
#include "r_compat.h"


context("Index tests") {

  test_that("find_na on sexp_view") {
    Serializer ser(64);

    // Scalar NA real
    SEXP scalar_na = PROTECT(Rf_ScalarReal(NA_REAL));
    sexp_view_t sexp_view = Serializer::unserialize_view(ser.serialize(scalar_na));
    UNPROTECT(1);
    expect_true(find_na(sexp_view));

    // Non-scalar vec with one NA
    SEXP vec_with_na = PROTECT(Rf_allocVector(REALSXP, 10));
    std::fill_n(REAL(vec_with_na), 9, 0.);
    REAL(vec_with_na)[9] = NA_REAL;
    sexp_view =  Serializer::unserialize_view(ser.serialize(vec_with_na));
    UNPROTECT(1);
    expect_true(find_na(sexp_view));

    // character vector
    SEXP na_string = PROTECT(Rf_ScalarString(NA_STRING));
    sexp_view =  Serializer::unserialize_view(ser.serialize(na_string));
    UNPROTECT(1);
    expect_true(find_na(sexp_view));

    // character vectors

    SEXP na_strings = PROTECT(Rf_allocVector(STRSXP, 10));
    const char* s = "aaaaaaaaaaaaa";
    for(int i = 0 ; i < 9 ; i++) {
      SET_STRING_ELT(na_strings, i, Rf_mkCharLen(s, i));
    }
    SET_STRING_ELT(na_strings, 9, NA_STRING);
    sexp_view =  Serializer::unserialize_view(ser.serialize(na_strings));
    UNPROTECT(1);
    expect_true(find_na(sexp_view));
  }

}

// Database::cache_sexp / cached_sexp flag values it has already seen. On R
// after 3.x the tracing bit (RTRACE) is gone, so we repurpose an unused bit of
// the general-purpose (gp) field (see r_compat.h). These tests pin that
// assumption for the R version we are built against: if a future R starts
// using our bit, or changes LEVELS/SETLEVELS, they fail immediately.
context("gp caching-flag assumptions") {

  test_that("cache flag is clear on fresh values and round-trips") {
    // Every SEXPTYPE Database::add_value accepts (all but ENVSXP/CLOSXP;
    // promises are forced before reaching C, so never cached).
    SEXPTYPE vector_types[] = { LGLSXP, INTSXP, REALSXP, CPLXSXP,
                                STRSXP, VECSXP, RAWSXP };
    for(SEXPTYPE t : vector_types) {
      SEXP v = PROTECT(Rf_allocVector(t, 4));
      // The key assumption: R does not use our flag bit for these types, so a
      // freshly allocated value must not already look cached.
      expect_false(sxpdb_is_cached(v));
      sxpdb_set_cached(v);
      expect_true(sxpdb_is_cached(v));
      UNPROTECT(1);
    }

    // pairlist and language objects are stored too
    SEXP lst = PROTECT(Rf_cons(R_NilValue, R_NilValue));
    expect_false(sxpdb_is_cached(lst));
    sxpdb_set_cached(lst);
    expect_true(sxpdb_is_cached(lst));
    UNPROTECT(1);

    SEXP call = PROTECT(Rf_lang2(Rf_install("f"), Rf_install("x")));
    expect_false(sxpdb_is_cached(call));
    sxpdb_set_cached(call);
    expect_true(sxpdb_is_cached(call));
    UNPROTECT(1);

    // Symbols are interned/shared, so only assert the round-trip after setting.
    SEXP sym = Rf_install("sxpdb_gp_test_symbol");
    sxpdb_set_cached(sym);
    expect_true(sxpdb_is_cached(sym));
  }

#if R_VERSION >= R_Version(4, 0, 0)
  test_that("the gp flag bit does not overlap bits R uses") {
    // Pin the exact bit: bits 7-10 of gp are free for the types we store.
    expect_true(SXPDB_CACHED_GP_BIT == (1 << 10));

    // It must not collide with any gp bit R assigns a meaning to: encoding /
    // hashash / ddval / missing (0-6), S4 object (4), assignment-pending (11),
    // special-symbol / base-sym-cached (12,13), binding/active locks (14,15).
    const int reserved = (1<<0)|(1<<1)|(1<<2)|(1<<3)|(1<<4)|(1<<5)|(1<<6)
                       | (1<<11)|(1<<12)|(1<<13)|(1<<14)|(1<<15);
    expect_true((SXPDB_CACHED_GP_BIT & reserved) == 0);
  }
#endif

}
