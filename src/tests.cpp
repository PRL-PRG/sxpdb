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
