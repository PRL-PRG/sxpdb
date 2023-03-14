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
#include "search_index.h"


context("Index tests") {

  test_that("find_na on sexp_view") {
    Serializer ser(64);
    
    const sexp_view_t sexp_view = Serializer::unserialize_view(ser.serialize(Rf_ScalarReal(NA_REAL)));

    expect_true(find_na(sexp_view));
  }

}
