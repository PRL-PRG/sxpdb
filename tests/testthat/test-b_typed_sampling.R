if (F) {

## Integer Section

test_that("sample int database with no values", {
	open_db("test_db/b_sample_val/simple_int_sampling", create = T)
	expect_error(sample_val("integer"))
	close_db()
})

test_that("sample int database with some int values", {
	open_db("test_db/b_sample_val/simple_int_sampling")
	add_val(1L)
	expect_silent(sample_val("integer"))
	expect_equal(sample_val("integer"), 1L)
	close_db()
})

test_that("sample int database with some int values", {
	open_db("test_db/b_sample_val/int_sampling", create = T)
	add_val(c(1L, 2L, 3L))
	expect_silent(sample_val("integer"))
	expect_equal(sample_val("integer"), c(1L, 2L, 3L))
	close_db()
})

## Double Section

test_that("sample dbl database with no values", {
	open_db("test_db/b_sample_val/simple_dbl_sampling", create = T)
	expect_error(sample_val("double"))
	close_db()
})

test_that("sample dbl database with some dbl values", {
	open_db("test_db/b_sample_val/simple_dbl_sampling")
	add_val(1)
	expect_silent(sample_val("double"))
	expect_equal(sample_val("double"), 1)
	close_db()
})

test_that("sample dbl database with some dbl values", {
	open_db("test_db/b_sample_val/dbl_sampling", create = T)
	add_val(c(1, 2, 3))
	expect_silent(sample_val("double"))
	expect_equal(sample_val("double"), c(1, 2, 3))
	close_db()
})

## Raw Section

test_that("sample raw database with no values", {
	open_db("test_db/b_sample_val/simple_raw_sampling", create = T)
	expect_error(sample_val("raw"))
	close_db()
})

test_that("sample raw database with some raw values", {
	open_db("test_db/b_sample_val/simple_raw_sampling")
	add_val(as.raw(1))
	expect_silent(sample_val("raw"))
	expect_equal(sample_val("raw"), as.raw(1))
	close_db()
})

test_that("sample raw database with some raw values", {
	open_db("test_db/b_sample_val/raw_sampling", create = T)
	add_val(c(as.raw(1), as.raw(2), as.raw(3)))
	expect_silent(sample_val("raw"))
	expect_equal(sample_val("raw"), c(as.raw(1), as.raw(2), as.raw(3)))
	close_db()
})

## String Section

test_that("sample str database with no values", {
	open_db("test_db/b_sample_val/simple_str_sampling", create = T)
	expect_error(sample_val("string"))
	close_db()
})

test_that("sample str database with some str values", {
	open_db("test_db/b_sample_val/simple_str_sampling")
	add_val("string1")
	expect_silent(sample_val("string"))
	expect_equal(sample_val("string"), "string1")
	close_db()
})

test_that("sample str database with some str values", {
	open_db("test_db/b_sample_val/str_sampling", create = T)
	add_val(c("string1", "string2", "string3"))
	expect_silent(sample_val("string"))
	expect_equal(sample_val("string"), c("string1", "string2", "string3"))
	close_db()
})

## Logical Section

test_that("sample log database with no values", {
	open_db("test_db/b_sample_val/simple_log_sampling", create = T)
	expect_error(sample_val("logical"))
	close_db()
})

test_that("sample log database with some log values", {
	open_db("test_db/b_sample_val/simple_log_sampling")
	add_val(T)
	expect_silent(sample_val("logical"))
	expect_equal(sample_val("logical"), T)
	close_db()
})

test_that("sample log database with some log values", {
	open_db("test_db/b_sample_val/log_sampling", create = T)
	add_val(c(T, F, as.logical(NA)))
	expect_silent(sample_val("logical"))
	expect_equal(sample_val("logical"), c(T, F, as.logical(NA)))
	close_db()
})

## Complex Section

test_that("sample cmp database with no values", {
	open_db("test_db/b_sample_val/simple_cmp_sampling", create = T)
	expect_error(sample_val("complex"))
	close_db()
})

test_that("sample cmp database with some cmp values", {
	open_db("test_db/b_sample_val/simple_cmp_sampling")
	add_val(1+1i)
	expect_silent(sample_val("complex"))
	expect_equal(sample_val("complex"), 1+1i)
	close_db()
})

test_that("sample cmp database with some cmp values", {
	open_db("test_db/b_sample_val/cmp_sampling", create = T)
	add_val(c(1+2i, 3+0.4i, 5.5+6.7i))
	expect_silent(sample_val("complex"))
	expect_equal(sample_val("complex"), c(1+2i, 3+0.4i, 5.5+6.7i))
	close_db()
})

## List Section

test_that("sample lst database with no values", {
	open_db("test_db/b_sample_val/simple_lst_sampling", create = T)
	expect_error(sample_val("list"))
	close_db()
})

test_that("sample lst database with some lst values", {
	open_db("test_db/b_sample_val/simple_lst_sampling")
	add_val(list(1,2,3))
	expect_silent(sample_val("list"))
	expect_equal(sample_val("list"), list(1,2,3))
	close_db()
})

## Environment Section

test_that("sample env database with no values", {
	open_db("test_db/b_sample_val/simple_env_sampling", create = T)
	expect_error(sample_val("environment"))
	close_db()
})

test_that("sample env database with some env values", {
	open_db("test_db/b_sample_val/simple_env_sampling")
	add_val(as.environment(1))
	expect_silent(sample_val("environment"))
	expect_equal(sample_val("environment"), as.environment(1))
	close_db()
})

## Function Section

test_that("sample fun database with no values", {
	open_db("test_db/b_sample_val/simple_fun_sampling", create = T)
	expect_error(sample_val("function"))
	close_db()
})

test_that("sample fun database with some fun values", {
	open_db("test_db/b_sample_val/simple_fun_sampling")
	add_val(function(x) {1})
	expect_silent(sample_val("function"))
	expect_equal(sample_val("function"), function(x) {1})
	close_db()
})

## Null Section

test_that("sample null empty database", {
	open_db("test_db/b_sample_val/null_sampling", create = T)
	expect_error(sample_val("null"))
	close_db()
})

test_that("sample null database with null", {
	open_db("test_db/b_sample_val/null_sampling")
	add_val(NULL)
	expect_silent(sample_val("null"))
	expect_equal(sample_val("null"), NULL)
	close_db()
})

## Generic Section

test_that("sample log database with no values", {
	open_db("test_db/b_sample_val/generic_sampling", create = T)
	expect_error(sample_val("generic"))
	close_db()
})

test_that("sample log database with some log values", {
	open_db("test_db/b_sample_val/generic_sampling")
	f <- function(x) (x + 1)
	add_val(formals(f))
	expect_silent(sample_val("generic"))
	args = sample_val("generic")
	expect_equal(formals(f), args)
	close_db()
})

}
