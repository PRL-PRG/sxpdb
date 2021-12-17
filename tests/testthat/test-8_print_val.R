if (F) {

test_that("print no value", {
	open_db("test_db/8_print_vals/print_vals_none", create = TRUE)
	expect_error(print_vals())
	close_db()
})

test_that("print 1 int value", {
	open_db("test_db/8_print_vals/print_vals_one_int", create = TRUE)
	add_val(1L)
	print_vals()
	close_db()
	expect_equal(T, T)
})

test_that("print some values", {
	print("Call print_vals() before opening a new database")
	expect_error(print_vals())

	open_db("test_db/8_print_vals/print_vals_some", create = TRUE)
	add_val("hello")
	add_val("a really long string should not be in simple string store")
	add_val(as.raw(1))
	add_val(as.raw(c(1, 1)))
	add_val(1)
	add_val(50000)
	add_val(1L)
	add_val(50000L)
	add_val(T)
	add_val(1+1i)
	add_val(as.list(c(1,2,3)))
	add_val(as.environment(1))
	add_val(function() (1 + 1))
	add_val(NULL)
	print("Call print_vals() after opening a new database and doing work")
	print_vals()
	close_db()

	open_db("test_db/8_print_vals/print_vals_some")
	print("Call report() after reopening the database")
	# print_vals()
	close_db()
})

}
