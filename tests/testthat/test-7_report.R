if (F) {

test_that("print some report", {
	print("Call report() before opening a new database")
	report()

	open_db("test_db/7_report/some_report", create = TRUE)
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
	print("Call report() after opening a new database and after doing work")
	expect_silent(report())
	close_db()

	open_db("test_db/7_report/some_report")
	print("Call report() after reopening the database")
	report()
	close_db()
})

}