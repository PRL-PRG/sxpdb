if (T) {

test_that("add one and record one", {
	open_db("test_db/4_size_db/size-one-u-val", create = TRUE)
	add_val(1:10)
	expect_equal(size_db(), 1)
	close_db()
})

test_that("add two equal vals and record one", {
	open_db("test_db/4_size_db/size-one-u-val2", create = TRUE)
	add_val("hello")
	add_val("hello")
	expect_equal(size_db(), 1)
	close_db()
})

test_that("add two equal vals and record one again", {
	open_db("test_db/4_size_db/size-one-u-val2")
	add_val("hello")
	add_val("hello")
	expect_equal(size_db(), 1)
	close_db()
})

test_that("add two equal vals and record one2", {
	open_db("test_db/4_size_db/size-two", create = TRUE)
	x <- 1
	add_val(x)
	add_val(1)
	expect_equal(size_db(), 1)
	close_db()
})

test_that("add two equal vals and record one2 again", {
	open_db("test_db/4_size_db/size-two")
	x <- 1
	add_val(x)
	add_val(1)
	expect_equal(size_db(), 1)
	close_db()
})


test_that("add 102 vals and record 101", {
	open_db("test_db/4_size_db/size-101", create = TRUE)
	for(i in 1:100) {
		add_val(i)
	}
	add_val(c("a","b","c"))
	expect_equal(size_db(), 101)

	add_val(c("a","b","c"))
	expect_equal(size_db(), 101)
	close_db()
})

test_that("check s_i_size across sessions 1", {
	open_db("test_db/4_size_db/size-101", create = FALSE)
	expect_equal(size_db(), 101)
	expect_equal(size_ints(), 100)
	close_db()
})

test_that("add 5000L and 5001L", {
	open_db("test_db/4_size_db/size-ints", create = TRUE)
	add_val(5000L)
	add_val(5001L)
	expect_equal(size_db(), 2)
	expect_equal(size_ints(), 1)
	close_db()
})

test_that("check s_i_size across sessions 2", {
	open_db("test_db/4_size_db/size-ints", create = FALSE)
	expect_equal(size_db(), 2)
	expect_equal(size_ints(), 1)
	close_db()
})

}
