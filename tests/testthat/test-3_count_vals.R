if (T) {

test_that("count one val", {
	open_db("test_db/3_count_val/count-one", create = T)
	add_val(1:10)
	expect_equal(count_vals(), 1)
	close_db()
})

test_that("count one val again", {
	open_db("test_db/3_count_val/count-one")
	expect_equal(count_vals(), 1)
	close_db()
})

test_that("count 101 vals", {
	open_db("test_db/3_count_val/count-101", create = T)
	for(i in 1:100) {
		add_val(i)
	}
	add_val(c("a","b","c"))
	expect_equal(count_vals(), 101)
	close_db()
})

test_that("count 101 vals again", {
	open_db("test_db/3_count_val/count-101")
	expect_equal(count_vals(), 101)
	close_db()
})

test_that("count two vals", {
	open_db("test_db/3_count_val/count-two", create = T)
	add_val(1:10)
	add_val("hello")
	expect_equal(count_vals(), 2)
	close_db()
})

test_that("count two vals again", {
	open_db("test_db/3_count_val/count-two")
	expect_equal(count_vals(), 2)
	close_db()
})

}
