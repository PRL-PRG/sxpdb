if (T) {

test_that("open a new db and close it", {
	db = open_db("test_db/1_open_close/new_db")
	expect_silent(close_db(db))
})

test_that("open existing db", {
	db = open_db("test_db/1_open_close/new_db")
	expect_silent(close_db(db))
})

}
