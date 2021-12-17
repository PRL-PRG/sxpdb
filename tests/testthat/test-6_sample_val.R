if (F) {

test_that("get random val hello after adding hello", {
	open_db("test_db/6_sample_val/get_random_1", create = TRUE)
	s1 = "hello"
	add_val(s1)
	expect_equal(sample_val(), s1)
	close_db()
})

test_that("get random val hello after adding hello redux", {
	open_db("test_db/6_sample_val/get_random_1")
	s1 = "hello"
	expect_equal(sample_val(), s1)
	close_db()
})

test_that("get all three added vals by sample_val in a loop", {
	open_db("test_db/6_sample_val/get_random_2", create = TRUE)
	add_val(TRUE)
	add_val(1)
	add_val("hello")

	vals_added <- list(TRUE, 1, "hello")

	rand_vals = vector('list', 100)
	for (i in seq_along(rand_vals)) {
		rand_vals[[i]] <- sample_val()
	}

	for(rand_val in rand_vals) {
		expect_true(list(rand_val) %in% vals_added)
	}

	close_db()
})

test_that("get all three added vals by sample_val in a loop redux", {
	open_db("test_db/6_sample_val/get_random_2")
	vals_added <- list(TRUE, 1, "hello")

	rand_vals = vector('list', 100)
	for (i in seq_along(rand_vals)) {
		rand_vals[[i]] <- sample_val()
	}

	for(rand_val in rand_vals) {
		expect_true(list(rand_val) %in% vals_added)
	}

	close_db()
})

# real values from stringr::str_detect
val_list <- readRDS("../resource/values.RDS")
vals <- lapply(val_list, function(x) x[[3]])

test_that("have seen real vals from stringr::str_detect", {
	open_db("test_db/6_sample_val/str_detect", create = TRUE)

	lapply(vals, add_val)

	for(val in vals) {
		expect_equal(have_seen(val), TRUE)
	}

	close_db()
})


test_that("get random val from stringr::str_detect from existing db", {
	val_list <- readRDS("../resource/values.RDS")
	vals <- lapply(val_list, function(x) x[[3]])

	open_db("test_db/6_sample_val/str_detect")

	rand_vals <- vector('list', 100)
	for (i in seq_along(rand_vals)) {
		rand_vals[[i]] <- sample_val()
	}

	for(rand_val in rand_vals) {
		expect_true(list(rand_val) %in% vals)
	}

	close_db()
})

test_that("get random val from a database with ints", {
	open_db("test_db/6_sample_val/int_value_get", T)

	ints = as.integer(1:5)

	for (i in seq_along(ints)) {
		add_val(ints[[i]])
		expect_true(have_seen(ints[[i]]))
	}

	vals = vector('list', 100)
	for (i in seq_along(vals)) {
		  vals[[i]] <- sample_val()
	}

	for(val in vals) {
		expect_true(typeof(val) == "integer")
		expect_true(length(val) == 1)
	}

	for (i in seq_along(ints)) {
		expect_true(ints[[i]] %in% vals)
	}

	close_db()
})

test_that("get random val from a database with mix ints and vals", {
	open_db("test_db/6_sample_val/mix_int_value_get", T)

	ints = as.integer(1:5)
	strings = as.character(1:5)

	for (i in seq_along(ints)) {
		add_val(ints[[i]])
		expect_true(have_seen(ints[[i]]))
	}

	for (i in seq_along(strings)) {
		add_val(strings[[i]])
		expect_true(have_seen(strings[[i]]))
	}

	vals = vector('list', 100)
	for (i in seq_along(vals)) {
		  vals[[i]] <- sample_val()
	}

	for (i in seq_along(ints)) {
		expect_true(ints[[i]] %in% vals)
	}

	for (i in seq_along(strings)) {
		expect_true(ints[[i]] %in% vals)
	}

	close_db()
})

test_that("sample few strings", {
	open_db("test_db/6_sample_val/add-few-strings", create = TRUE)
	for (i in 1:10) {
		expect_equal(add_val(paste(as.character(i))), as.character(i))
	}
	close_db()

	open_db("test_db/6_sample_val/add-few-strings")
	for (i in 1:100) {
		expect_true(have_seen(sample_val()))
	}
	close_db()
})

test_that("sample empty string", {
	open_db("test_db/6_sample_val/empty-string", create = TRUE)
	expect_equal(add_val(""), "")
	expect_equal(sample_val(), "")
	expect_equal(size_db(), 1)
	close_db()
})

}
