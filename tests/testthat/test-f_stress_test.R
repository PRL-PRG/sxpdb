if (T) {

## Stress Test Section

test_that("add hello", {
	open_db("test_db/f_stress_test/add-hello", create = T)
	s = "hello"
	expect_equal(add_val(s), s)
	close_db()
})

test_that("add alphabet", {
	open_db("test_db/f_stress_test/add-alphabet", create = T)
	alphabet = c("a", "b", "c", "d", "e", "f", "g", "h", "i")
	alphabet = c(alphabet, "j", "k", "l", "m", "n", "o", "p")
	alphabet = c(alphabet, "q", "r", "s", "t", "u", "v", "w")
	alphabet = c(alphabet, "x", "y", "z")

	alphabets = c(alphabet, alphabet)

	expect_equal(length(add_val(alphabets)), length(alphabets))
	close_db()
})

test_that("add val twice", {
	open_db("test_db/f_stress_test/add-val-twice", create = T)
	once <- "1"
	twice <- "2"
	expect_equal(add_val(once), "1")
	expect_equal(add_val(twice), "2")
	close_db()
})

test_that("add duplicate simple val", {
	open_db("test_db/f_stress_test/add-duplicate-simple-val", create = T)
	expect_equal(add_val(1), 1)
	expect_equal(add_val(1), NULL)
	close_db()
})

test_that("add duplicate simple val again", {
	open_db("test_db/f_stress_test/add-duplicate-simple-val")
	expect_equal(add_val(1), NULL)
	expect_equal(add_val("new value string"), "new value string")
	close_db()
})

test_that("add duplicate logicals", {
	open_db("test_db/f_stress_test/logical", create = TRUE)
	for(i in 1:10) {
		add_val(TRUE)
		add_val(FALSE)
	}

	expect_equal(size_db(), 2)
	close_db()
})

test_that("add double 1", {
	open_db("test_db/f_stress_test/add-1", create = TRUE)
	expect_equal(add_val(1), 1)
	close_db()
})

test_that("add simple double", {
	open_db("test_db/f_stress_test/add-simple-double", create = TRUE)
	expect_equal(add_val(5), 5)
	close_db()
})

test_that("add few strings", {
	open_db("test_db/f_stress_test/add-few-strings", create = TRUE)
	for (i in 1:10) {
		expect_equal(add_val(paste(as.character(i))), as.character(i))
	}
	report()
	close_db()
})

test_that("add many strings", {
	open_db("test_db/f_stress_test/add-many-strings", create = TRUE)
	for (i in 1:3000) {
		expect_equal(add_val(paste(as.character(i))), as.character(i))
	}
	report()
	close_db()
})

test_that("add huge number of strings", {
	open_db("test_db/f_stress_test/add-many-many-strings", create = TRUE)
	for (i in 99990000:100002000) {
		expect_equal(add_val(paste(as.character(i))), as.character(i))
	}
	report()
	close_db()
})

test_that("add empty string", {
	open_db("test_db/f_stress_test/empty-string", create = TRUE)
	expect_equal(add_val(""), "")
	report()
	close_db()
})

}

