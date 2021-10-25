if (T) {

test_that("test_hello_1", {
	expect_equal(hello("User"), "Hello, User!")
})

test_that("test_hello_2", {
	expect_equal(hello("World"), "Hello, World!")
})

}
