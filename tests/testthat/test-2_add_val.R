if (T) {

    ## Simple cases

    test_that("add simple int", {
        db = open_db("test_db/2_add_val/simple_int")
        expect_silent(add_val(db, 1L))
        expect_equal(sample_val(db), 1L)
        close_db(db)
    })

    test_that("add vector int", {
        db = open_db("test_db/2_add_val/vector_int_1")
        expect_silent(add_val(db, c(1L, 2L, 3L)))
        expect_equal(sample_val(db), c(1L, 2L, 3L))
        close_db(db)
    })

    test_that("add first", {
        db = open_db("test_db/2_add_val/add-again")
        expect_silent(add_val(db, 5000L))
        expect_equal(sample_val(db), 5000L)
        close_db(db)
    })

    test_that("add again", {
        db = open_db("test_db/2_add_val/add-again")
        expect_silent(add_val(db, 5000L))

        for (i in 1:100)
            expect_equal(sample_val(db), 5000L)

        close_db(db)
    })

    test_that("add 10 vals", {
        db = open_db("test_db/2_add_val/add-10-vals")
        expect_silent(add_val(db, 1:10))
        expect_equal(sample_val(db), 1:10)
        close_db(db)
    })

    ##  Environments

    test_that("add environments", {
        db = open_db("test_db/2_add_val/environment")
        add_val(db, as.environment(1))
        expect_error(sample_db(db))
        close_db(db)
    })


}

if (F) {

test_that("add 100 vals", {
	open_db("test_db/2_add_val/add-100-vals", create = T)
	expect_equal(add_val(1:100), 1:100)
	close_db()
})

test_that("add large vals", {
	open_db("test_db/2_add_val/add-large-vals", create = T)
	expect_equal(add_val(c(1:500, 600:100000)), c(1:500, 600:100000))
	close_db()
})

test_that("add attributed int", {
	open_db("test_db/2_add_val/add-attributed-0L", create = TRUE)

	x <- 0L
	attr(x, "name") <- "attributed 0"

	expect_equal(add_val(x), x)
	expect_equal(size_db(), 1)
	close_db()
})

## Double Section

test_that("add simple dbl", {
	open_db("test_db/2_add_val/simple_dbl_1", create = T)
	expect_equal(add_val(1), 1)
	close_db()
})

test_that("add scalar int", {
	open_db("test_db/2_add_val/dbl_1", create = T)
	expect_equal(add_val(10000), 10000)
	close_db()
})

test_that("add vector dbl", {
	open_db("test_db/2_add_val/dbl_1")
	expect_equal(add_val(c(1, 2, 3)), c(1, 2, 3))
	close_db()
})

test_that("add attributed dbl", {
	open_db("test_db/2_add_val/add-attributed-0", create = TRUE)

	x <- 0
	attr(x, "name") <- "attributed 0"

	expect_equal(add_val(x), x)
	expect_equal(size_db(), 1)
	close_db()
})

## Raw Section

test_that("add simple raw", {
	open_db("test_db/2_add_val/simple_raw_1", create = T)
	expect_equal(add_val(as.raw(1)), as.raw(1))
	close_db()
})

test_that("add scalar raw", {
	open_db("test_db/2_add_val/raw_1", create = T)
	expect_equal(add_val(as.raw(2)), as.raw(2))
	close_db()
})

test_that("add vector raw", {
	open_db("test_db/2_add_val/raw_1")
	expect_equal(add_val(c(as.raw(1), as.raw(2), as.raw(3))),
				c(as.raw(1), as.raw(2), as.raw(3)))
	close_db()
})

test_that("add attributed dbl", {
	open_db("test_db/2_add_val/add-attributed-raw", create = TRUE)

	x <- as.raw(0)
	attr(x, "name") <- "attributed 0"

	expect_equal(add_val(x), x)
	expect_equal(size_db(), 1)
	close_db()
})

## String Section

test_that("add simple str", {
	open_db("test_db/2_add_val/simple_str_1", create = T)
	expect_equal(add_val("1"), "1")
	close_db()
})

test_that("add scalar str", {
	open_db("test_db/2_add_val/str_1", create = T)
	expect_equal(add_val("2"), "2")
	close_db()
})

test_that("add vector str", {
	open_db("test_db/2_add_val/str_1")
	expect_equal(add_val(c("1", "2", "3")),
				c("1", "2", "3"))
	close_db()
})

test_that("add attributed str", {
	open_db("test_db/2_add_val/add-attributed-str", create = TRUE)

	x <- "0"
	attr(x, "name") <- "attributed 0"

	expect_equal(add_val(x), x)
	expect_equal(size_db(), 1)
	close_db()
})

## Logical Section

test_that("add scalar log", {
	open_db("test_db/2_add_val/log_1", create = T)
	expect_equal(add_val(T), T)
	close_db()
})

test_that("add vector log", {
	open_db("test_db/2_add_val/log_1")
	expect_equal(add_val(c(T, F, as.logical(NA))),
				c(T, F, as.logical(NA)))
	close_db()
})

test_that("add attributed log", {
	open_db("test_db/2_add_val/add-attributed-log", create = TRUE)

	x <- T
	attr(x, "name") <- "attributed 0"

	expect_equal(add_val(x), x)
	expect_equal(size_db(), 1)
	close_db()
})

## Complex Section

test_that("add scalar cmp", {
	open_db("test_db/2_add_val/cmp_1", create = T)
	expect_equal(add_val(1+1i), 1+1i)
	close_db()
})

test_that("add vector cmp", {
	open_db("test_db/2_add_val/cmp_1")
	expect_equal(add_val(c(2+2i, 3+3i, 3.14 + 2.72i)),
				c(2+2i, 3+3i, 3.14 + 2.72i))
	close_db()
})

test_that("add attributed cmp", {
	open_db("test_db/2_add_val/add-attributed-cmp", create = TRUE)

	x <- 4+4i
	attr(x, "name") <- "attributed 0"

	expect_equal(add_val(x), x)
	expect_equal(size_db(), 1)
	close_db()
})

## List Section

test_that("add list", {
	open_db("test_db/2_add_val/lst_1", T)
	expect_equal(add_val(as.list(c(2+2i, 3+3i, 3.14 + 2.72i))),
				as.list(c(2+2i, 3+3i, 3.14 + 2.72i)))
	close_db()
})

test_that("add attributed list", {
	open_db("test_db/2_add_val/add-attributed-lst", create = TRUE)

	x <- as.list(c(1,2,3,4,5))
	attr(x, "name") <- "attributed 0"

	expect_equal(add_val(x), x)
	expect_equal(size_db(), 1)
	close_db()
})

## Environment Section

test_that("add environment", {
	open_db("test_db/2_add_val/env_1", T)
	expect_equal(add_val(as.environment(1)),
				as.environment(1))
	close_db()
})

test_that("add attributed environment", {
	open_db("test_db/2_add_val/add-attributed-env", create = TRUE)

	x <- as.environment(1)
	attr(x, "name") <- "attributed 0"

	expect_equal(add_val(x), x)
	expect_equal(size_db(), 1)

	close_db()
})

## Function Section

test_that("add function", {
	open_db("test_db/2_add_val/fun_1", T)
	f = function(x) {x + 1}
	expect_equal(add_val(f), f)
	close_db()
})

test_that("add attributed function", {
	open_db("test_db/2_add_val/add-attributed-fun", create = TRUE)

	x <- function(x) {x + 1}
	attr(x, "name") <- "attributed 0"

	expect_equal(add_val(x), x)
	expect_equal(size_db(), 1)

	close_db()
})

}
