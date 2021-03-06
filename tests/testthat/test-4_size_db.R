if (T) {

    test_that("add one and record one", {
        db = open_db("test_db/4_size_db/size-one-u-val")
        add_val(db, 1:10)
        expect_equal(size_db(db), 1)
        close_db(db)
    })

    test_that("add two equal vals and record one", {
        db = open_db("test_db/4_size_db/size-one-u-val2")
        add_val(db, "hello")
        add_val(db, "hello")
        expect_equal(size_db(db), 1)
        close_db(db)
    })

    test_that("add two equal vals and record one again", {
        db = open_db("test_db/4_size_db/size-one-u-val2")
        add_val(db, "hello")
        add_val(db, "hello")
        expect_equal(size_db(db), 1)
        close_db(db)
    })

    test_that("add two equal vals and record one2", {
        db = open_db("test_db/4_size_db/size-two")
        x <- 1
        add_val(db, x)
        add_val(db, 1)
        expect_equal(size_db(db), 1)
        close_db(db)
    })

    test_that("add two equal vals and record one2 again", {
        db = open_db("test_db/4_size_db/size-two")
        x <- 1
        add_val(db, x)
        add_val(db, 1)
        expect_equal(size_db(db), 1)
        close_db(db)
    })


    test_that("add 102 vals and record 101", {
        db = open_db("test_db/4_size_db/size-101")
        for(i in 1:100) {
            add_val(db, i)
        }
        add_val(db, c("a","b","c"))
        expect_equal(size_db(db), 101)

        add_val(db, c("a","b","c"))
        expect_equal(size_db(db), 101)
        close_db(db)
    })

    test_that("check size across sessions 1", {
        db = open_db("test_db/4_size_db/size-101")
        expect_equal(size_db(db), 101)
        close_db(db)
    })

    test_that("add 102 vals and record 101", {
        db = open_db("test_db/4_size_db/size-101-string")
        for(i in 1:100) {
            add_val(db, as.character(i))
        }
        add_val(db, c("a","b","c"))
        expect_equal(size_db(db), 101)

        add_val(db, c("a","b","c"))
        expect_equal(size_db(db), 101)
        close_db(db)
    })

    test_that("check size across sessions 1", {
        db = open_db("test_db/4_size_db/size-101-string")
        expect_equal(size_db(db), 101)
        close_db(db)
    })



}
