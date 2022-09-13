test_that("open db in read mode", {
    db <- db_from_values(c(1L, "tu", 45.9))
    path <- path_db(db)
    close(db)

    db <- open_db(path)
    expect_equal(class(db), "sxpdb")
    close(db)
})

test_that("open_db in write mode", {
    db <- db_from_values(c(1L, "tu", 45.9))
    path <- path_db(db)
    close(db)

    db <- open_db(path, mode = TRUE)
    expect_equal(class(db), "sxpdb")
    close(db)
})

test_that("open_db in merge mode", {
    db <- db_from_values(c(1L, "tu", 45.9))
    path <- path_db(db)
    close(db)

    db <- open_db(path, mode = "merge")
    expect_equal(class(db), "sxpdb")
    close(db)
})