test_that("merge", {
    l1 <- list(1L, "tu", 45.9)
    db1 <- db_from_values(l1)

    l2 <- list(3L, "tu", 45.9)
    db2 <- db_from_values(l2)

    mapping <- merge_into(db2, db1)

    l <- unique(c(l1, l2))

    expect_equal(size_db(db2), length(l))
    expect_equal(view_db(db2), l)

    close(db1)
    close(db2)
})

test_that("merge all", {
    l1 <- list(1L, "tu", 45.9)
    db1 <- db_from_values(l1)
    path1 <- path_db(db1)
    nb1 <- size_db(db1)
    close(db1)

    l2 <- list(3L, "tu", 45.9)
    db2 <- db_from_values(l2)
    path2 <- path_db(db2)
    nb2 <- size_db(db2)
    close(db2)

    result_db_name <- tempfile("result_sxpdb")
    res <- merge_all_dbs(c(path1, path2), result_db_name, legacy = FALSE)

    l <- unique(c(l1, l2))

    # try to popen the result db
    result_db <- open_db(result_db_name)

    expect_length(res, 2)
    expect_lte(nb1, size)
    expect_lte(nb2, size)
    expect_equal(size_db(result_db), length(l))
    expect_equal(view_db(db2), l)

    close(result_db)
})