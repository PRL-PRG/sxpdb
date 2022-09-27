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