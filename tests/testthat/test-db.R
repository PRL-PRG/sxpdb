test_that("open db in read mode", {
    db <- db_from_values(list(1L, "tu", 45.9))
    path <- path_db(db)
    close(db)

    db <- open_db(path)
    expect_equal(class(db), "sxpdb")
    close(db)
})

test_that("add_val_origin", {
    db <- open_db(tempfile("sxpdb"), mode = TRUE)
    add_val_origin(db, 34, "pkg", "f", "arg")

    v <- get_value_idx(db, 0)
    expect_equal(v, 34)

    origs <- get_origins_idx(db, 0)
    expect_equal(origs$pkg, "pkg")
    expect_equal(origs$fun, "f")
    expect_equal(origs$param, "arg")

    close_db(db)
})

test_that("read values", {
    l <- list(1L, "tu", 45.9)
    db <- db_from_values(l)
    path <- path_db(db)
    close(db)

    db <- open_db(path)
    v <- view_db(db)
    for(i in seq_along(l)) {
        expect_equal(v[[i]], l[[i]])
    }

    close(db)
})

test_that("open_db in write mode", {
    db <- db_from_values(list(1L, "tu", 45.9))
    path <- path_db(db)
    close(db)

    db <- open_db(path, mode = TRUE)
    expect_equal(class(db), "sxpdb")
    close(db)
})

# test_that("open_db in merge mode", {
#     db <- db_from_values(c(1L, "tu", 45.9))
#     path <- path_db(db)
#     close(db)

#     db <- open_db(path, mode = "merge")
#     expect_equal(class(db), "sxpdb")
#     close(db)
# })