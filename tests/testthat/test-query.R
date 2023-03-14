test_that("basic sampling", {
     l <- list(1L, "tu", 45.9)
     db <- db_from_values(l)

     v <- sample_val(db)

     close(db)

     expect_true(v %in% l)
})

test_that("sample_index", {
     l <- list(1L, "tu", 45.9)
     db <- db_from_values(l)

     idx <- sample_index(db)
     expect_true(is.numeric(idx))
     v <- get_value_idx(db, idx)

     close(db)

    expect_true(idx %in% (seq_along(l) - 1))
    expect_true(v %in% l)
})

test_that("simple query", {
     l <- list(1L, "tu", 45.9, TRUE)
     db <- db_from_values(l, with_search_index=TRUE)

     q <- query_from_value(FALSE)

     idx <- sample_index(db, q)
     expect_true(is.numeric(idx))
     v <- get_value_idx(db, idx)

     close(db)

    expect_true(idx %in% (seq_along(l) - 1))
    expect_true(v %in% l)
    expect_equal(v, TRUE)
})

test_that("relaxed query", {
     l <- list(1L, "tu", 45.9, TRUE, c(2, 4), NA_real_)
     has_search_index(db) # just for the debugger
     db <- db_from_values(l, with_search_index=TRUE)

     q <- query_from_value(2)
     res <- view_db(db, q)
     expect_length(res, 1)
     expect_equal(res[[1]], 45.9)

     relax_query(q, c("length", "vector"))
     res <- view_db(db, q)
     expect_length(res, 2)
     expect_equal(res[[1]], 45.9)
     expect_equal(res[[2]], c(2, 4))

     close(db)
})