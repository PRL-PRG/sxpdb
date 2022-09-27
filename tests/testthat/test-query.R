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
     v <- get_value_idx(db, idx)

     close(db)

    expect_true(idx %in% (seq_along(l) - 1))
    expect_true(v %in% l)
})