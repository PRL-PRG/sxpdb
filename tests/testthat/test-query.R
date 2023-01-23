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
     show_query(q)
     #relax_query(q, "keep_type"); #necessary
     # TODO: bug, matching should operate here, without having to relax
     #show_query(q)
     #relax_query(q, "length")

     print(view_meta_db(db))

     idx <- sample_index(db, q)
     print(idx)
     expect_true(is.numeric(idx))
     v <- get_value_idx(db, idx)

     close(db)

    expect_true(idx %in% (seq_along(l) - 1))
    expect_true(v %in% l)
    expect_equal(v, TRUE)
})