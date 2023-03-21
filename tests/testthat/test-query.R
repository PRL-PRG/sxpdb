
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
     l <- list(1L, "tu", 45.9, TRUE, c(2.1, 4), NA_real_)
     db <- db_from_values(l, with_search_index=TRUE)

     expect_equal(nb_values_db(db), length(l))

     q <- query_from_value(2)
     res <- view_db(db, q)

     expect_length(res, 1)
     expect_equal(res[[1]], 45.9)

     relax_query(q, c("length", "vector"))
     res <- view_db(db, q)
     expect_length(res, 2)
     expect_equal(res[[1]], 45.9)
     expect_equal(res[[2]], c(2.1, 4))

     relax_query(q, "na")
     expect_equal(nb_values_db(db, q), 3)

     q <- query_from_value(2)
     relax_query(q, "keep_type")
     expect_equal(nb_values_db(db, q), 3)

     close(db)
})


test_that("query from plan", {
    l <- list(1L, "tu", 45.9, NA_integer_, TRUE, c(2.1, 4), NA_real_)
    db <- db_from_values(l, with_search_index=TRUE)

     expect_equal(nb_values_db(db), length(l))

     q <- query_from_plan(list("na" = TRUE))
     res <- view_db(db, q)
     expect_length(res, 2)
     expect_equal(res[[1]], NA_integer_)
     expect_equal(res[[2]], NA_real_)

     q <- query_from_plan(list("vector" = TRUE))
     expect_equal(nb_values_db(db, q), 1)
     expect_equal(sample_val(db, q), c(2.1, 4))

     q <- query_from_plan(list("length" = 2))
     expect_equal(nb_values_db(db, q), 1)
     expect_equal(sample_val(db, q), c(2.1, 4))

     close(db)

})

test_that("class names", {
     o1 <- structure(3:8, class="some_class")
     o2 <- structure("hello world", class="another_class")

     l <- list(1L, "tu", 45.9, NA_integer_, o2, TRUE, c(2.1, 4), o1)
     db <- db_from_values(l, with_search_index=TRUE)

     expect_equal(nb_values_db(db), length(l))

     q <- query_from_plan(list("classname" = "some_class"))
     res <- view_db(db, q)
     expect_length(res, 1)
     expect_equal(res[[1]], o1)

     q <- query_from_plan(list("classname" = TRUE))
     res <- view_db(db, q)
     expect_length(res, 2)
     expect_equal(res[[1]], o2)
     expect_equal(res[[2]], o1)

     close(db)
})

# TODO:
# - test class names
# - multi-dimentional values
# - with attributes
# - 0-length vector
