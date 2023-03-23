
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
  has_search_index(db)
  db <- db_from_values(l, with_search_index = TRUE)

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
  db <- db_from_values(l, with_search_index = TRUE)

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
  db <- db_from_values(l, with_search_index = TRUE)

  expect_equal(nb_values_db(db), length(l))

  q <- query_from_plan(list(na = TRUE))
  res <- view_db(db, q)
  expect_length(res, 2)
  expect_equal(res[[1]], NA_integer_)
  expect_equal(res[[2]], NA_real_)

  q <- query_from_plan(list(vector = TRUE))
  expect_equal(nb_values_db(db, q), 1)
  expect_equal(sample_val(db, q), c(2.1, 4))

  q <- query_from_plan(list(length = 2))
  expect_equal(nb_values_db(db, q), 1)
  expect_equal(sample_val(db, q), c(2.1, 4))

  close(db)
})

test_that("class names", {
  o1 <- structure(3:8, class = "some_class")
  o2 <- structure("hello world", class = "another_class")

  l <- list(1L, "tu", 45.9, NA_integer_, o2, TRUE, c(2.1, 4), o1)
  db <- db_from_values(l, with_search_index = TRUE)

  expect_equal(nb_values_db(db), length(l))

  q <- query_from_plan(list(classname = "some_class"))
  res <- view_db(db, q)
  expect_length(res, 1)
  expect_equal(res[[1]], o1)

  q <- query_from_plan(list(classname = TRUE))
  res <- view_db(db, q)
  expect_length(res, 2)
  expect_equal(res[[1]], o2)
  expect_equal(res[[2]], o1)

  close(db)
})

test_that("0-length stuff", {
  l <- list(1L, "tu", 45.9, NA_integer_, integer(0), TRUE, c(2.1, 4), double(0))
  db <- db_from_values(l, with_search_index = TRUE)

  expect_equal(nb_values_db(db), length(l))

  q <- query_from_plan(list(length = 0))
  res <- view_db(db, q)
  expect_length(res, 2)
  expect_equal(res[[1]], integer(0))
  expect_equal(res[[2]], double(0))

  q <- query_from_plan(list(type = 1L))
  res <- view_db(db, q)
  expect_length(res, 3)
  expect_equal(res[[1]], 1L)
  expect_equal(res[[2]], NA_integer_)
  expect_equal(res[[3]], integer(0))

  close(db)
})

test_that("many different lengths", {
  db <- open_db(tempfile("sxpdb"), mode = TRUE, quiet = TRUE)
  n <- 0
  lengths <- c()
  for (i in 1:5) {
    base <- 10^i
    for (j in 1:10) {
      v <- rep.int(2, base + j)
      lengths <- c(lengths, base + j)
      add_val_origin(db, v, "pkg", "f", "arg")
    }
  }
  expect_equal(nb_values_db(db), length(lengths))
  build_indexes(db)

  for (len in lengths) {
    q <- query_from_plan(list(length = len))
    expect_equal(nb_values_db(db, q), 1)
  }

  close(db)
})

test_that("attributes", {
  v1 <- structure(3:23, my_attr = "plop")
  v2 <- structure(c("1", "one"), attr2 = -234L)
  l <- list(1L, "tu", 45.9, NA_integer_, v1, TRUE, c(2.1, 4), v2)
  db <- db_from_values(l, with_search_index = TRUE)

  expect_equal(nb_values_db(db), length(l))

  q <- query_from_plan(list(attributes = TRUE))
  res <- view_db(db, q)
  expect_length(res, 2)
  expect_equal(res[[1]], v1)
  expect_equal(res[[2]], v2)

  q <- query_from_plan(list(attributes = FALSE))
  expect_equal(nb_values_db(db, q), nb_values_db(db) - 2)

  close(db)
})

test_that("dimensions", {
  v <- c(2, 4, 3, 5)
  v1 <- array(v)
  v2 <- matrix(v, nrow = 2, ncol = 2)
  l <- list(1L, "tu", 45.9, NA_integer_, v1, TRUE, c(2.1, 4), v2)
  db <- db_from_values(l, with_search_index = TRUE)

  expect_equal(nb_values_db(db), length(l))

  q <- query_from_plan(list(ndims = 1))
  res <- view_db(db, q)
  expect_length(res, 1)
  expect_equal(res[[1]], v1)

  q <- query_from_plan(list(ndims = 2))
  res <- view_db(db, q)
  expect_length(res, 1)
  expect_equal(res[[1]], v2)


  q <- query_from_plan(list(ndims = 0))
  expect_equal(nb_values_db(db, q), nb_values_db(db) - 2)

  close(db)
})
