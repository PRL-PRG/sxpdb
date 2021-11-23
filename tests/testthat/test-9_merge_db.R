if (F) {

## Integer Section

test_that("merge empty simple ints", {
	open_db("test_db/9_merge_db/s_ints1", create = T)
	close_db()

	open_db("test_db/9_merge_db/s_ints2", create = T)
	add_val(3L)
	add_val(4L)
	expect_true(size_db() == 2)
	merge_db("test_db/9_merge_db/s_ints1")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge simple ints", {
	open_db("test_db/9_merge_db/s_ints1")
	add_val(1L)
	add_val(2L)
	expect_true(size_db() == 2)
	close_db()

	open_db("test_db/9_merge_db/s_ints2")
	merge_db("test_db/9_merge_db/s_ints1")
	expect_true(size_db() == 4)
	close_db()
})

test_that("merge simple ints reverse", {
	open_db("test_db/9_merge_db/s_ints1")
	expect_true(size_db() == 2)
	merge_db("test_db/9_merge_db/s_ints2")
	expect_true(size_db() == 4)
	close_db()
})

test_that("merge empty ints", {
	open_db("test_db/9_merge_db/ints1", create = T)
	close_db()

	open_db("test_db/9_merge_db/ints2", create = T)
	add_val(30000L)
	add_val(40000L)
	expect_true(size_db() == 2)
	merge_db("test_db/9_merge_db/ints1")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge ints", {
	open_db("test_db/9_merge_db/ints1")
	add_val(10000L)
	add_val(20000L)
	expect_true(size_db() == 2)
	close_db()

	open_db("test_db/9_merge_db/ints2")
	merge_db("test_db/9_merge_db/ints1")
	expect_true(size_db() == 4)
	close_db()
})

test_that("merge ints reverse", {
	open_db("test_db/9_merge_db/ints1")
	expect_true(size_db() == 2)
	merge_db("test_db/9_merge_db/ints2")
	expect_true(size_db() == 4)
	close_db()
})

## Double Section

test_that("merge empty simple dbls", {
	open_db("test_db/9_merge_db/s_dbls1", create = T)
	close_db()

	open_db("test_db/9_merge_db/s_dbls2", create = T)
	add_val(3)
	add_val(4)
	expect_true(size_db() == 2)
	merge_db("test_db/9_merge_db/s_dbls1")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge simple dbls", {
	open_db("test_db/9_merge_db/s_dbls1")
	add_val(1)
	add_val(2)
	expect_true(size_db() == 2)
	close_db()

	open_db("test_db/9_merge_db/s_dbls2")
	merge_db("test_db/9_merge_db/s_dbls1")
	expect_true(size_db() == 4)
	close_db()
})

test_that("merge dbls simple reverse", {
	open_db("test_db/9_merge_db/s_dbls1")
	expect_true(size_db() == 2)
	merge_db("test_db/9_merge_db/s_dbls2")
	expect_true(size_db() == 4)
	close_db()
})

test_that("merge empty dbls", {
	open_db("test_db/9_merge_db/dbls1", create = T)
	close_db()

	open_db("test_db/9_merge_db/dbls2", create = T)
	add_val(30000)
	add_val(40000)
	expect_true(size_db() == 2)
	merge_db("test_db/9_merge_db/dbls1")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge dbls", {
	open_db("test_db/9_merge_db/dbls1")
	add_val(10000)
	add_val(20000)
	expect_true(size_db() == 2)
	close_db()

	open_db("test_db/9_merge_db/dbls2")
	merge_db("test_db/9_merge_db/dbls1")
	expect_true(size_db() == 4)
	close_db()
})

test_that("merge dbls reverse", {
	open_db("test_db/9_merge_db/dbls1")
	expect_true(size_db() == 2)
	merge_db("test_db/9_merge_db/dbls2")
	expect_true(size_db() == 4)
	close_db()
})

## Raw Section

test_that("merge empty simple raws", {
	open_db("test_db/9_merge_db/s_raws1", create = T)
	close_db()

	open_db("test_db/9_merge_db/s_raws2", create = T)
	add_val(as.raw(3))
	add_val(as.raw(4))
	expect_true(size_db() == 2)
	merge_db("test_db/9_merge_db/s_raws1")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge simple raws", {
	open_db("test_db/9_merge_db/s_raws1")
	add_val(as.raw(1))
	add_val(as.raw(2))
	expect_true(size_db() == 2)
	close_db()

	open_db("test_db/9_merge_db/s_raws2")
	merge_db("test_db/9_merge_db/s_raws1")
	expect_true(size_db() == 4)
	close_db()
})

test_that("merge raws simple reverse", {
	open_db("test_db/9_merge_db/s_raws1")
	expect_true(size_db() == 2)
	merge_db("test_db/9_merge_db/s_raws2")
	expect_true(size_db() == 4)
	close_db()
})

test_that("merge empty raws", {
	open_db("test_db/9_merge_db/raws1", create = T)
	close_db()

	open_db("test_db/9_merge_db/raws2", create = T)
	add_val(c(as.raw(3), as.raw(3), as.raw(3)))
	add_val(c(as.raw(4), as.raw(4), as.raw(4)))
	expect_true(size_db() == 2)
	merge_db("test_db/9_merge_db/raws1")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge raws", {
	open_db("test_db/9_merge_db/raws1")
	add_val(c(as.raw(1), as.raw(1), as.raw(1)))
	add_val(c(as.raw(2), as.raw(2), as.raw(2)))
	expect_true(size_db() == 2)
	close_db()

	open_db("test_db/9_merge_db/raws2")
	merge_db("test_db/9_merge_db/raws1")
	expect_true(size_db() == 4)
	close_db()
})

test_that("merge raws reverse", {
	open_db("test_db/9_merge_db/raws1")
	expect_true(size_db() == 2)
	merge_db("test_db/9_merge_db/raws2")
	expect_true(size_db() == 4)
	close_db()
})

## String Section

test_that("merge empty simple strs", {
	open_db("test_db/9_merge_db/s_strs1", create = T)
	close_db()

	open_db("test_db/9_merge_db/s_strs2", create = T)
	add_val("3")
	add_val("4")
	expect_true(size_db() == 2)
	merge_db("test_db/9_merge_db/s_strs1")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge simple strs", {
	open_db("test_db/9_merge_db/s_strs1")
	add_val("1")
	add_val("2")
	expect_true(size_db() == 2)
	close_db()

	open_db("test_db/9_merge_db/s_strs2")
	merge_db("test_db/9_merge_db/s_strs1")
	expect_true(size_db() == 4)
	close_db()
})

test_that("merge simple strs reverse", {
	open_db("test_db/9_merge_db/s_strs1")
	expect_true(size_db() == 2)
	merge_db("test_db/9_merge_db/s_strs2")
	expect_true(size_db() == 4)
	close_db()
})

test_that("merge empty strs", {
	open_db("test_db/9_merge_db/strs1", create = T)
	close_db()

	open_db("test_db/9_merge_db/strs2", create = T)
	add_val("300000000")
	add_val("400000000")
	expect_true(size_db() == 2)
	merge_db("test_db/9_merge_db/strs1")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge strs", {
	open_db("test_db/9_merge_db/strs1")
	add_val("100000000")
	add_val("200000000")
	expect_true(size_db() == 2)
	close_db()

	open_db("test_db/9_merge_db/strs2")
	merge_db("test_db/9_merge_db/strs1")
	expect_true(size_db() == 4)
	close_db()
})

test_that("merge strs reverse", {
	open_db("test_db/9_merge_db/strs1")
	expect_true(size_db() == 2)
	merge_db("test_db/9_merge_db/strs2")
	expect_true(size_db() == 4)
	close_db()
})

## Logical Section

test_that("merge empty simple logs", {
	open_db("test_db/9_merge_db/s_logs1", create = T)
	close_db()

	open_db("test_db/9_merge_db/s_logs2", create = T)
	add_val(T)
	expect_true(size_db() == 1)
	merge_db("test_db/9_merge_db/s_logs1")
	expect_true(size_db() == 1)
	close_db()
})

test_that("merge simple logs", {
	open_db("test_db/9_merge_db/s_logs1")
	add_val(F)
	expect_true(size_db() == 1)
	close_db()

	open_db("test_db/9_merge_db/s_logs2")
	merge_db("test_db/9_merge_db/s_logs1")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge simple logs reverse", {
	open_db("test_db/9_merge_db/s_logs1")
	expect_true(size_db() == 1)
	merge_db("test_db/9_merge_db/s_logs2")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge empty logs", {
	open_db("test_db/9_merge_db/logs1", create = T)
	close_db()

	open_db("test_db/9_merge_db/logs2", create = T)
	add_val(c(T, F))
	expect_true(size_db() == 1)
	merge_db("test_db/9_merge_db/logs1")
	expect_true(size_db() == 1)
	close_db()
})

test_that("merge logs", {
	open_db("test_db/9_merge_db/logs1")
	add_val(c(F, T))
	expect_true(size_db() == 1)
	close_db()

	open_db("test_db/9_merge_db/logs2")
	merge_db("test_db/9_merge_db/logs1")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge logs reverse", {
	open_db("test_db/9_merge_db/logs1")
	expect_true(size_db() == 1)
	merge_db("test_db/9_merge_db/logs2")
	expect_true(size_db() == 2)
	close_db()
})

## Complex Section

test_that("merge empty simple cmps", {
	open_db("test_db/9_merge_db/s_cmps1", create = T)
	close_db()

	open_db("test_db/9_merge_db/s_cmps2", create = T)
	add_val(1+1i)
	expect_true(size_db() == 1)
	merge_db("test_db/9_merge_db/s_cmps1")
	expect_true(size_db() == 1)
	close_db()
})

test_that("merge simple cmps", {
	open_db("test_db/9_merge_db/s_cmps1")
	add_val(2+2i)
	expect_true(size_db() == 1)
	close_db()

	open_db("test_db/9_merge_db/s_cmps2")
	merge_db("test_db/9_merge_db/s_cmps1")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge simple cmps reverse", {
	open_db("test_db/9_merge_db/s_cmps1")
	expect_true(size_db() == 1)
	merge_db("test_db/9_merge_db/s_cmps2")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge empty cmps", {
	open_db("test_db/9_merge_db/cmps1", create = T)
	close_db()

	open_db("test_db/9_merge_db/cmps2", create = T)
	add_val(c(1+1i, 2+2i))
	expect_true(size_db() == 1)
	merge_db("test_db/9_merge_db/cmps1")
	expect_true(size_db() == 1)
	close_db()
})

test_that("merge cmps", {
	open_db("test_db/9_merge_db/cmps1")
	add_val(c(2+2i, 1+1i))
	expect_true(size_db() == 1)
	close_db()

	open_db("test_db/9_merge_db/cmps2")
	merge_db("test_db/9_merge_db/cmps1")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge cmps reverse", {
	open_db("test_db/9_merge_db/cmps1")
	expect_true(size_db() == 1)
	merge_db("test_db/9_merge_db/cmps2")
	expect_true(size_db() == 2)
	close_db()
})

## List Section

test_that("merge empty lsts", {
	open_db("test_db/9_merge_db/lsts1", create = T)
	close_db()

	open_db("test_db/9_merge_db/lsts2", create = T)
	add_val(list(1,2,3))
	expect_true(size_db() == 1)
	merge_db("test_db/9_merge_db/lsts1")
	expect_true(size_db() == 1)

	close_db()
})

test_that("merge lsts", {
	open_db("test_db/9_merge_db/lsts1")
	add_val(list(2,4,6))
	expect_true(size_db() == 1)
	close_db()

	open_db("test_db/9_merge_db/lsts2")
	merge_db("test_db/9_merge_db/lsts1")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge lsts reverse", {
	open_db("test_db/9_merge_db/lsts1")
	expect_true(size_db() == 1)
	merge_db("test_db/9_merge_db/lsts2")
	expect_true(size_db() == 2)
	close_db()
})

## Environment Section

test_that("merge empty envs", {
	open_db("test_db/9_merge_db/envs1", create = T)
	close_db()

	open_db("test_db/9_merge_db/envs2", create = T)
	add_val(as.environment(-1))
	expect_true(size_db() == 1)
	merge_db("test_db/9_merge_db/envs1")
	expect_true(size_db() == 1)

	close_db()
})

test_that("merge envs", {
	open_db("test_db/9_merge_db/envs1")
	add_val(as.environment(1))
	expect_true(size_db() == 1)
	close_db()

	open_db("test_db/9_merge_db/envs2")
	merge_db("test_db/9_merge_db/envs1")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge envs reverse", {
	open_db("test_db/9_merge_db/envs1")
	expect_true(size_db() == 1)
	merge_db("test_db/9_merge_db/envs2")
	expect_true(size_db() == 2)
	close_db()
})

## Function Section

test_that("merge empty funs", {
	open_db("test_db/9_merge_db/funs1", create = T)
	close_db()

	open_db("test_db/9_merge_db/funs2", create = T)
	add_val(function(x) {1})
	expect_true(size_db() == 1)
	merge_db("test_db/9_merge_db/funs1")
	expect_true(size_db() == 1)

	close_db()
})

test_that("merge funs", {
	open_db("test_db/9_merge_db/funs1")
	add_val(function(x) {2})
	expect_true(size_db() == 1)
	close_db()

	open_db("test_db/9_merge_db/funs2")
	merge_db("test_db/9_merge_db/funs1")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge funs reverse", {
	open_db("test_db/9_merge_db/funs1")
	expect_true(size_db() == 1)
	merge_db("test_db/9_merge_db/funs2")
	expect_true(size_db() == 2)
	close_db()
})

## Generics Section

test_that("merge empty generics", {
	open_db("test_db/9_merge_db/generics1", create = T)
	close_db()

	open_db("test_db/9_merge_db/generics2", create = T)
	add_val(function() { 1 })
	expect_true(size_db() == 1)
	merge_db("test_db/9_merge_db/generics1")
	expect_true(size_db() == 1)
	close_db()
})

test_that("merge generics", {
	open_db("test_db/9_merge_db/generics1")
	add_val(function() { 2 })
	expect_true(size_db() == 1)
	close_db()

	open_db("test_db/9_merge_db/generics2")
	merge_db("test_db/9_merge_db/generics1")
	expect_true(size_db() == 2)
	close_db()
})

test_that("merge generics reverse", {
	open_db("test_db/9_merge_db/generics1")
	expect_true(size_db() == 1)
	merge_db("test_db/9_merge_db/generics2")
	expect_true(size_db() == 2)
	close_db()
})

}
