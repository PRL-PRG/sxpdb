if (F) {

test_that("view db", {
	open_db("test_db/a_view_db/view_db", create = TRUE)
	add_val("1")
	add_val(2L)
	add_val(3.0000)
	add_val(as.raw(4))
	add_val(c(5L, 6L))
	add_val(c(7, 8))
	add_val(c(as.raw(9), as.raw(9)))
	add_val(T)
	add_val(10+11i)
	add_val(as.list(c("twelve", "thirteen", "fourteen")))
	add_val(as.environment(-1))
	add_val(function(x) {x + 1	})
	add_val("A really really really really really long string")
	viewer <- view_db()
	expect_equal(length(viewer), 13)
	close_db()
})

}
