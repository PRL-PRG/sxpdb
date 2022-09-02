test_that("open db in read mode", {
    # TODO: write a with_db function and one that create a custom db programmatically
    # ( so no need to create a new version each time.), for instance given a list of values
    # and optionally origins, call ids and so on

    db <- db_from_values(c(1L, "tu", 45.9))
    path <- path_db(db)
    close(db)

    db <- open_db(path)
    expect_equal(class(db), "sxpdb")
    close(db)
})