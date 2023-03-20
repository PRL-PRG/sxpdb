with_db <- function(db, code) {
    # execute the code 

    # close the db
}

# Create a db from a vector of values, and optionnally origins
db_from_values <- function(values, origins=NULL, name="sxpdb", with_search_index=FALSE) {
    # We create a name in the session temp directory for the db.
    # It will actually create a directory with this name there
    db <- open_db(tempfile(name), mode=TRUE, quiet=TRUE)

    if(is.null(origins)) {
        for(v in values) {
            add_val_origin(db, v, "pkg", "f", "arg")
        }
    }
    else {
        stopifnot(length(values) == length(origins))
        for(i in seq_along(values)) {
            add_val_origin(db, values[[i]], origins[[i]][[1]], origins[[i]][[2]], origins[[i]][[3]])
        }
    }
    build_indexes(db)
    db
}