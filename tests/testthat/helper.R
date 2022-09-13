with_db <- function(db, code) {
    # execute the code 

    # close the db
}

# Create a db from a vector of values, and optionnally origins
db_from_values <- function(values, origins=NULL, name="sxpdb") {
    # We create a name in the session temp directory for the db.
    # It will actually create a directory with this name there
    db <- open_db(tempfile(name), mode=TRUE)

    for(v in values) {
        add_val_origin(db, v, "pkg", "f", "arg")
    }
    db
}