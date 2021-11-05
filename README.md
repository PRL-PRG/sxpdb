# sxpdb

Stores R values. It stores the values in an efficient ways that scales to millions of unique values and provides a rich query API to find values from the database.



```R
db <- open_db("cran_db")

add_val(db, 45)
add_val(db, list(a = 34, b = c(34.5, 45), c = "test"))

print(sample_val(db))

db2 <- open_db("db2")

merge_db(db, db2)

close_db(db)
close_db(db2)

```
