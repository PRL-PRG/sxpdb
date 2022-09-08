<!-- badges: start -->
[![R-CMD-check](https://github.com/PRL-PRG/sxpdb/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/PRL-PRG/sxpdb/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->


# sxpdb

Stores R values. It stores the values in an efficient ways that scales to millions of unique values and provides a rich query API to find values from the database.



```R
db <- open_db("cran_db")

add_val(db, 45)
add_val(db, list(a = 34, b = c(34.5, 45), c = "test"))

add_val_origin(db, 450, "pkg", "fun", "arg1")

print(sample_val(db))

db2 <- open_db("db2")

merge_db(db, db2)

close_db(db)
close_db(db2)

```


## S3 API

```R
db <- open_db("cran_db", quiet=FALSE)

add_val(db, 45)

db[0]

summary(db)

length(db)

close(db)
```
