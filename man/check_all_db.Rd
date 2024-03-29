% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sxpdb.R
\name{check_all_db}
\alias{check_all_db}
\title{Checks if the db is not corrupted.}
\usage{
check_all_db(db, slow = FALSE)
}
\arguments{
\item{db}{database, sxpdb object}

\item{slow}{boolean, if \code{TRUE}, it will unserialize all the values in the database and check
them all, if `FALSE, it will perform only quick consistency checks.}
}
\value{
integer vector of indices of values with problems
}
\description{
\code{check_all_db} checks if the database is not currupted and suggests some fixes if it is.
}
