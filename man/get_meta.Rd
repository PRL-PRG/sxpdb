% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sxpdb.R
\name{get_meta}
\alias{get_meta}
\title{Get the metadata associated to a value}
\usage{
get_meta(db, val)
}
\arguments{
\item{db}{database, sxpdb object}

\item{val}{a R value}
}
\value{
a named list with the metadata, or \code{NULL} if there is no such value in the database
}
\description{
\code{get_meta} returns the metadata associated to a value. It includes:
* \emph{runtime} metadata: number of calls the value has been seen in, number of times the value has been seen during merges
* \emph{static} metadata: type, size in bytes, length (for vector values), number of attributes, number of dimensionsm number of rows (for data frames, matrixes)
* \emph{debug} metadata: only if the database was created with sxpdb in debug mode, includes how many times \code{MAYBE_SHARED} has been true on the value,
and how many times we were able to use the SEXP address optimization
}
\seealso{
\code{\link[=get_meta_idx]{get_meta_idx()}}
}
