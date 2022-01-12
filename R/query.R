#' @export
#' @param basic_type one of the basic R types ("real", "integer", including "any")
#' @param is_vector vector or scalar
#' @param has_na can there be NA in the vector (does not take into account lists or environments)
#' @param has_attr does it have attributes or not
#' @param length
#' @param ndims number of dimensions
#' @param class_names a character vector of class names
#' @param subtypes a list of queries for the components of a list
#' if one of the refining parameter is `NA`, it means that there are no constraints on it, and the query will consider
#' all possible values for it
#' @return external pointer, a query description
build_query <- function(basic_type = "any", is_vector = NA, has_na=NA, has_attr=NA,
                        has_class = NA, length = NA_integer_, ndims= NA_integer_,
                        class_names=NA_character_, subtypes=NA) {

}

#' @export
#' Build a query from an actual R value using the most restrictive constraints possible.
#' You can then relax the query thanks to `modify_query` with some `NA` arguments.
#' @examples
#' v <- runif(1:4)
#' query <- build_query_from_value(v)
#' modify_query(query, length=NA) # will look for real vectors (not scalars), whose size is not specified.
build_query_from_value <- function(v) {

}

#' @export
#' Modifies the components of the query. `NA` to relax a constraint on a parameter, `"any"` to allow any types.
#' @return external pointer, q query description
modify_query <- function(query, basic_type = "any", is_vector = NA, has_na=NA, has_attr=NA,
                         has_class = NA, length = NA_integer_, ndims= NA_integer_,
                         class_names=NA_character_, subtypes=NA) {

}

#' @export
#' Perform the union of a number of queries
#' @return external pointer, a query description
unify_queries <- function(...) {

}
