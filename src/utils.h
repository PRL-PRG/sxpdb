#ifndef SXPDB_UTILS_H
#define SXPDB_UTILS_H

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include <vector>
#include <algorithm>
#include <cassert>

#include "roaring.hh"

inline const SEXP create_data_frame(
  const std::vector<std::pair<std::string, SEXP>>& columns) {
  int column_count = columns.size();
  int row_count = column_count == 0 ? 0 : Rf_length(columns[0].second);

  SEXP r_data_frame = PROTECT(Rf_allocVector(VECSXP, column_count));
  SEXP r_column_names = PROTECT(Rf_allocVector(STRSXP, column_count));


  for (int column_index = 0; column_index < column_count; ++column_index) {
    SET_STRING_ELT(r_column_names,
                   column_index,
                   Rf_mkChar(columns[column_index].first.c_str()));
    SET_VECTOR_ELT(
      r_data_frame, column_index, columns[column_index].second);
  }

  // the data frame needs to have row names but we can put a special value for this attribute
  // see .set_row_names
  SEXP r_row_names = PROTECT(Rf_allocVector(INTSXP, 2));
  INTEGER(r_row_names)[0] = NA_INTEGER;
  INTEGER(r_row_names)[1] = -row_count;

  Rf_setAttrib(r_data_frame, R_NamesSymbol, r_column_names);
  Rf_setAttrib(r_data_frame, R_RowNamesSymbol, r_row_names);
  Rf_setAttrib(r_data_frame, R_ClassSymbol, Rf_mkString("data.frame"));

  UNPROTECT(3);

  return r_data_frame;
}

// because Rf_getAttrib is naughty and
//tries to play smart when there is the special row names and returns an empty INTSXP...
inline SEXP get_attrib(SEXP vec, SEXP name) {
  for (SEXP s = ATTRIB(vec); s != R_NilValue; s = CDR(s))
    if (TAG(s) == name) {
      if (name == R_DimNamesSymbol && TYPEOF(CAR(s)) == LISTSXP)
        Rf_error("old list is no longer allowed for dimnames attribute");
      MARK_NOT_MUTABLE(CAR(s));
      return CAR(s);
    }
  return R_NilValue;
}

// For a dataframe without row names
inline R_xlen_t get_nb_rows(SEXP df) {
  return -INTEGER(get_attrib(df, R_RowNamesSymbol))[1];
}

// Bind the rows of a list of data frames, which we assume to be compatible
// We assume they do not have "real" row names
inline const SEXP bind_rows(const SEXP df_list) {

  R_xlen_t total_rows = 0;
  // Find out what is the dataframe with the largest number of rows
  for(R_xlen_t i =0; i < Rf_xlength(df_list); i++ ) {
    SEXP df = VECTOR_ELT(df_list, i);
    int nb_rows = get_nb_rows(df);
    assert(nb_rows >= 0);
    total_rows += nb_rows;
  }


  SEXP df1 = VECTOR_ELT(df_list, 0);
  std::vector<std::pair<std::string, SEXP>> columns(Rf_xlength(df1));

  SEXP r_names = Rf_getAttrib(df1, R_NamesSymbol);

  // Allocate space for each column and put the name
  R_xlen_t i = 0;
  for(auto& column : columns) {
    column.first = CHAR(STRING_ELT(r_names, i));
    SEXPTYPE stype = TYPEOF(VECTOR_ELT(df1, i));
    column.second = PROTECT(Rf_allocVector(stype, total_rows));
    // Copy everything, data frame per data frame
    R_xlen_t nb_rows=0;
    for(R_xlen_t j = 0; j < Rf_xlength(df_list) ; j++) {
      // current data frame
      SEXP df = VECTOR_ELT(df_list, j);

      SEXP cur_column = VECTOR_ELT(df, i);
      assert(TYPEOF(cur_column) == stype);
      if(stype == INTSXP) {
        std::copy_n(INTEGER(cur_column), Rf_xlength(cur_column), INTEGER(column.second) + nb_rows);
      }
      else { //TODO: handle other types than STR
        for(R_xlen_t k = 0; k < Rf_xlength(cur_column); k++) {
          SET_STRING_ELT(column.second, nb_rows + k, STRING_ELT(cur_column, k));
        }
      }

      nb_rows += Rf_xlength(cur_column);
    }

    i++;
  }

  SEXP res = PROTECT(create_data_frame(columns));

  UNPROTECT(i + 1);

  return res;
}

// minimum will return uint64_t max value if teh set is empty, which we do not want
inline uint64_t safe_minimum(const roaring::Roaring64Map& set) {
  return set.isEmpty() ? 0 : set.minimum();
}

#endif
