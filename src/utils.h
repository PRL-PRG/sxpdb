#ifndef SXPDB_UTILS_H
#define SXPDB_UTILS_H

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include <vector>
#include <cassert>

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

// Bind the rows of a list of data frames, which we assume to be compatible 
// We assume they do not have "real" row names
inline const SEXP bind_rows(const SEXP df_list) {
  
  R_xlen_t total_rows = 0;
  // Find out what is the dataframe with the largest number of rows
  for(R_xlen_t i =0; i < Rf_xlength(df_list); i++ ) {
    SEXP df = VECTOR_ELT(df_list, i);
    int nb_rows = -INTEGER(Rf_getAttrib(df, R_RowNamesSymbol))[1];
    assert(nb_rows > 0);
    total_rows += nb_rows;
  }
  
  
  SEXP df1 = VECTOR_ELT(df_list, 0);
  std::vector<std::pair<std::string, SEXP>> columns(Rf_xlength(df1));
  
  SEXP r_names = Rf_getAttrib(df1, R_NamesSymbol);
  
  // Allocate space for each column and put the name
  R_xlen_t i = 0;
  for(auto& column : columns) {
    column.first = CHAR(VECTOR_ELT(r_names, i));
    SEXPTYPE stype = TYPEOF(VECTOR_ELT(df1, i));
    column.second = PROTECT(Rf_allocVector(stype, total_rows));
    // Copy everything
    R_xlen_t nb_rows=0;
    for(R_xlen_t j; j < Rf_xlength(df_list) ; j++) {
      SEXP df = VECTOR_ELT(df_list, j);
      for(R_xlen_t k; k < Rf_xlength(df); k++) {
        SET_VECTOR_ELT(column.second, nb_rows + k, VECTOR_ELT(df, k));
      }
      nb_rows += Rf_xlength(df);
    }
    
    i++;
  }
  
  SEXP res = PROTECT(create_data_frame(columns));
  
  UNPROTECT(i + 1);
  
  return res;
}

#endif