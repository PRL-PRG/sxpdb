#ifndef SXPDB_DESCRIPTION_H
#define SXPDB_DESCRIPTION_H


#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include "Rversion.h"

#include <optional>
#include <vector>
#include <cstdint>
#include <string>

#define UNIONTYPE 40

/**
 * Description of a value (aka type...)
 *
 */

template <typename T>
bool na_in(SEXP value, T check_na) {
  int length = Rf_length(value);

  for (int i = 0; i < length; ++i) {
    if (check_na(value, i)) {
      return true;
    }
  }
  return false;
}

inline bool find_na(SEXP val) {
  switch(TYPEOF(val)) {
  case STRSXP:
    return na_in(val, [](SEXP vector, int index) -> bool {
      return STRING_ELT(vector, index) == NA_STRING;
    });
  case CPLXSXP:
    return na_in(val, [](SEXP vector, int index) -> bool {
      Rcomplex v = COMPLEX_ELT(vector, index);
      return (ISNAN(v.r) || ISNAN(v.i));
    });
  case REALSXP:
    return na_in(val, [](SEXP vector, int index) -> bool {
      return ISNAN(REAL_ELT(vector, index));
    });
  case LGLSXP:
    return  na_in(val, [](SEXP vector, int index) -> bool {
      return LOGICAL_ELT(vector, index) == NA_LOGICAL;
    });
  case INTSXP:
    return na_in(val, [](SEXP vector, int index) -> bool {
      return INTEGER_ELT(vector, index) == NA_INTEGER;
    });
  }

  return false;
}

class Description {
public:
  SEXPTYPE type = ANYSXP;
  std::optional<bool> is_vector;
  std::optional<bool> has_na;
  std::optional<bool> has_attributes;
  std::optional<bool> has_class;
  std::optional<uint64_t> length;
  std::optional<int> ndims; // 2 = matrix, otherwise = array
  std::vector<std::string> class_names;
  std::vector<Description> descriptions;// For union types, lists...
public:
  Description() {}
  Description(SEXPTYPE type_) : type(type_) {}

  void relax_na() {has_na.reset();}
  void relax_vector() {is_vector.reset();}
  void relax_length() {length.reset();}
  void relax_attributes() {has_attributes.reset(); }
  void relax_ndims() {ndims.reset(); }
  void relax_class() {has_class.reset(); class_names.clear();}
  void relax_type() {type = ANYSXP; }

  // returns the closest description of the SEXP
  // We may relax it later on
  inline static const Description description_from_value(SEXP val) {
    Description d(TYPEOF(val));

    d.length = Rf_length(val);

    d.is_vector = *d.length != 1 && d.type != ENVSXP && d.type != LISTSXP;

    d.has_attributes = Rf_length(ATTRIB(val)) > 0;

    SEXP klass = Rf_getAttrib(val, R_ClassSymbol);
    d.has_class = klass != R_NilValue;

    if(d.has_class && TYPEOF(klass) == STRSXP) {
      for (int index = 0; index < Rf_length(klass); index++) {
        d.class_names.push_back(CHAR(STRING_ELT(klass, index)));
      }
    }

    SEXP dim = Rf_getAttrib(val, R_DimSymbol);
    d.ndims = Rf_length(dim);

    //NA
    d.has_na = find_na(val);

    if(d.type == VECSXP) {
      for(int index = 0; index < d.length ; index++) {
        d.descriptions.push_back(description_from_value(VECTOR_ELT(val, index)));
      }
    }

    return d;
  }

  inline static const Description union_description(const Description& d1, const Description& d2) {
    Description d(UNIONTYPE);

    // unify if it is the same for both, otherwise, make non defined
    if(d1.is_vector && d2.is_vector && *d1.is_vector == *d2.is_vector) {
      d.is_vector = d1.is_vector;
    }
    if(d1.has_na && d2.has_na && *d1.has_na == *d2.has_na) {
      d.has_na = d1.has_na;
    }
    if(d1.has_attributes && d2.has_attributes && *d1.has_attributes == *d2.has_attributes) {
      d.has_attributes = d1.has_attributes;
    }
    if(d1.has_class && d2.has_class && *d1.has_class == *d2.has_class) {
      d.has_class = d1.has_class;
      //we do not unify here the class names
    }
    if(d1.length && d2.length && *d1.length == *d2.length) {
      d.length = d1.length;
    }
    if(d1.ndims && d2.ndims && *d1.ndims == *d1.ndims) {
      d.ndims = d1.ndims;
    }

    d.descriptions = {d1, d2}; // does it work even if d1 or d2 is already an union?

    return d;
  }
};


#endif
