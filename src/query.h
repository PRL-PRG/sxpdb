#ifndef SXPDB_QUERY_H
#define SXPDB_QUERY_H


#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include "Rversion.h"

#include <optional>
#include <vector>
#include <cstdint>
#include <string>
#include <algorithm>
#include <execution>
#include <random>

#include "roaring.hh"

#include "utils.h"
#include "search_index.h"
#include "database.h"

inline const SEXPTYPE UNIONTYPE = 40;


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
  case CPLXSXP: {
      Rcomplex* v = COMPLEX(val);
      int length = Rf_length(val);
      return std::find_if(std::execution::par_unseq, v, v + length, [](const Rcomplex& c) -> bool {return ISNAN(c.r) || ISNAN(c.i);}) != v + length;
    }
  case REALSXP: {
      double* v = REAL(val);
      int length = Rf_length(val);
      return std::find_if(std::execution::par_unseq, v, v + length, [](double d) -> bool {return ISNAN(d) ;}) != v + length;
    }
  case LGLSXP:{
    int* v = LOGICAL(val);
    int length = Rf_length(val);

    return std::find(std::execution::par_unseq, v, v + length, NA_LOGICAL) != v + length;
  }
  case INTSXP: {
    int* v = INTEGER(val);
    int length = Rf_length(val);

    return std::find(std::execution::par_unseq, v, v + length, NA_INTEGER) != v + length;
    }
  }
  return false;
}

class Query {
private:
  bool init = false;
  bool quiet = false;
  roaring::Roaring64Map index_cache;
  std::uniform_int_distribution<uint64_t> dist_cache;
public:
  SEXPTYPE type = ANYSXP;
  std::optional<bool> is_vector;
  std::optional<bool> has_na;
  std::optional<bool> has_attributes;
  std::optional<bool> has_class;
  std::optional<uint64_t> length;
  std::optional<int> ndims; // 2 = matrix, otherwise = array
  std::vector<std::string> class_names;
  std::vector<Query> queries;// For union types, lists...
public:
  Query(bool quiet_ = true) : quiet(quiet_) {}
  Query(SEXPTYPE type_, bool quiet_ = true) : type(type_), quiet(quiet_) {}

  bool update(const SearchIndex& search_index) {
    if(!search_index.is_initialized()) {
        Rf_error("Please build the search indexes to be able to sample with a complex query.\n");
    }

    if(!init&& !quiet) {
      Rprintf("Building the search index for the query.\n");
    }
    else if(init && search_index.new_elements && !quiet) {
      Rprintf("Updating the search index for the query.\n");
    }
    if(type != UNIONTYPE) {
      index_cache |= search_index.types_index[type];
    }
    else if(type == UNIONTYPE) {
      for(auto& desc : queries) {
        assert(desc.type != UNIONTYPE);
        index_cache |= search_index.types_index[desc.type];
      }
    }

    if(has_class && has_class.value()) {
      index_cache &= search_index.class_index;
    }
    else if(has_class && !has_class.value()) {
      roaring::Roaring64Map nonclass = search_index.class_index;

      // range end excluded
      nonclass.flip(safe_minimum(index_cache), index_cache.maximum() + 1);

      index_cache &= nonclass;
    }

    if(has_attributes && has_attributes.value()) {
      index_cache &= search_index.attributes_index;
    }
    else if(has_attributes && !has_attributes.value()) {
      roaring::Roaring64Map nonattributes = search_index.attributes_index;
      nonattributes.flip(safe_minimum(index_cache), index_cache.maximum() + 1);
      index_cache &= nonattributes;
    }
    //TODO: union
    /*else if(d.type == UNIONTYPE) {
     roaring::Roaring64Map attrindex;

     for(auto& desc : d.queries) {

     }
    }
     */

    if(is_vector && is_vector.value()) {
      index_cache &= search_index.vector_index;
    }
    else if(is_vector && !is_vector.value()) {
      roaring::Roaring64Map nonvector = search_index.vector_index;
      nonvector.flip(safe_minimum(index_cache), index_cache.maximum() + 1);
      index_cache &= nonvector;
    }

    if(has_na && has_na.value()) {
      index_cache &= search_index.na_index;
    }
    else if(has_na && !has_na.value()) {
      roaring::Roaring64Map nonna = search_index.na_index;
      nonna.flip(safe_minimum(index_cache), index_cache.maximum() + 1);
      index_cache &= nonna;
    }

    if(length) {
      auto low_bound = std::lower_bound(search_index.length_intervals.begin(), search_index.length_intervals.end(), length.value());
      if(low_bound == search_index.length_intervals.end()) {
        return R_NilValue;// should never happen
      }
      int length_idx = std::distance(SearchIndex::length_intervals.begin(), low_bound);
      // Check if the index_cache in tat slot represents one length or several ones
      // Either it is the last slot or the length difference is > 1
      if( length_idx == SearchIndex::nb_intervals - 1 || SearchIndex::length_intervals.at(length_idx + 1) - SearchIndex::length_intervals.at(length_idx) > 1) {
        // we manually build an index_cache for the given length
        // it will perform a linear search but there should not be more than 10^2 elements in those slots anyway
        roaring::Roaring64Map precise_length_index = search_index.search_length(search_index.lengths_index[length_idx], length.value());
        index_cache &= precise_length_index;
      }
      else {
        index_cache &= search_index.lengths_index[length_idx];
      }
    }

  }

  std::optional<uint64_t> sample(std::default_random_engine& rand_engine) {
    if(index_cache.cardinality() == 0) {
      return {};
    }

    if(dist_cache.max() == 0) {
      std::uniform_int_distribution<uint64_t> dist(0, index_cache.cardinality() - 1);
      dist_cache.param(dist.param());
    }

    uint64_t element;

    bool res = index_cache.select(dist_cache(rand_engine), &element);

    return res ? std::optional<uint64_t>(element) : std::nullopt;
  }

  // Sample without replacement: uses selection sampling or reservoir sampling
  // it will generate min(n, index_cache.cardinality())
  const std::vector<uint64_t> sample_n(uint64_t n, std::default_random_engine& rand_engine) {
    if(index_cache.cardinality() == 0) {
      return std::vector<uint64_t>();
    }

    uint64_t nb_samples = std::min(n, index_cache.cardinality());

    std::vector<uint64_t> samples;
    samples.reserve(nb_samples);
    std::sample(index_cache.begin(), index_cache.end(), std::back_inserter(samples), n, rand_engine);

    return samples;
  }

  const roaring::Roaring64Map& view() const {return index_cache; }//TODO
  bool is_initialized() const {return init;}

  void relax_na() {has_na.reset();}
  void relax_vector() {is_vector.reset();}
  void relax_length() {length.reset();}
  void relax_attributes() {has_attributes.reset(); }
  void relax_ndims() {ndims.reset(); }
  void relax_class() {has_class.reset(); class_names.clear();}
  void relax_type() {type = ANYSXP; }

  // returns the closest description of the SEXP
  // We may relax it later on
  inline static const Query from_value(SEXP val, bool quiet = true) {
    Query d(TYPEOF(val), quiet = quiet);

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
        d.queries.push_back(from_value(VECTOR_ELT(val, index)));
      }
    }

    return d;
  }

  inline static const Query unify(const Query& d1, const Query& d2) {
    Query d(UNIONTYPE);

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

    // We do not deduplicate descriptions here
    if(d1.type == UNIONTYPE) {
      d.queries.insert(d.queries.end(), d1.queries.begin(), d1.queries.end());
    }
    else {
      d.queries.push_back(d1);
    }
    if(d2.type == UNIONTYPE) {
      d.queries.insert(d.queries.end(), d2.queries.begin(), d2.queries.end());
    }
    else {
      d.queries.push_back(d2);
    }


    return d;
  }
};


#endif
