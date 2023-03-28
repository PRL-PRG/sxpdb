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
#include <random>

#ifdef SXPDB_PARALLEL_STD
#include <execution>
#endif

#include "roaring++.h"

#include "utils.h"
#include "search_index.h"
#include "database.h"

inline const SEXPTYPE UNIONTYPE = 40;

// The compiler did not manage to infer iterator_traits by itself
template<>
struct std::iterator_traits<roaring::Roaring64MapSetBitForwardIterator> {
  typedef std::forward_iterator_tag iterator_category;
  typedef uint64_t *pointer;
  typedef uint64_t &reference_type;
  typedef uint64_t value_type;
  typedef int64_t difference_type;
  typedef roaring::Roaring64MapSetBitForwardIterator type_of_iterator;
};

/**
 * Description of a value (aka type...)
 *
 */


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
  std::optional<int> ndims; // 2 = matrix, 0 = nothing, otherwise = array
  std::vector<std::string> class_names;
  std::vector<std::string> packages;
  std::vector<std::string> functions;
  std::vector<Query> queries;// For union types, lists...
public:
  Query(bool quiet_ = true) : quiet(quiet_), dist_cache(0, 0) {}
  Query(SEXPTYPE type_, bool quiet_ = true) : type(type_), quiet(quiet_), dist_cache(0, 0) {}

  // Just pass the db, we can then access the search index from it
  void update(const Database& db);

  // If the index cache is not initialized, it will return 0
  uint64_t nb_values() const { return index_cache.cardinality(); }

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


  const roaring::Roaring64Map& view() const {return index_cache; }//TODO: provide an R API for that
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

  // Creates a query from a query plan (a plain R list)
  inline static const Query from_plan(SEXP plan, bool quiet = true) {
    if(!Rf_isVectorList(plan)) {
      Rf_error("Expecting a list as input.\n");
    }
    SEXP names = Rf_getAttrib(plan, R_NamesSymbol);

    Query d(ANYSXP, quiet = quiet);

    // If something is not present, it is just not taken into account for the search plan
    std::string cur_name = "";
    SEXP cur_sexp;
    for(int i = 0 ; i < Rf_length(names) ; i++) {
      cur_name = CHAR(STRING_ELT(names, i));
      cur_sexp = VECTOR_ELT(plan, i);
      if(cur_name == "type") {
        d.type = TYPEOF(cur_sexp);// put whatever you want the type of here
      }
      else if(cur_name == "vector") {
        d.is_vector = Rf_asLogical(cur_sexp);
      }
      else if (cur_name == "length") {
        d.length = Rf_asInteger(cur_sexp);
      }
      else if(cur_name == "classname") {
        d.has_class = true;
        if(TYPEOF(cur_sexp) == STRSXP) {
          d.class_names = std::vector<std::string>(Rf_xlength(cur_sexp));
          for(R_xlen_t j = 0; j < Rf_xlength(cur_sexp); j++) {
            d.class_names[j] = CHAR(STRING_ELT(cur_sexp, j));
          }
        }
      }
      else if(cur_name == "na") {
        d.has_na = Rf_asLogical(cur_sexp);
      }
      else if(cur_name == "ndims") {
        d.ndims = Rf_asInteger(cur_sexp);
      }
      else if (cur_name == "attributes") {
        d.has_attributes = Rf_asLogical(cur_sexp);
      }
      else if (cur_name == "package") {
        if(TYPEOF(cur_sexp) == STRSXP) {
          d.packages = std::vector<std::string>(Rf_xlength(cur_sexp));
           for(R_xlen_t j = 0; j < Rf_xlength(cur_sexp); j++) {
            d.packages[j] = CHAR(STRING_ELT(cur_sexp, j));
          }
        }
      }
      else if (cur_name == "func") {
          if(TYPEOF(cur_sexp) == STRSXP) {
          d.functions = std::vector<std::string>(Rf_xlength(cur_sexp));
           for(R_xlen_t j = 0; j < Rf_xlength(cur_sexp); j++) {
            d.functions[j] = CHAR(STRING_ELT(cur_sexp, j));
          }
        }
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

    // Non quietness wins!
    d.quiet = d1.quiet || d2.quiet;

    // TODO: implement other query parameters


    return d;
  }
};


#endif
