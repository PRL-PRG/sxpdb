#ifndef SXPDB_SEARCH_INDEX_H
#define SXPDB_SEARCH_INDEX_H

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include "Rversion.h"

#include <filesystem>
#include <fstream>
#include <unistd.h>
#include <vector>
#include <string>

#ifdef SXPDB_PARALLEL_STD
#include <execution>
#endif

#include "roaring.hh"
#include "robin_hood.h"
#include "config.h"

#include "reverse_index.h"
#include "serialization.h"

class Database;


namespace fs = std::filesystem;

roaring::Roaring64Map read_index(const fs::path& path) ;
void write_index(const fs::path& path, const roaring::Roaring64Map& index);


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
#ifdef SXPDB_PARALLEL_STD
    return std::find_if(std::execution::par_unseq, v, v + length, [](const Rcomplex& c) -> bool {return ISNAN(c.r) || ISNAN(c.i);}) != v + length;
#else
    return std::find_if(v, v + length, [](const Rcomplex& c) -> bool {return ISNAN(c.r) || ISNAN(c.i);}) != v + length;
#endif
  }
  case REALSXP: {
    double* v = REAL(val);
    int length = Rf_length(val);
#ifdef SXPDB_PARALLEL_STD
    return std::find_if(std::execution::par_unseq, v, v + length, [](double d) -> bool {return ISNAN(d) ;}) != v + length;
#else
    return std::find_if( v, v + length, [](double d) -> bool {return ISNAN(d) ;}) != v + length;
#endif
  }
  case LGLSXP:{
    int* v = LOGICAL(val);
    int length = Rf_length(val);
#ifdef SXPDB_PARALLEL_STD
    return std::find(std::execution::par_unseq, v, v + length, NA_LOGICAL) != v + length;
#else
    return std::find(v, v + length, NA_LOGICAL) != v + length;
#endif
  }
  case INTSXP: {
    int* v = INTEGER(val);
    int length = Rf_length(val);
#ifdef SXPDB_PARALLEL_STD
    return std::find(std::execution::par_unseq, v, v + length, NA_INTEGER) != v + length;
#else
    return std::find(v, v + length, NA_INTEGER) != v + length;
#endif
    }
  }
  return false;
}

// This version works on the array of bytes directly
inline bool find_na(const sexp_view_t& sexp_view) {
    size_t length = sexp_view.length;

    switch(sexp_view.type) {
    case LGLSXP: {
      const int* v = static_cast<const int*>(sexp_view.data);
#ifdef SXPDB_PARALLEL_STD
      return std::find(std::execution::par_unseq, v, v + length, NA_LOGICAL) != v +length;
#else
      return std::find(v, v + length, NA_LOGICAL) != v + length;
#endif
    }
      case INTSXP: {
      const int* v = static_cast<const int*>(sexp_view.data);
#ifdef SXPDB_PARALLEL_STD
      return std::find(std::execution::par_unseq, v, v + length, NA_INTEGER) != v + length;
#else
      return std::find(v, v + length, NA_INTEGER) != v + length;
#endif
      }
    case REALSXP: {
        const double* v = static_cast<const double*>(sexp_view.data);
#ifdef SXPDB_PARALLEL_STD
      return std::find_if(std::execution::par_unseq, v, v + length, [](double d) -> bool {return ISNAN(d) ;}) != v + length;
#else
      return std::find_if( v, v + length, [](double d) -> bool {return ISNAN(d) ;}) != v + length;
#endif
      }
    case CPLXSXP: {
      const Rcomplex* v = static_cast<const Rcomplex*>(sexp_view.data);
#ifdef SXPDB_PARALLEL_STD
      return std::find_if(std::execution::par_unseq, v, v + length, [](const Rcomplex& c) -> bool {return ISNAN(c.r) || ISNAN(c.i);}) != v + length;
#else
      return std::find_if(v, v + length, [](const Rcomplex& c) -> bool {return ISNAN(c.r) || ISNAN(c.i);}) != v + length;
#endif
    }
    case STRSXP: {
      // This one is more complex has it stores CHARSXP which do not have the same length
      const char* data = static_cast<const char*>(sexp_view.data);
      SEXPTYPE type = ANYSXP;
      int size = 0;
      for(size_t i = 0; i < length; i++) {
        std::memcpy(&type, data, sizeof(int));
        type &= 255;// this would also store the encoding...
        assert(type == CHARSXP);
        data += sizeof(int);
        std::memcpy(&size, data, sizeof(int));
        data += sizeof(int);
        if(size == -1) {// this is NA_STRING
          return true;
        }
        assert(size >= 0);
        //else we jump to the next CHARSXP
        data += size;
      }
      return false;
    }

    }

    return false;
}

class SearchIndex {
  friend class Query;
public:
  inline static const int nb_sexptypes = 26;
  inline static const int nb_intervals= 200;
  inline static std::array<uint64_t, nb_intervals> length_intervals{0};
private:
  pid_t pid;
  // Paths
  fs::path types_index_path = "";
  fs::path na_index_path = "";
  fs::path class_index_path = "";
  fs::path vector_index_path = "";
  fs::path attributes_index_path = "";
  fs::path lengths_index_path = "";
  fs::path classnames_index_path = "";

  // Actual indexes
  std::vector<roaring::Roaring64Map> types_index;//the index in the vector is the type (from TYPEOF())
  roaring::Roaring64Map na_index;//has at least one NA In the vector
  roaring::Roaring64Map class_index;//has a class a attribute
  roaring::Roaring64Map vector_index;//vector (but not scalar)
  roaring::Roaring64Map attributes_index;
  std::vector<roaring::Roaring64Map> lengths_index;

  ReverseIndex classnames_index;


  bool index_generated = false;
  bool new_elements = false;

  uint64_t last_computed = 0;

  bool write_mode = false;

  friend class Database;


  static const std::vector<std::pair<std::string, roaring::Roaring64Map>> build_indexes_static_meta(const Database& db, uint64_t start, uint64_t end);
  static const std::vector<std::pair<std::string, roaring::Roaring64Map>> build_indexes_values(const Database& db, uint64_t start, uint64_t end);
  static const std::vector<std::pair<std::string, roaring::Roaring64Map>> build_indexes_classnames(const Database& db, ReverseIndex& index, uint64_t start, uint64_t end);
  static const std::vector<std::pair<std::string, roaring::Roaring64Map>> build_values(const std::vector<std::vector<std::byte>>& bufs, uint64_t start);

public:
   SearchIndex() : pid(getpid()), classnames_index(200) {
    // We populate the length intervals in any cases
    // init intervals
    for(int i = 0; i < 101; i++) {
      length_intervals[i] = i;
    }
    int base = 10;
    int power = 10;
    for(int i = 0; i < 10; i++) {
      for(int j = 100 + 10 * i + 1; j < 100 + 10 * (i + 1); j++) {
        length_intervals[j] = length_intervals[j - 1] + power;
      }
      power *= base;
    }

    types_index.resize(26);
    lengths_index.resize(nb_intervals);
  }

  void set_write_mode(bool write) { write_mode = write; }

  bool is_initialized() const {return index_generated; }

  void open_from_config(const fs::path& base_path, const Config& config);

  void add_paths_config(std::unordered_map<std::string, std::string>& conf, const fs::path& base_path_) {
    fs::path base_path = base_path_ / "search_index";

    // Write the ath in write_mode or if in read mode, if the index have already been generated before
    // ( in write mode)
    if(write_mode || index_generated) {
      if(!fs::exists(base_path)) {
        fs::create_directory(base_path);
      }

      if(types_index_path.empty()) {
        types_index_path = base_path / "types_index";
      }
      conf["types_index"] = fs::relative(types_index_path, base_path_);
      if(na_index_path.empty()) {
        na_index_path = base_path / "na_index.ror";
      }
      conf["na_index"] = fs::relative(na_index_path, base_path_);
      if(class_index_path.empty()) {
        class_index_path = base_path / "class_index.ror";
      }
      conf["class_index"] = fs::relative(class_index_path, base_path_);
      if(vector_index_path.empty()) {
        vector_index_path = base_path / "vector_index.ror";
      }
      conf["vector_index"] = fs::relative(vector_index_path, base_path_);
      if(attributes_index_path.empty()) {
        attributes_index_path = base_path / "attributes_index.ror";
      }
      conf["attributes_index"] = fs::relative(attributes_index_path, base_path_);
      if(lengths_index_path.empty()) {
        lengths_index_path = base_path / "lengths_index";
      }
      conf["lengths_index"] = fs::relative(lengths_index_path, base_path_);

      if(classnames_index_path.empty()) {
        classnames_index_path = base_path / "classnames_index.conf";
      }
      conf["classnames_index"] = fs::relative(classnames_index_path, base_path_);

      conf["index_last_computed"] = std::to_string(last_computed);

      conf["index_generated"] = std::to_string(index_generated);
    }
    else {
      conf["index_last_computed"] = "0";
      conf["index_generated"] = "0";
    }

  }

  roaring::Roaring64Map search_length(const Database& db, const roaring::Roaring64Map& bin_index, uint64_t precise_length) const;

  roaring::Roaring64Map search_classname(const Database& db, const roaring::Roaring64Map& bin_index, uint32_t precise_classname) const;

  virtual ~SearchIndex();

  void build_indexes(const Database& db);

};

#endif
