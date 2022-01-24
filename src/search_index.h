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
#include "config.h"


class Database;


namespace fs = std::filesystem;


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

  // Actual indexes
  std::vector<roaring::Roaring64Map> types_index;//the index in the vector is the type (from TYPEOF())
  roaring::Roaring64Map na_index;//has at least one NA In the vector
  roaring::Roaring64Map class_index;//has a class a attribute
  roaring::Roaring64Map vector_index;//vector (but not scalar)
  roaring::Roaring64Map attributes_index;
  std::vector<roaring::Roaring64Map> lengths_index;

  bool index_generated = false;
  bool new_elements = false;

  uint64_t last_computed = 0;

  friend class Database;

  static roaring::Roaring64Map read_index(const fs::path& path) {
    std::ifstream index_file(path, std::fstream::binary);
    if(!index_file) {
      Rf_error("Index file %s does not exist.\n", path.c_str());
    }

    //get length of file
    index_file.seekg(0, std::ios::end);
    size_t length = index_file.tellg();
    index_file.seekg(0, std::ios::beg);

    std::vector<char> buf;
    buf.reserve(length);
    std::copy( std::istreambuf_iterator<char>(index_file),
               std::istreambuf_iterator<char>(),
               std::back_inserter(buf) );

    return roaring::Roaring64Map::read(buf.data(), true);
  }

  static void write_index(const fs::path& path, const roaring::Roaring64Map& index) {
    std::ofstream index_file(path, std::fstream::binary | std::fstream::trunc);

    if(!index_file) {
      Rf_error("Cannot create index file %s: %s.\n", path.c_str(), strerror(errno));
    }

    size_t size = index.getSizeInBytes();
    std::vector<char> buf(size);
    size_t written = index.write(buf.data(), true);

    if(size != written) {
      Rf_error("Incorrect number of bytes written for index %s: expected = %lu vs actual =%lu.\n", path.c_str(), size, written);
    }

    index_file.write(buf.data(), buf.size());
  }

  static const std::vector<std::pair<std::string, roaring::Roaring64Map>> build_index(const Database& db, uint64_t start, uint64_t end);

public:
  SearchIndex() : pid(getpid()) {
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

  bool is_initialized() const {return index_generated; }

  void open_from_config(const fs::path& base_path, const Config& config) {
    types_index_path = base_path / config["types_index"];
    na_index_path = base_path / config["na_index"];
    class_index_path = base_path / config["class_index"];
    vector_index_path = base_path / config["vector_index"];
    attributes_index_path = base_path / config["vector_index"];
    lengths_index_path = base_path / config["lengths_index"];

    if(!types_index_path.empty()) {
      for(int i = 0; i < 25; i++) {
          types_index[i] = read_index(types_index_path.parent_path() / (types_index_path.stem().string() + "_" + std::to_string(i) + ".ror"));
          new_elements = true;
      }
    }

    if(!na_index_path.empty()) {
      na_index = read_index(na_index_path);
      new_elements = true;
    }

    if(!class_index_path.empty()) {
      class_index = read_index(class_index_path);
      new_elements = true;
    }

    if(!vector_index_path.empty()) {
      vector_index = read_index(vector_index_path);
      new_elements = true;
    }

    if(!attributes_index_path.empty()) {
      attributes_index = read_index(attributes_index_path);
      new_elements = true;
    }

    if(!lengths_index_path.empty()) {
      for(int i = 0; i < nb_intervals; i++) {
        lengths_index[i] = read_index(lengths_index_path.parent_path() / (lengths_index_path.stem().string() + "_" + std::to_string(i) + ".ror"));
        new_elements = true;
      }
    }

  }


  void add_paths_config(std::unordered_map<std::string, std::string>& conf, const fs::path& base_path_) {
    fs::path base_path = base_path_ / "search_index";

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
  }

  roaring::Roaring64Map search_length(const Database& db, const roaring::Roaring64Map& bin_index, uint64_t precise_length) const;

  virtual ~SearchIndex() {
    // Write all the indexes
    if(pid == getpid()) {
      for(int i = 0 ; i < types_index.size() ; i++) {
        write_index(types_index_path.parent_path() / (types_index_path.stem().string()  + "_" + std::to_string(i) + ".ror"), types_index[i]);
      }

      write_index(na_index_path, na_index);
      write_index(class_index_path, class_index);
      write_index(vector_index_path, vector_index);
      write_index(attributes_index_path, attributes_index);

      for(int i = 0; i < lengths_index.size() ; i++) {
        write_index(lengths_index_path.parent_path() / (lengths_index_path.stem().string() + "_" + std::to_string(i) + ".ror"), lengths_index[i]);
      }
    }
  }

  void build_indexes(const Database& db);

};

#endif