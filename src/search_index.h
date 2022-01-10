#ifndef SXPDB_SEARCH_INDEX_H
#define SXPDB_SEARCH_INDEX_H

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include "Rversion.h"

#include <filesystem>
#include <fstream>

#include "roaring.hh"
#include "config.h"

namespace fs = std::filesystem;

class SearchIndex {
public:
  inline static const int nb_intervals= 200;
  inline static std::array<uint64_t, nb_intervals> length_intervals{0};
private:
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

public:
  SearchIndex() {
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

  void open_from_config(const Config& config) {
    types_index_path = config["types_index"];
    na_index_path = config["na_index"];
    class_index_path = config["class_index"];
    vector_index_path = config["vector_index"];
    attributes_index_path = config["vector_index"];
    lengths_index_path = config["lengths_index"];

    if(!types_index_path.empty()) {
      for(int i = 0; i < 25; i++) {
          types_index[i] = read_index(types_index_path.parent_path().append(types_index_path.filename().string() + "_" + std::to_string(i)));
      }
    }

    if(!na_index_path.empty()) {
      na_index = read_index(na_index_path);
    }

    if(!class_index_path.empty()) {
      class_index = read_index(class_index_path);
    }

    if(!vector_index_path.empty()) {
      vector_index = read_index(vector_index_path);
    }

    if(!attributes_index_path.empty()) {
      attributes_index = read_index(attributes_index_path);
    }

    if(!lengths_index_path.empty()) {
      for(int i = 0; i < nb_intervals; i++) {
        lengths_index[i] = read_index(lengths_index_path.parent_path().append(lengths_index_path.filename().string() + "_" + std::to_string(i)));
      }
    }
  }
};

#endif
