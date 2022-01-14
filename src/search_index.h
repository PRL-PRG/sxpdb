#ifndef SXPDB_SEARCH_INDEX_H
#define SXPDB_SEARCH_INDEX_H

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include "Rversion.h"

#include <filesystem>
#include <fstream>
#include <unistd.h>
#include <thread>
#include <future>
#include <vector>
#include <string>

#include "roaring.hh"
#include "config.h"
#include "database.h"

namespace fs = std::filesystem;

class SearchIndex {
  friend class Query;
public:
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
      Rf_error("Cannot create index file %s.\n", path.c_str());
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

  void open_from_config(const Config& config) {
    types_index_path = config["types_index"];
    na_index_path = config["na_index"];
    class_index_path = config["class_index"];
    vector_index_path = config["vector_index"];
    attributes_index_path = config["vector_index"];
    lengths_index_path = config["lengths_index"];

    if(!types_index_path.empty()) {
      for(int i = 0; i < 25; i++) {
          types_index[i] = read_index(types_index_path.parent_path() / (types_index_path.filename().string() + "_" + std::to_string(i) + ".ror"));
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
        lengths_index[i] = read_index(lengths_index_path.parent_path() / (lengths_index_path.filename().string() + "_" + std::to_string(i) + ".ror"));
        new_elements = true;
      }
    }

  }


  void add_paths_config(std::unordered_map<std::string, std::string>& conf, const fs::path& base_path) {
    if(types_index_path.empty()) {
      types_index_path = base_path / "types_index";
    }
    conf["types_index"] = types_index_path;
    if(na_index_path.empty()) {
      na_index_path = base_path / "na_index.ror";
    }
    conf["na_index"] = na_index_path;
    if(class_index_path.empty()) {
      class_index_path = base_path / "class_index.ror";
    }
    conf["class_index"] = class_index_path;
    if(vector_index_path.empty()) {
      vector_index_path = base_path / "vector_index.ror";
    }
    conf["vector_index"] = vector_index_path;
    if(attributes_index_path.empty()) {
      attributes_index_path = base_path / "attributes_index.ror";
    }
    conf["attributes_index"] = attributes_index_path;
    if(lengths_index_path.empty()) {
      lengths_index_path = base_path / "lengths_index";
    }
    conf["lengths_index"] = lengths_index_path;
  }

  roaring::Roaring64Map search_length(const roaring::Roaring64Map& bin_index, uint64_t precise_length) const;

  virtual ~SearchIndex() {
    // Write all the indexes
    if(pid == getpid()) {
      for(int i = 0 ; i < types_index.size() ; i++) {
        write_index(types_index_path / ("_" + std::to_string(i) + ".ror"), types_index[i]);
      }

      write_index(na_index_path, na_index);
      write_index(class_index_path, class_index);
      write_index(vector_index_path, vector_index);
      write_index(attributes_index_path, attributes_index);

      for(int i = 0; i < lengths_index.size() ; i++) {
        write_index(lengths_index_path / ("_" + std::to_string(i) + ".ror"), lengths_index[i]);
      }
    }
  }

  void build_indexes(const Database& db, int nb_workers = std::thread::hardware_concurrency() - 1) {
    // We dot no clear the indexes: indeed, we cannot remove values from the database

    //TODO: only do it for the ones that are actually reading from the value store, because ifstream is not thread-safe
    // So we also need to add a mutex...

    // Would be better if we could use the new execution policies of C++17, so we could do it at the level
    // of individual value and use work stealing

    uint64_t chunk_size = db.nb_values() / nb_workers;
    std::vector<std::future<const std::vector<std::pair<std::string, roaring::Roaring64Map>>>> future_results;
    future_results.reserve(nb_workers + 1);

    for(uint64_t end = chunk_size ; end < db.nb_values() ; end += chunk_size) {
      future_results.push_back(std::async(std::launch::async, build_index, db, end - chunk_size, end));
    }
    // there is  a remaining chunk that is smaller than chunk_size
    future_results.push_back(std::async(std::launch::async, build_index, db, (nb_workers - 1) * chunk_size, db.nb_values()));

    // Wait for the results
    std::vector<std::vector<std::pair<std::string, roaring::Roaring64Map>>> results;
    results.reserve(nb_workers + 1);
    std::transform(future_results.begin(), future_results.end(), results.begin(), [](auto& fut) { return fut.get(); });

    for(const auto& indexes : results) {
      assert(indexes.size() == types_index.size() + 4 + lengths_index.size());
      int i = 0;
      for(; i < types_index.size() ; i ++) {
        assert(indexes[i].first == "type_index");
        types_index[i] |= indexes[i].second;
      }
      assert(indexes[i].first == "na_index");
      na_index |= indexes[i].second;
      i++;
      assert(indexes[i].first == "class_index");
      class_index |= indexes[i].second;
      i++;
      assert(indexes[i].first == "vector_index");
      vector_index |= indexes[i].second;
      i++;
      assert(indexes[i].first == "na_index");
      attributes_index |= indexes[i].second;
      i++;
      int start = i;
      for(; i < start + lengths_index.size() ; i++) {
        assert(indexes[i].first == "length_index");
        lengths_index[i] |= indexes[i].second;
      }
    }
  }

};

#endif
