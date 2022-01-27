#ifndef SXPDB_REVERSE_INDEX_H
#define SXPDB_REVERSE_INDEX_H

#include <vector>
#include <filesystem>
#include <map>
#include <algorithm>
#include <unordered_map>
#include <cassert>

namespace fs =  std::filesystem;

#include "roaring.hh"

#include "config.h"


roaring::Roaring64Map read_index(const fs::path& path) ;
void write_index(const fs::path& path, const roaring::Roaring64Map& index);


class ReverseIndex {
private:
  // Represents the intervals on the integer property of values stored at index i
  // indexes[i] represents the values with property in [intervals[i - 1], intervals[i]]
  std::vector<uint64_t> intervals;
  std::vector<roaring::Roaring64Map> indexes;

  // maximum number of elements per interval
  uint64_t threshold = 100;

  //we are interested in the order
  std::map<uint64_t, std::vector<uint64_t>> reverse;
public:
  ReverseIndex(uint64_t thr) : threshold(thr) {

  }

  void open(const fs::path& config_path) {
    Config conf(config_path);

    threshold = std::stoul(conf["threshold"]);
    uint64_t nb_indexes = std::stoul(conf["nb_indexes"]);

    for(uint64_t i = 0; i < nb_indexes ; i++) {
      fs::path index_path = config_path.parent_path() / (config_path.stem().string() + "_" + std::to_string(i) + ".ror");
      auto index = read_index(index_path);
      indexes.push_back(index);
      intervals.push_back(index.minimum());
    }
  }

  void write(const fs::path& config_path) {
    std::unordered_map<std::string, std::string> conf;
    conf["threshold"] = std::to_string(threshold);
    conf["nb_indexes"] = std::to_string(indexes.size());

    for(uint64_t i = 0 ; i < indexes.size(); i++) {
      fs::path index_path = config_path.parent_path() / (config_path.stem().string() + "_" + std::to_string(i) + ".ror");
      write_index(index_path, indexes[i]);
    }

    Config file(std::move(conf));
    file.write(config_path);
  }


  void add_property(uint64_t index, uint64_t property) {
    auto it = reverse.find(property);

    if(it != reverse.end()) {
      it->second.push_back(index);
    }else {
      reverse[property] = {0};
    }
  }

  void finalize_indexes() {
    indexes.clear();
    intervals.clear();
    roaring::Roaring64Map cur_index;
    for(const auto& prop : reverse) {
      if(cur_index.cardinality() > threshold) {
        indexes.push_back(cur_index);
        intervals.push_back(cur_index.minimum());

        cur_index.clear();
      }

      cur_index.addMany(prop.second.size(), prop.second.data());
    }
    //add the last one if it has not just been added (i.e. is not empty)
    if(!cur_index.isEmpty()) {
      indexes.push_back(cur_index);
      intervals.push_back(cur_index.minimum());
    }

    reverse.clear();// or keep it in case we need to add things in it?
  }

  // Returns the set of indexes corresponding to the property
  // For that, we locate the bin in indexes
  // It is up to the caller to perform a linear search in it if needed
  // to build the actual index
  // The second element of the pair is whether the bitmap represents one
  // or several properties. In the second case, we will have to do the linear search...
  std::pair<roaring::Roaring64Map, bool> get_index(uint64_t property) const {
    // > and not >= so rather give a custom comparison operator?
    // lower_bound or upper_bound
    auto it = std::lower_bound(intervals.begin(), intervals.end(), property);

    uint64_t idx = 0;
    if(it != intervals.end()) {
      idx = it  - intervals.begin();
    }
    else {
      idx = indexes.size() - 1;
    }
    return {indexes[idx], idx < intervals.size() &&  intervals[idx + 1] - intervals[idx] <= 1} ;
  }
};

#endif
