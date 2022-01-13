#ifndef SXPDB_ORIGINS_H
#define SXPDB_ORIGINS_H

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include "robin_hood.h"



#include <filesystem>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <iostream>
#include <fstream>
#include <tuple>
#include <optional>
#include <unistd.h>

#include "table.h"
#include "hasher.h"

struct location_t {
  uint32_t package = 0;
  uint32_t function = 0;
  uint32_t argument = 0;//std::numeric_limits<uint32_t>::max() will be the magic value to indicate that it is a return value

  bool operator== (const location_t& loc) const  {
    return package == loc.package && function == loc.function && argument == loc.argument;
  }

  location_t(uint32_t pkg, uint32_t fun, uint32_t param) : package(pkg), function(fun), argument(param) {}
};


namespace std
{
  template <>
  struct hash<location_t*>
  {
    std::size_t operator()(const location_t* c) const
    {
      std::size_t result = 0;
      hash_combine(result, c->package);
      hash_combine(result, c->function);
      hash_combine(result, c->argument);
      return result;
    }
  };
}

class Origins {
private:
  pid_t pid;

  std::vector<robin_hood::unordered_set<location_t>> locations;

  UniqTextTable package_names;
  UniqTextTable function_names;
  UniqTextTable param_names;

  bool new_origins;

  fs::path base_path;
public:
  Origins(const fs::path& base_path_) : pid(getpid()), base_path(base_path_) {
    VSizeTable<std::vector<location_t>> location_table(base_path / "origins");
    package_names.open(base_path / "packages.txt");
    function_names.open(base_path/ "functions.txt");
    param_names.open(base_path/ "params.txt");

    // Now populate the locations and the hash table
    locations.resize(locations.size());
    for(uint64_t i = 0; i < location_table.nb_values() ; i++) {
      std::vector<location_t> locs = location_table.read(i);
      locations[i].reserve(locs.size());
      locations[i].insert(locs.begin(), locs.end());
    }
  }

  void add_origin(uint64_t index, const std::string& package_name, const std::string& function_name, const std::string& param_name) {
      location_t loc(package_names.append_index(package_name),
                     function_names.append_index(function_name),
                     param_names.append_index(param_name));

      // either the index is a value already seen, or it is a new one, and in that case, the index must size(), i.e.
      // just one past the last valid index
      if(index < locations.size()) {
          auto res = locations[index].insert(loc);
        if(res.second) {
          new_origins = true;
        }
      }
      else if(index == locations.size()) {
          locations.emplace_back(loc);
          new_origins = true;
      }
      else {
        Rf_error("Cannot add an origin for a value that was not recorded in the main table."
                   " Last index is %lu, but the index of that new origin is %lu.\n",
                   locations.size(), index);
      }
  }

  const robin_hood::unordered_set<location_t>& get_locs(uint64_t index) const {
    return locations[index];
  }

  const std::vector<std::tuple<const std::string_view, const std::string_view, const std::string_view>> source_locations(uint64_t index) const {
    auto locs = get_locs(index);

    std::vector<std::tuple<const std::string_view, const std::string_view, const std::string_view>> str_locs;
    str_locs.reserve(locs.size());
    for(const auto& loc : locs) {
      str_locs.emplace_back(package_names.read(loc.package),
                            function_names.read(loc.function),
                            param_names.read(loc.argument));
    }

    return str_locs;
  }

  const fs::path& get_base_path() const { return base_path; }

  uint64_t nb_packages() const { return package_names.nb_values(); }
  uint64_t nb_functions() const { return function_names.nb_values(); }
  uint64_t nb_parameters() const { return param_names.nb_values(); }

  virtual ~Origins() {
    if(pid == getpid() && new_origins) {
      {
        //create a new table
        VSizeTable<std::vector<location_t>> location_table(base_path / "origins-new");
        std::vector<location_t> buf;
        for(const auto& loc : locations) {
          buf.clear();
          buf.insert(buf.end(), loc.begin(), loc.end());
          location_table.append(buf);
        }
        location_table.flush(); // Should not be needed as we leave scope
      }

      // rename the new one table to the old one, so if there is a crash while we write our big file, it is ok
      // it will just roll back to the old data
      // will throw in case of problems
      fs::rename(base_path / "origins-new.bin", base_path / "origins.bin");
      fs::rename(base_path / "origins-new_offsets.bin", base_path / "origins_offsets.bin");
    }
  }
};

#endif
