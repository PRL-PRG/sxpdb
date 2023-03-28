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

constexpr inline uint32_t return_value =std::numeric_limits<uint32_t>::max();
inline const std::string return_value_str = "";

struct location_t {
  uint32_t package = 0;
  uint32_t function = 0;
  uint32_t param = 0;//std::numeric_limits<uint32_t>::max() will be the magic value to indicate that it is a return value
  bool operator== (const location_t& loc) const  {
    return package == loc.package && function == loc.function && param == loc.param;
  }

  location_t() {}

  location_t(uint32_t pkg, uint32_t fun, uint32_t par) : package(pkg), function(fun), param(par) {}
};


namespace std
{
  template <>
  struct hash<location_t>
  {
    std::size_t operator()(const location_t& c) const
    {
      std::size_t result = 0;
      hash_combine(result, c.package);
      hash_combine(result, c.function);
      hash_combine(result, c.param);
      return result;
    }
  };
}

class Origins {
private:
  pid_t pid;

  std::vector<robin_hood::unordered_set<location_t>> locations;

  robin_hood::unordered_set<location_t> dummy_loc;
  robin_hood::unordered_set<location_t> empty_loc;
  mutable std::vector<location_t> current_loc;

  UniqTextTable package_names;
  UniqTextTable function_names;
  UniqTextTable param_names;

  std::unique_ptr<VSizeTable<std::vector<location_t>>> location_table;

  bool new_origins;
  bool write_mode = false;

  fs::path base_path = "";
public:
  Origins() : pid(getpid()), dummy_loc({location_t(0, 0, 0)}) {}

  Origins(const fs::path& base_path_, bool write = true) : pid(getpid()), dummy_loc({location_t(0, 0, 0)}) {
    open(base_path, write);
  }

  void open(const fs::path& base_path_, bool write = true) {
    write_mode = write;
    base_path = fs::absolute(base_path_);

    location_table = std::make_unique<VSizeTable<std::vector<location_t>>>(base_path / "origins.bin", write_mode);
    package_names.open(base_path / "packages.bin", write_mode);
    function_names.open(base_path/ "functions.bin", write_mode);
    param_names.open(base_path/ "params.bin", write_mode);

    if(write_mode) {
      // Now populate the locations
      locations.clear();
      locations.resize(location_table->nb_values());
      for(uint64_t i = 0; i < location_table->nb_values() ; i++) {
        const std::vector<location_t>& locs = location_table->read(i);
        locations[i].reserve(locs.size());
        if(locs.size() > 1 || (locs.size() == 1 && !(locs[0] == location_t(0, 0, 0)))) {
          locations[i].insert(locs.begin(), locs.end());
#ifndef NDEBUG
          for(const auto& loc : locs) {
            assert(loc.package < package_names.nb_values());
            assert(loc.function < function_names.nb_values());
            assert(loc.param< param_names.nb_values());
          }
#endif
        }
        else {
          assert(locations[i].empty());
        }
      }
      // destroy the location table
      // We will build it again in the destructor
      location_table.reset();
    }
    // inject empty strings in each table, at positions 0
    // it will be used for the empty origins
    if(package_names.nb_values() == 0) {
      package_names.append("");
    }
    if(function_names.nb_values() == 0) {
      function_names.append("");
    }
    if(param_names.nb_values() == 0) {
      param_names.append("");
    }
  }

  void add_origin(uint64_t index, const std::string& package_name, const std::string& function_name, const std::string& param_name) {
      assert(write_mode);
      assert(pid == getpid());
      if(index > locations.size()) {
        Rf_error("Cannot add an origin for a value that was not recorded in the main table."
                   " Last index is %lu, but the index of that new origin is %lu.\n",
                   locations.size(), index);
      }

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
          robin_hood::unordered_set<location_t> locs{loc};
          locations.push_back(locs);
          new_origins = true;
      }
  }

  void append_empty_origin() {
    assert(write_mode);
    locations.push_back(empty_loc);
  }

  const std::vector<location_t>& get_locs(uint64_t index) const {
    if(write_mode) { // we read from memory
      if(index < locations.size()) {
        current_loc.clear();
        auto locs_set = locations[index];
        current_loc.insert(current_loc.end(), locs_set.begin(), locs_set.end());

        return current_loc;
      }
      else {
        current_loc.clear();
        return current_loc;
      }
    }
    else {
      auto& locs = location_table->read(index);

      if(locs.size() > 1 || (locs.size() == 1 && !(locs[0] == location_t(0, 0, 0)))) {
        return locs;
      }
      else {
        return current_loc;// it should be empty by default as we are in read mode
      }
    }
  }

  const std::string& package_name(uint32_t i) const {return package_names.read(i); }
  const std::string& function_name(uint32_t i) const { return function_names.read(i);}
  const std::string& param_name(uint32_t i) const { return param_names.read(i); }

  const std::optional<uint64_t> package_id(const std::string& package_name) const {
    return package_names.get_index(package_name);
  }

  const std::optional<uint64_t> function_id(const std::string& function_name) const {
    return function_names.get_index(function_name);
  }

  const std::vector<std::tuple<std::string, std::string, std::string>> source_locations(uint64_t index) const {
    auto locs = get_locs(index);

    std::vector<std::tuple<std::string, std::string, std::string>> str_locs;
    str_locs.reserve(locs.size());
    for(const auto& loc : locs) {
      assert(loc.package < package_names.nb_values());
      assert(loc.function < function_names.nb_values());
      assert(loc.param < param_names.nb_values());
      str_locs.emplace_back(package_names.read(loc.package),
                            function_names.read(loc.function),
                            param_names.read(loc.param));
    }

    return str_locs;
  }

  const fs::path& get_base_path() const { return base_path; }

  // DO not count the dummy packages
  uint64_t nb_packages() const { return package_names.nb_values() - 1; }
  uint64_t nb_functions() const { return function_names.nb_values() - 1; }
  uint64_t nb_parameters() const { return param_names.nb_values() - 1; }

  const SEXP package_cache() const {return package_names.to_sexp(); }
  const SEXP function_cache() const {return function_names.to_sexp(); }
  const SEXP parameter_cache() const {return param_names.to_sexp();}

  virtual ~Origins() {
    if(write_mode && pid == getpid() && new_origins) {
      {
        fs::rename(base_path / "origins.bin", base_path / "origins-old.bin");
        fs::rename(base_path / "origins.conf", base_path / "origins-old.conf");
        fs::rename(base_path / "origins_offsets.bin", base_path / "origins_offsets-old.bin");
        fs::rename(base_path / "origins_offsets.conf", base_path / "origins_offsets-old.conf");
        //create a new table
        VSizeTable<std::vector<location_t>> location_table(base_path / "origins.bin");
        std::vector<location_t> buf;
        for(const auto& loc : locations) {
          buf.clear();
          if(loc.empty()) {
            buf.push_back(*dummy_loc.begin());
          } else {
            buf.insert(buf.end(), loc.begin(), loc.end());
          }

          location_table.append(buf);
        }
        location_table.flush(); // Should not be needed as we leave scope

        fs::remove(base_path / "origins-old.bin");
        fs::remove(base_path / "origins-old.conf");
        fs::remove(base_path / "origins_offsets-old.bin");
        fs::remove(base_path / "origins_offsets-old.conf");
      }
    }
  }

  void load_hashtables() {
    package_names.load_unique();
    function_names.load_unique();
    // not for the parameters
  }

  bool is_loaded() const {
    return package_names.is_loaded() && function_names.is_loaded();
  }

  uint64_t nb_values() const {
    if(write_mode) {
      return locations.size();
    }
    else {
      return location_table->nb_values();
    }
  }
};

#endif
