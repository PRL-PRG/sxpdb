#ifndef SXPDB_SRC_REF_H
#define SXPDB_SRC_REF_H

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include "hasher.h"
#include "store.h"

#include <filesystem>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <iostream>
#include <fstream>
#include <tuple>
#include <optional>

#include "hasher.h"

constexpr inline uint32_t return_value =std::numeric_limits<uint32_t>::max();
inline const std::string return_value_str = "";

struct location_t {
  uint32_t package;
  uint32_t function;
  uint32_t argument;//std::numeric_limits<uint32_t>::max() will be the magic value to indicate that it is a return value

  bool operator== (const location_t& loc) const  {
    return package == loc.package && function == loc.function && argument == loc.argument;
  }
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
    hash_combine(result, c.argument);
    return result;
  }
};

}

namespace fs = std::filesystem;

class SourceRefs {
private:
  fs::path config_path;
  fs::path offsets_path;//offsets to the list of source locations per value
  fs::path index_path;//the list of source locations per value (as serialized location_t)
  fs::path store_path;//all the components of a source location

  size_t n_packages;
  size_t n_functions;
  size_t n_args;

  size_t n_values;
  bool new_elements = false;


  void load_configuration();
  void load_index();
  void load_store();


  void write_store();
  void write_index();
  void write_configuration();

  // the custom hasher and comparison directly work on the value pointed to, not on the pointer
  typedef std::unordered_map<std::shared_ptr<const std::string>, uint64_t, unique_string_hasher, unique_string_equal> unique_names_t;
  typedef std::vector<std::shared_ptr<const std::string>> ordered_names_t;

  // Set to get a stable order on it
  ordered_names_t package_names;
  ordered_names_t function_names;
  ordered_names_t argument_names;
  unique_names_t pkg_names_u;
  unique_names_t function_names_u;
  unique_names_t arg_names_u;



  std::unordered_map<sexp_hash, std::unordered_set<location_t>, xxh128_hasher> index;
  std::unordered_map<sexp_hash, uint64_t,xxh128_hasher> offsets;

  static uint64_t inline add_name(const std::string& name, unique_names_t& unique_names, ordered_names_t& ordering);

public:
  SourceRefs(fs::path config_path);

  void merge_in(SourceRefs& srcrefs);

  bool add_value(const sexp_hash& key, const std::string& package_name, const std::string& function_name, const std::string& argument_name);

  const std::unordered_set<location_t> get_locs(const sexp_hash& key) const;

  const ordered_names_t pkg_names(const sexp_hash& key) const;

  const std::vector<std::tuple<const std::string, const std::string, const std::string>> source_locations(const sexp_hash& key) const;

  uint64_t get_offset(const sexp_hash& key) const;

  const fs::path& description_path() const { return config_path;}

  size_t nb_packages() const { return package_names.size(); }

  virtual ~SourceRefs();
};




#endif
