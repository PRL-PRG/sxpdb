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



struct location_t {
  uint32_t package;
  uint32_t function;
  uint32_t argument;

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


  void load_configuration();
  void load_index();
  void load_store();


  void write_store();
  void write_index();
  void write_configuration();


  // Set to get a stable order on it
  std::vector<const std::string*> package_names;
  std::vector<const std::string*> function_names;
  std::vector<const std::string*> argument_names;
  std::unordered_map<std::string, size_t> pkg_names_u;
  std::unordered_map<std::string, size_t> function_names_u;
  std::unordered_map<std::string, size_t> arg_names_u;



  std::unordered_map<sexp_hash, std::unordered_set<location_t>, container_hasher> index;
  std::unordered_map<sexp_hash, size_t,container_hasher> offsets;

  static size_t inline add_name(const std::string& name, std::unordered_map<std::string, size_t>& unique_names, std::vector<const std::string*>& ordering);

public:
  SourceRefs(fs::path config_path);

  void merge_in(SourceRefs& srcrefs);

  bool add_value(const sexp_hash& key, const std::string& package_name, const std::string& function_name, const std::string& argument_name);

  const std::unordered_set<location_t> get_locs(const sexp_hash& key) const;

  const std::vector<const std::string*> pkg_names(const sexp_hash& key) const;
  const std::vector<const std::string*> func_names(const sexp_hash& key) const;
  const std::vector<const std::string*> arg_names(const sexp_hash& key) const;

  const std::vector<std::tuple<const std::string&, const std::string&, const std::string&>> source_locations(const sexp_hash& key) const;

  size_t get_offset(const sexp_hash& key) const;

  const fs::path& description_path() const { return config_path;}

  size_t nb_packages() const { return package_names.size(); }

  virtual ~SourceRefs();
};




#endif
