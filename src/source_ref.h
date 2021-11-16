#ifndef SXPDB_SRC_REF_H
#define SXPDB_SRC_REF_H

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include <filesystem>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <iostream>
#include <fstream>
#include <tuple>
#include <optional>

extern void hash_combine(std::size_t& seed, std::size_t value);

struct container_hasher;

struct location_t {
  uint32_t package;
  uint32_t function;
  uint32_t argument;
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
  fs::path index_path;//the list of source locations per value (as serialized location_t)
  fs::path store_path;//all the components of a source location

  size_t n_packages;
  size_t n_functions;
  size_t n_args;


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



  std::unordered_map<std::array<char, 20>, std::unordered_set<location_t>, container_hasher> index;
  std::unordered_map<std::array<char, 20>, size_t,container_hasher> offsets;

  static size_t inline add_name(const std::string& name, std::unordered_map<std::string, size_t>& unique_names, std::vector<const std::string*>& ordering);

public:
  SourceRefs(fs::path config_path);

  void merge_in(SourceRefs& srcrefs);

  bool add_value(const std::array<char, 20>& key, const std::string& package_name, const std::string& function_name, const std::string& argument_name);

  std::optional<const location_t&> get_loc(const std::array<char, 20>& key) const;

  const std::string& package_name(const std::array<char, 20>& key) const;
  const std::string& function_name(const std::array<char, 20>& key) const;
  const std::string& argument_name(const std::array<char, 20>& key) const;

  const std::tuple<const std::string&, const std::string&, const std::string&> source_location(const std::array<char, 20>& key) const;

  size_t get_offset(const std::array<char, 20>& key) const;

  const fs::path& description_path() const { return config_path;}

  size_t nb_packages() const { return package_names.size(); }
};




#endif
