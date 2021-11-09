#ifndef SXPDB_SRC_REF_H
#define SXPDB_SRC_REF_H

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>
#include <iostream>
#include <fstream>


namespace fs = std::filesystem;

class SourceRefs {
private:
  fs::path config_path;
  fs::path source_refs_path;

  // Src ref content
  struct src_ref_t {
    std::string function_name;
    std::string argument_name; //"" => return value
    // store name, offset in the store
    std::vector<std::pair<std::string, size_t>> values;
  };

  struct package_t {
    size_t offset;// first offset for this package
    std::vector<src_ref_t> srcrefs;
  };

  std::unordered_map<std::string, package_t> packages;

  void load_configuration();
  void load_store();

public:
  SourceRefs(fs::path config_path);

  void merge_in(SourceRefs& srcrefs);

  // must be called before write_configuration
  // because write_configuration needs offset to be set first
  void write();
  void write_configuration();
};

#endif
