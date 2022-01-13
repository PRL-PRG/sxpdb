#ifndef SXPDB_DATABASE_H
#define SXPDB_DATABASE_H

#if !defined(PKG_V_MAJOR) || !defined(PKG_V_MINOR) || !defined(PKG_V_PATCH)
#error "The version major.minor.patch is not defined."
#endif

#ifndef PKG_V_DEVEL
#define PKG_V_DEVEL "0"
#endif

#include <unistd.h>
#include <optional>
#include <random>

#include "table.h"
#include "query.h"
#include "search_index.h"
#include "utils.h"
#include "hasher.h"
#include "origins.h"

#include "robin_hood.h"
#include "xxhash.h"
#include "roaring.hh"


typedef XXH128_hash_t sexp_hash;

struct runtime_meta_t {
  uint64_t n_calls;
  uint32_t n_merges;
};

struct static_meta_t {
  SEXPTYPE sexptype;// we can also pack some booleans here, as a SEXPTYPE is 25 maximum
  uint64_t size;
  uint64_t length;
  uint64_t n_attributes;
  uint32_t n_dims;
};

struct debug_counters_t {
  uint64_t n_maybe_shared = 0;// how many times MAYBE_SHARED(val) has been true on a SEXP that was being added
  uint64_t n_sexp_address_opt = 0;// how many times we have been able to use the SEXP address optimization
};

class Database {
public:
  static const int version_major = stoi(PKG_V_MAJOR);
  static const int version_minor = stoi(PKG_V_MINOR);
  static const int version_patch = stoi(PKG_V_PATCH);
  static const int version_development = stoi(PKG_V_DEVEL);
private:
  uint64_t nb_total_values = 0;
  bool new_elements = false;
  bool write_mode;
  bool quiet;
  pid_t pid;
  std::default_random_engine rand_engine;


  //TODO: Try to share the sexp_hash from the table in the hash table
  // Create a new Table derivative for that?
  FSizeTable<sexp_hash> hashes;
  robin_hood::unordered_map<const sexp_hash*, uint64_t, xxh128_pointer_hasher> unique_sexps;

  VSizeTable<std::vector<std::byte>> sexp_table;
  FSizeTable<runtime_meta_t> runtime_meta;//Data that changes at runtime
  FSizeTable<static_meta_t> static_meta;//Data that will never change after being written once

  FSizeTable<debug_counters_t> debug_counters;// will be loaded into in debug mode

  SearchIndex search_index;

  Origins origins;

  //TODO: add the SourceRefs store (but modify it first so that it uses the new Table<T> derivatives)

  fs::path config_path;

  void write_configuration();
public:
  // write_mode entails loading more data structures in memory
  // So choosing to read only should be much quicker
  Database(const fs::path& config_, bool write_mode = false, bool quiet_ = true);

  // Merge two databases
  void merge_in(Database& db);

  // Adding R values/origins
  std::pair<const sexp_hash*, bool> add_value(SEXP val);
  std::pair<const sexp_hash*, bool> add_value(SEXP val, const std::string& pkg_name, const std::string& func_name, const std::string& arg_name);
  bool add_origins(const sexp_hash& hash, const std::string& pkg_name, const std::string& func_name, const std::string& arg_name);

  // Accessors for individual elements
  bool have_seen(SEXP val) const;
  const sexp_hash& get_hash(uint64_t index) const;
  uint64_t get_index(const sexp_hash& h) const;
  const SEXP get_value(uint64_t index) const;
  const SEXP get_metadata(uint64_t index) const;
  const std::vector<std::tuple<const std::string, const std::string, const std::string>> source_locations(size_t index) const;

  //Accessors for sampling
  const SEXP sample_value();
  const SEXP sample_value(Query& query);

  // Accessors for elements in bulk
  const SEXP view_values();// or just have a special query that returns everything in an efficient way?
  const SEXP view_values(const Query& query);

  const SEXP view_metadata();
  const SEXP view_metadata(const Query& query);

  const SEXP view_origins();
  const SEXP view_origins(const Query& query);

  // Map on all the elements and return an R value
  const SEXP map(const SEXP function);
  const SEXP map(const Query& query, const SEXP function);

  //Rebuilding the indexes from scratch
  void build_indexes();

  // Utilities
  const std::vector<size_t> check(bool slow_check);
  const fs::path& configuration_path() const {return config_path; }
  uint64_t nb_values() const;

  virtual ~Database();

};

#endif
