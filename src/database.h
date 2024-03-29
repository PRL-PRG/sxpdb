#ifndef SXPDB_DATABASE_H
#define SXPDB_DATABASE_H

#if !defined(PKG_V_MAJOR) || !defined(PKG_V_MINOR) || !defined(PKG_V_PATCH)
#error "The version major.minor.patch is not defined."
#endif

#ifndef PKG_V_DEVEL
#pragma message("Not a development version.")
#define PKG_V_DEVEL "0" // if it were in devel, it would be at least 9000
#endif

#include <unistd.h>
#include <optional>
#include <random>
#include <optional>

#include "table.h"
#include "query.h"
#include "search_index.h"
#include "utils.h"
#include "hasher.h"
#include "origins.h"
#include "classnames.h"
#include "serialization.h"
#include "call_ids.h"
#include "dbnames.h"

#include "robin_hood.h"
#include "xxhash.h"
#include "roaring++.h"


class Query;

struct runtime_meta_t {
  uint64_t n_calls = 1;
  uint32_t n_merges = 0;
};

struct static_meta_t {
  SEXPTYPE sexptype;// we can also pack some booleans here, as a SEXPTYPE is 25 maximum
  uint64_t size;
  uint64_t length;
  uint64_t n_attributes;
  uint32_t n_dims;
  uint32_t n_rows;// length will n_rows * n_cols in the case of matrixes
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

  friend class SearchIndex;
  friend class Query;

  typedef std::unordered_map<const sexp_hash*, uint64_t, xxh128_pointer_hasher, xxh128_pointer_equal> sexp_hash_map;

  enum class OpenMode {Read, Write, Merge};
private:
  uint64_t nb_total_values = 0;
  bool new_elements = false;
  bool new_index = false;
  OpenMode mode;
  bool quiet;
  pid_t pid;
  std::default_random_engine rand_engine;
  fs::path config_path;
  fs::path base_path;

  // Tables:
  // values, metadata, debug counters,
  // search indexes and origin tables
  // **********************************
  FSizeMemoryViewTable<sexp_hash> hashes;
  sexp_hash_map sexp_index;

  VSizeTable<std::vector<std::byte>> sexp_table;
  FSizeTable<runtime_meta_t> runtime_meta;//Data that change at runtime
  FSizeTable<static_meta_t> static_meta;//Data that will never change after being written once
  FSizeTable<debug_counters_t> debug_counters;// will be loaded into in debug mode only
  SearchIndex search_index;
  Origins origins;
  ClassNames classes;
  CallIds call_ids;
  DBNames dbnames;

  // Handling of SEXP serialization,
  // SEXP caching and so on
  // We usually do not need these things in read mode
  // ********************
  mutable robin_hood::unordered_map<SEXP, uint64_t> sexp_addresses;
  mutable Serializer ser;

  static inline const bool maybe_shared(SEXP val) { return REFCNT(val) - 1 > 1;}
  std::optional<uint64_t> cached_sexp(SEXP val) const;
  void cache_sexp(SEXP val, uint64_t index);
  const sexp_hash compute_hash(SEXP val) const;
  const sexp_hash compute_hash(SEXP val, const std::vector<std::byte>& buf) const;


  void write_configuration();
public:
  // write_mode entails loading more data structures in memory
  // So choosing to read only should be much quicker if the goal is just to sample from the database
  Database(const fs::path& config_, OpenMode mode, bool quiet_ = true);

  // Merge two databases
  // returns the number of new values
  uint64_t merge_in(Database& db);
  uint64_t parallel_merge_in(Database& other, uint64_t min_chunk_size = 10);
  // returns a mapping from old indexes in the source db to new indexes in the new db
  std::vector<uint64_t> merge_into(Database& db);

  // Adding R values/origins
  std::tuple<const sexp_hash*, uint64_t, bool> add_value(SEXP val);//TODO this should add dummy origins
  std::tuple<const sexp_hash*, uint64_t, bool> add_value(SEXP val, const std::string& pkg_name, const std::string& func_name, const std::string& arg_name, uint64_t call_id);
  void add_origin(uint64_t index, const std::string& pkg_name, const std::string& func_name, const std::string& param_name);

  // Accessors for individual elements
  std::optional<uint64_t> have_seen(SEXP val) const;
  const sexp_hash& get_hash(uint64_t index) const;
  std::optional<uint64_t> get_index(const sexp_hash& h) const;
  const SEXP get_value(uint64_t index) const;
  const SEXP get_metadata(uint64_t index) const;
  const std::vector<std::tuple<std::string, std::string, std::string>> source_locations(uint64_t index) const;

  //Accessors for sampling
  const SEXP sample_value();
  // Sample n elements without replacement
  // TODO: support n > 1 ; what should we return? Simply a list of SEXP?
  const SEXP sample_value(Query& query, uint64_t n = 1);
  const std::optional<uint64_t> sample_index(Query& query);
  const std::optional<uint64_t> sample_index();

  // TODO: see if it is possible to implement the version without Query in terms of
  // the other version, in an efficient way.
  // Accessors for elements in bulk
  const SEXP view_values() const;// or just have a special query that returns everything in an efficient way?
  const SEXP view_values(Query& query) const;

  const SEXP view_metadata() const;
  const SEXP view_metadata(Query& query) const;

  const SEXP view_origins() const;
  const SEXP view_origins(Query& query) const;

  const SEXP view_call_ids() const;

  const SEXP view_db_names() const;

  const SEXP values_from_origin(const std::string& package, const std::string& function);
  const SEXP values_from_calls(const std::string& package, const std::string& function);

  // Map on all the elements and return an R value
  const SEXP map(const SEXP function);
  const SEXP map(Query& query, const SEXP function);

  // Filter given a function and returns the indexes for which it is true
  const SEXP filter_index(const SEXP function);
  const SEXP filter_index(Query& query, const SEXP function);

  //Rebuilding the indexes from scratch
  void build_indexes() {
    if(mode == OpenMode::Read) {
      Rf_warning("Cannot build the index in read mode.\n");
      return;
    }
    
    //it populates a hash table
    // required to build later the reverse index
    classes.load_all();
    search_index.build_indexes(*this) ;
    new_index = true;
  }

  bool has_search_index() const {
    return search_index.is_initialized();
  }

  fs::path search_index_path() const {
    return search_index.base_path();
  }

  void update_query(Query& query) const;

  // Utilities
  const std::vector<size_t> check(bool slow_check);
  const fs::path& configuration_path() const {return config_path; }

  uint64_t nb_values() const { return nb_total_values; }
  uint64_t nb_values(Query& query) const;

  bool rw_mode() const { return mode == OpenMode::Write; }

  bool is_quiet() const { return quiet; }

  virtual ~Database();

};

#endif
