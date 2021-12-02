#ifndef SXPDB_DEFAULTSTORE_H
#define SXPDB_DEFAULTSTORE_H

#include "store.h"
#include "config.h"
#include "serialization.h"
#include "hasher.h"

#include <fstream>
#include <unordered_map>
#include <array>
#include <random>





class DefaultStore : public Store {
protected:
  fs::path configuration_path;
  std::string type;
  std::string index_name;
  std::string store_name;
  std::string metadata_name;
  size_t n_values;
  std::fstream index_file;//hash of value, offset to value in the store, offset to metadata
  mutable std::fstream store_file;//values

  // it means that the order in the hash map won't necessary be the order of offsets
  // it that bad because of data locality?
  std::unordered_map<sexp_hash, uint64_t, xxh128_hasher> index;
  // new during that session or not
  std::unordered_map<sexp_hash, bool, xxh128_hasher> newly_seen;

  size_t bytes_read;

  mutable Serializer ser;
  std::default_random_engine rand_engine;


  // Optimization
  // we store the hash of the addresses of the sexp during a session
  // also involves setting the tracing bit
  mutable std::unordered_map<SEXP, sexp_hash> sexp_adresses;



protected:
  virtual void load_index();
  virtual void write_index();
  virtual void create();
  virtual void write_configuration();

  sexp_hash* const cached_hash(SEXP val) const;

  const sexp_hash compute_hash(SEXP val) const;

  sexp_hash* const compute_cached_hash(SEXP val, const std::vector<std::byte>& buf) const;

// Some debuging counters
#ifndef NDEBUG
  struct debug_counters_t {
    uint64_t n_maybe_shared = 0;// how many times MAYBE_SHARED(val) has been true on a SEXP that was being added
    uint64_t n_sexp_address_opt = 0;// how many times we have been able to use the SEXP address optimization
  };
  std::unordered_map<sexp_hash, debug_counters_t, xxh128_hasher> debug_counters;
#endif

public:
  DefaultStore(const fs::path& description_name);

  DefaultStore(const fs::path& description_name, const std::string& type);

  virtual bool load();

  virtual bool merge_in(Store& other);
  virtual bool merge_in(DefaultStore& other);

  virtual  std::pair<const sexp_hash*, bool> add_value(SEXP val);
  virtual bool have_seen(SEXP val) const;

  virtual size_t nb_values() const { return n_values; }

  virtual const std::string& sexp_type() const { return type; };

  virtual SEXP get_value(uint64_t index);

  virtual const sexp_hash& get_hash(uint64_t index) const;


  virtual const fs::path& description_path() const  { return configuration_path; }

  virtual SEXP get_metadata(SEXP val) const;
  virtual SEXP get_metadata(uint64_t index) const;

  // Pass it a Description and a Distribution that precises what kind of values
  // we want
  virtual SEXP sample_value(); // attach metadata to an attribute?

  virtual ~DefaultStore();
};


#endif
