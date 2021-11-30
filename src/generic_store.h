#ifndef SXPDB_GENERIC_STORE_H
#define SXPDB_GENERIC_STORE_H

#include "default_store.h"
#include "source_ref.h"
#include "hasher.h"

#include <memory>


// A generic store that stores more metadata that the default store
class GenericStore : public DefaultStore {
private:
  struct metadata_t {
    uint64_t n_calls;
    SEXPTYPE sexptype;
    uint64_t size;
    uint32_t n_merges;
#ifdef SXPDB_TIMER_SER_HASH
    std::chrono::nanoseconds first_seen_dur;
    std::chrono::nanoseconds next_seen_dur;
#endif
  };
  std::unordered_map<sexp_hash, metadata_t, xxh128_hasher> metadata;
  std::shared_ptr<SourceRefs> src_locs;

protected:
  virtual void load_metadata();
  virtual void write_metadata();

public:
  GenericStore(const fs::path& config_path, std::shared_ptr<SourceRefs> source_locations);

  virtual std::pair<const sexp_hash*, bool> add_value(SEXP val);

  virtual bool merge_in(Store& other);
  virtual bool merge_in(GenericStore& other);

  virtual SEXP get_metadata(SEXP val) const;
  virtual SEXP get_metadata(uint64_t idx) const;


  virtual ~GenericStore();
};

#endif
