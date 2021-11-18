#ifndef SXPDB_GENERIC_STORE_H
#define SXPDB_GENERIC_STORE_H

#include "default_store.h"
#include "source_ref.h"
#include "hasher.h"

#include <memory>


// A generic store that stores more metadata that the default store
class GenericStore : protected DefaultStore {
private:
  struct metadata_t {
    size_t n_calls;
    SEXPTYPE sexptype;
    size_t size;
  };
  std::unordered_map<sexp_hash, metadata_t, container_hasher> metadata;
  std::shared_ptr<SourceRefs> src_locs;

protected:
  virtual void load_metadata();
  virtual void write_metadata();

  virtual void create();

public:
  GenericStore(const fs::path& config_path, std::shared_ptr<SourceRefs> source_locations);

  virtual std::pair<const sexp_hash*, bool> add_value(SEXP val);

  virtual const std::string& sexp_type() const {
          static const std::string any = "any";
          return any;
  }


  virtual SEXP get_metadata(SEXP val) const;


  virtual ~GenericStore();
};

#endif
