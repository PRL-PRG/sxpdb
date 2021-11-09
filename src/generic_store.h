#ifndef SXPDB_GENERIC_STORE_H
#define SXPDB_GENERIC_STORE_H

#include "default_store.h"


// A generic store that stores more metadata that the default store
class GenericStore : protected DefaultStore {
private:
  struct metadata_t {
    size_t n_calls;
    SEXPTYPE sexptype;
    size_t size;
  };
  std::unordered_map<std::array<std::byte, 20>, metadata_t, container_hasher> metadata;

protected:

  //virtual void create();

public:
  GenericStore(const fs::path& config_path);

  // virtual bool add_value(SEXP val);
  // virtual bool have_seen(SEXP val) const;
  //
  // virtual const std::string& sexp_type() const;
  //
  // virtual SEXP get_value(size_t index);
  //
  // virtual size_t nb_values() const;
  //
  // virtual SEXP get_metadata(SEXP val) const;
  //
  // virtual const fs::path& description_path() const ;
  //
  // virtual SEXP sample_value();

  virtual ~GenericStore() {};
};

#endif
