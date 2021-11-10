#ifndef SXPDB_GLOBAL_STORE_H
#define SXPDB_GLOBAL_STORE_H

#include "store.h"
#include "default_store.h"
#include "source_ref.h"

#include <vector>
#include <memory>
#include <random>



class GlobalStore : Store {
  private:
    // Stores the names of the various stores, their types, their number of values
    // it is redundant with the configuration files of the various stores, but it is ok
    fs::path configuration_path;

    // to preserve the order
    std::vector<std::unique_ptr<DefaultStore>> stores;
    std::unordered_map<std::string, size_t> types;


    size_t bytes_read;

    size_t total_values;

    std::default_random_engine rand_engine;

  protected:
    virtual void write_configuration();
    virtual void create();

    SourceRefs src_refs;//TODO

  public:
    GlobalStore(const std::string& description_name);

    virtual bool merge_in(GlobalStore& store);

    virtual const std::string& sexp_type() const {
      static std::string any = "any";
      return any;
    }

    virtual bool add_value(SEXP val);
    virtual bool have_seen(SEXP val) const;

    virtual const fs::path& description_path() const {return configuration_path; }
    virtual size_t nb_values() const {return total_values; }

    virtual SEXP get_value(size_t index);

    virtual SEXP get_metadata(SEXP val) const;
    virtual SEXP get_metadata(size_t index) const;

    // Pass it a Description and a Distribution that precises what kind of values
    // we want
    virtual SEXP sample_value();

    virtual ~GlobalStore();
};

#endif
