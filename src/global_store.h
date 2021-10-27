#ifndef SXPDB_GLOBAL_STORE_H
#define SXPDB_GLOBAL_STORE_H

#include "store.h"
#include "default_store.h"

#include <vector>
#include <memory>

class GlobalStore : Store {
  private:
    // Stores the names of the various stores, their types, their number of values
    // it is redundant with the configuration files of the various stores, but it is ok
    std::string configuration_name;
    std::vector<std::unique_ptr<DefaultStore>> stores;

    size_t bytes_read;

  protected:
    virtual void load_index();
    virtual void load_metadata();
    virtual void write_index();

  public:
    GlobalStore(const std::string& description_name);

    virtual bool load();

    virtual bool merge_in(const std::string& filename);

    virtual const std::string& sexp_type() const { return "any"; };

    virtual bool add_value(SEXP val);
    virtual bool have_seen(SEXP val) const;

    virtual SEXP get_value(size_t index) const;

    // Pass it a Description and a Distribution that precises what kind of values
    // we want
    virtual SEXP sample_value() const;

    virtual ~GlobalStore();
};

#endif
