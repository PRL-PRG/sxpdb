#ifndef SXPDB_GLOBAL_STORE_H
#define SXPDB_GLOBAL_STORE_H

#include "store.h"
#include "default_store.h"
#include "generic_store.h"
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
    std::vector<std::unique_ptr<GenericStore>> stores;
    std::unordered_map<std::string, uint64_t> types;


    size_t bytes_read;
    size_t total_values;

    std::default_random_engine rand_engine;

    bool quiet;

  protected:
    virtual void write_configuration();
    virtual void create();

    std::shared_ptr<SourceRefs> src_refs;


  public:
    GlobalStore(const std::string& description_name, bool _quiet);

    virtual bool merge_in(Store& store);
    virtual bool merge_in(GlobalStore& store);

    virtual const std::string& sexp_type() const {
      static std::string any = "any";
      return any;
    }

    virtual std::pair<const sexp_hash*, bool> add_value(SEXP val);
    // if arg_name == "", then it is a return value, not an argument
    virtual std::pair<const sexp_hash*, bool> add_value(SEXP val, const std::string& pkg_name, const std::string& func_name, const std::string& arg_name);
    virtual bool have_seen(SEXP val) const;

    virtual const fs::path& description_path() const {return configuration_path; }
    virtual size_t nb_values() const {return total_values; }

    virtual SEXP get_value(uint64_t index);
    virtual const SEXP view_values();

    virtual const sexp_hash& get_hash(uint64_t index) const;


    virtual SEXP get_metadata(SEXP val) const;
    virtual SEXP get_metadata(uint64_t index) const;
    virtual const SEXP view_metadata() const;

    virtual SEXP sample_value();

    virtual bool add_origins(const sexp_hash& hash, const std::string& pkg_name, const std::string& func_name, const std::string& arg_name);

    const std::vector<std::tuple<const std::string, const std::string, const std::string>> source_locations(const sexp_hash& key) const {
        return src_refs->source_locations(key);
    }

    const std::vector<std::tuple<const std::string, const std::string, const std::string>> source_locations(size_t index) const;
    const SEXP view_origins() const;

    virtual const std::vector<size_t> check(bool slow_check);

    virtual const SEXP map(const SEXP function);

    virtual ~GlobalStore();
};

#endif
