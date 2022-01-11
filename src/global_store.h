#ifndef SXPDB_GLOBAL_STORE_H
#define SXPDB_GLOBAL_STORE_H

#include "store.h"
#include "default_store.h"
#include "generic_store.h"
#include "source_ref.h"
#include "roaring.hh"
#include "description.h"
#include "robin_hood.h"

#include <vector>
#include <memory>
#include <random>
#include <unistd.h>



class GlobalStore : Store {
  private:
    // Stores the names of the various stores, their types, their number of values
    // it is redundant with the configuration files of the various stores, but it is ok
    fs::path configuration_path;

    // to preserve the order
    std::vector<std::unique_ptr<GenericStore>> stores;
    robin_hood::unordered_map<std::string, uint64_t> types;


    size_t bytes_read;
    size_t total_values;

    std::default_random_engine rand_engine;

    bool quiet;

    pid_t pid;

  protected:
    virtual void write_configuration();
    virtual void create();

    std::shared_ptr<SourceRefs> src_refs;

    //Indexes
    bool index_generated = false;
    std::vector<roaring::Roaring64Map> types_index;//the index in the vector is the type (from TYPEOF())
    roaring::Roaring64Map na_index;//has at least one NA In the vector
    roaring::Roaring64Map class_index;//has a class a attribute
    roaring::Roaring64Map vector_index;//vector (but not scalar)
    roaring::Roaring64Map attributes_index;
    roaring::Roaring64Map integer_real;

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

    virtual SEXP sample_value(const Description& description);

    virtual void build_indexes();
    
    virtual const SEXP get_integer_real()  {
      return stores[0]->get_integer_real(integer_real);
    }

    virtual bool add_origins(const sexp_hash& hash, const std::string& pkg_name, const std::string& func_name, const std::string& arg_name);

    const std::vector<std::tuple<const std::string, const std::string, const std::string>> source_locations(const sexp_hash& key) const {
        return src_refs->source_locations(key);
    }

    const std::vector<std::tuple<const std::string, const std::string, const std::string>> source_locations(size_t index) const;
    virtual const SEXP view_origins() const;

    virtual const SEXP view_origins(std::shared_ptr<SourceRefs>) const {return view_origins();}

    virtual const std::vector<size_t> check(bool slow_check);

    virtual const SEXP map(const SEXP function);

    virtual ~GlobalStore();
};

#endif
