#ifndef SXPDB_DEFAULTSTORE_H
#define SXPDB_DEFAULTSTORE_H

#include "store.h"
#include "config.h"
#include "serialization.h"

#include <fstream>
#include <unordered_map>
#include <array>
#include <random>


// from boost::hash_combine
void hash_combine(std::size_t& seed, std::size_t value);


// TODO: make it work for more complex values (when elem is a pair for instance)
struct container_hasher {
  template<class T>
  std::size_t operator()(const T& c) const {
    std::size_t seed = 0;
    for(const auto& elem : c) {
      hash_combine(seed, std::hash<typename T::value_type>()(elem));
    }
    return seed;
  }
};

class DefaultStore : protected Store {
private:
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
  std::unordered_map<std::array<char, 20>, size_t, container_hasher> index;
  // new during that session or not
  std::unordered_map<std::array<char, 20>, bool, container_hasher> newly_seen;

  size_t bytes_read;

  mutable Serializer ser;
  std::default_random_engine rand_engine;

protected:
  virtual void load_index();
  virtual void load_metadata();
  virtual void write_index();
  virtual void create();
  virtual void write_configuration();

public:
  DefaultStore(const fs::path& description_name);

  DefaultStore(const fs::path& description_name, const std::string& type);

  virtual bool load();

  virtual bool merge_in(DefaultStore& other);

  virtual bool add_value(SEXP val);
  virtual bool have_seen(SEXP val) const;

  virtual size_t nb_values() const { return n_values; }

  virtual const std::string& sexp_type() const { return type; };

  virtual SEXP get_value(size_t index);

  virtual const fs::path& description_path() const  { return configuration_path; }

  virtual SEXP get_metadata(SEXP val) const;
  virtual SEXP get_metadata(size_t index) const;

  // Pass it a Description and a Distribution that precises what kind of values
  // we want
  virtual SEXP sample_value(); // attach metadata to an attribute?

  virtual ~DefaultStore();
};


#endif
