#ifndef SXPDB_DEFAULTSTORE_H
#define SXPDB_DEFAULTSTORE_H

#include "store.h"
#include "config.h"

#include <fstream>
#include <unordered_map>
#include <array>


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

class DefaultStore : Store {
private:
  std::string description_name;
  std::string type;
  std::string index_name;
  std::string store_name;
  std::string metadata_name;
  size_t n_values;
  std::fstream index_file;//hash of value, offset to value in the store, offset to metadata
  std::fstream store_file;//values
  std::fstream metadata_file;//function, package, srcref, number of times seen, offset to the value (TODO)

  std::unordered_map<std::array<char, 20>, size_t, container_hasher> index;
  // Values (number of times seen, new during that session or not)
  std::unordered_map<std::array<char, 20>, std::pair<size_t, bool>, container_hasher> metadata;

  size_t bytes_read;

protected:
  virtual void load_index();
  virtual void load_metadata();
  virtual void write_index();

public:
  DefaultStore(const std::string& description_name);

  virtual bool load();

  virtual bool merge_in(const std::string& filename);

  virtual bool add_value(SEXP val);
  virtual bool have_seen(SEXP val) const;

  virtual size_t nb_values() const { return n_values; }

  virtual const std::string& sexp_type() const { return type; };

  virtual SEXP get_value(size_t index) const;

  virtual const std::string& description_file() const  { return description_name; }

  // Pass it a Description and a Distribution that precises what kind of values
  // we want
  virtual SEXP sample_value() const;

  virtual ~DefaultStore();
};


#endif
