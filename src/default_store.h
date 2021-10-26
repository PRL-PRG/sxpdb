#ifndef SXPDB_DEFAULTSTORE_H
#define SXPDB_DEFAULTSTORE_H

#include "store.h"

#include <fstream>
#include <unordered_map>
#include <array>

struct Description {
  std::string type;
  std::string index_name;
  std::string store_name;
  std::string metadata_name;
  size_t nb_values;

  Description(const std::string& filename);

  void write(std::ostream& output);

};

// from boost::hash_combine
void hash_combine(std::size_t& seed, std::size_t value) {
  seed ^= value + 0x9e3779b9 + (seed<<6) + (seed>>2);
}


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
  Description description;//description of the file (types stored, sizes and names of the 3 other files, number of values)
  std::fstream index_file;//hash of value, offset to value in the store, offset to metadata
  std::fstream store_file;//values
  std::fstream metadata_file;//function, package, srcref, number of times seen, offset to the value

  std::unordered_map<std::array<char, 20>, size_t, container_hasher> index;

  size_t bytes_read;

protected:
  void load_index();

public:
  DefaultStore(const std::string& description_name);

  virtual bool load();

  virtual bool merge_in(const std::string& filename);

  virtual bool add_value(SEXP val);
  virtual bool have_seen(SEXP val) const;

  virtual SEXP get_value(size_t index) const;

  // Pass it a Description and a Distribution that precises what kind of values
  // we want
  virtual SEXP sample_value() const;

  virtual ~DefaultStore();
};


#endif
