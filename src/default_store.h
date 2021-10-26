#ifndef SXPDB_DEFAULTSTORE_H
#define SXPDB_DEFAULTSTORE_H

#include "store.h"

#include <fstream>
#include <unordered_map>
#include <Rinternals.h>

struct Description {
  std::string type;
  std::string index_name;
  std::string store_name;
  std::string metadata_name;
  size_t nb_values;

  Description(const std::string& filename);

  void write(std::ostream& output);

};

class DefaultStore : Store {
private:
  Description description;//description of the file (types stored, sizes and names of the 3 other files, number of values)
  std::fstream index_file;//hash of value, offset to value in the store, offset to metadata
  std::fstream store_file;//values
  std::fstream metadata_file;//function, package, srcref, number of times seen, offset to the value

  //TODO: write a proper hash class that can automatically give its hash
  // instead of wasting memory
  // Or use std::map?
  std::unordered_map<std::string, size_t> index;


public:
  DefaultStore(std::string description_name);

  virtual bool open(std::string filename);

  virtual bool merge_in(std::string filename);

  virtual bool add_value(SEXP val);
  virtual bool have_seen(SEXP val) const;

  virtual SEXP get_value(size_t index) const;

  // Pass it a Description and a Distribution that precises what kind of values
  // we want
  virtual SEXP sample_value() const;

  virtual ~DefaultStore() {};
};


#endif
