#ifndef RCRD_STORE_H
#define RCRD_STORE_H

#include <fstream>
#include <unordered_map>
#include <Rinternals.h>

class Store {
private:
  std::fstream description_file;//description of the file (types stored, sizes and names of the 3 other files, number of values)
  std::fstream index_file;//hash of value, offset to value in the store, offset to metadata
  std::fstream store_file;//values
  std::fstream metadata_file;//function, package, srcref, number of times seen, offset to the value

  //TODO: write a proper hash class that can automatically give its hash
  // instead of wasting memory
  // Or use std::map?
  std::unordered_map<std::string, size_t> index;

public:
    Store(std::string description_name);

    virtual bool merge_in(std::string filename) = 0;

    virtual bool add_value(SEXP val) = 0 ;
    virtual bool have_seen(SEXP val) const = 0;

    virtual SEXP get_value(size_t index) const = 0;

    // Pass it a Description and a Distribution that precises what kind of values
    // we want
    virtual SEXP sample_value() const = 0;

    virtual ~Store() {};
};


#endif
