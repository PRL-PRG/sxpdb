#ifndef RCRD_STORE_H
#define RCRD_STORE_H

#include <iostream>
#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

class Store {
public:
    // virtual bool merge_in(const std::string& filename) = 0;

    virtual bool add_value(SEXP val) = 0 ;
    virtual bool have_seen(SEXP val) const = 0;

    virtual const std::string& sexp_type() const = 0;// the return type will be more complex when we deal with richer queries

    virtual SEXP get_value(size_t index) const = 0;

    virtual size_t nb_values() const = 0;

    virtual const std::string& description_file() const = 0;

    // Pass it a Description and a Distribution that precises what kind of values
    // we want
    virtual SEXP sample_value() const = 0;

    virtual ~Store() {};
};


#endif
