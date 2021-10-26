#ifndef RCRD_STORE_H
#define RCRD_STORE_H

#include <iostream>
#include <Rinternals.h>

class Store {
public:
    virtual bool open(std::string filename) = 0;
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
