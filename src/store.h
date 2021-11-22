#ifndef RCRD_STORE_H
#define RCRD_STORE_H

#include <iostream>
#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include <filesystem>
#include <string>
#include <chrono>
#include <optional>

namespace fs = std::filesystem;


typedef std::array<char, 20> sexp_hash;

class Store {
private:
    std::string store_k;
protected:
    virtual void create() = 0;
    void set_kind(const std::string& kind) {store_k = kind;}
public:
    // virtual bool merge_in(const std::string& filename) = 0;
    Store(const std::string& kind) : store_k(kind) {}

    //returns the hash of the value, and true if is newly inserted, false if it was already there
    // hash will be nullptr if we don't want to compute it at all
    virtual std::pair<const sexp_hash*, bool> add_value(SEXP val) = 0 ;
    virtual bool have_seen(SEXP val) const = 0;
    //virtual const sexp_hash& get_sexp_hash(SEXP val) = 0;

    virtual const std::string& sexp_type() const = 0;// the return type will be more complex when we deal with richer queries
    virtual const std::string& store_kind() const { return store_k;};// typed, generic, specialized...

    virtual SEXP get_value(size_t index) = 0;

    virtual const sexp_hash& get_hash(size_t index) const = 0;

    virtual size_t nb_values() const = 0;

    virtual SEXP get_metadata(SEXP val) const = 0;
    virtual SEXP get_metadata(size_t index) const = 0;

    virtual const fs::path& description_path() const = 0;

    virtual std::chrono::microseconds avg_insertion_duration() const = 0;

    // Pass it a Description and a Distribution that precises what kind of values
    // we want
    virtual SEXP sample_value() = 0;

    virtual ~Store() {};
};


#endif
