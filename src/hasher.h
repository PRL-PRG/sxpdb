#ifndef SXPDB_HASHER_H
#define SXPDB_HASHER_H

#include <iostream>
#include <memory>

#include "xxhash.h"

typedef XXH128_hash_t sexp_hash;

// from boost::hash_combine
inline void hash_combine(std::size_t& seed, std::size_t value) {
  seed ^= value + 0x9e3779b9 + (seed<<6) + (seed>>2);
}





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

struct xxh128_hasher {
  std::size_t operator()(const sexp_hash& c) const
  {
    std::size_t result = 0;
    hash_combine(result, c.low64);
    hash_combine(result, c.high64);
    return result;
  }
};

struct xxh128_pointer_hasher {
  std::size_t operator()(const sexp_hash* c) const
  {
    std::size_t result = 0;
    hash_combine(result, c->low64);
    hash_combine(result, c->high64);
    return result;
  }
};

struct xxh128_pointer_equal{
  std::size_t operator()(const sexp_hash* h1, const sexp_hash* h2) const {
    return XXH128_isEqual(*h1, *h2);;
  }
};


struct string_pointer_hasher {
  std::size_t operator()(const std::string*  s) const {
    return std::hash<std::string>()(*s);
  }
};

struct string_pointer_equal{
  std::size_t operator()(const std::string*   s1, const std::string* s2) const {
    return *s1 == *s2;
  }
};

inline bool operator==(const sexp_hash& h1, const sexp_hash& h2)
{
  return XXH128_isEqual(h1, h2);
}




struct unique_string_hasher {
    std::size_t operator()(const std::shared_ptr<const std::string>& c) const
    {
      std::hash<std::string> hasher;
      return hasher(*c);
    }
};

struct unique_string_equal {
  std::size_t operator()(const std::shared_ptr<const std::string>& s1, const std::shared_ptr<const std::string>& s2) const
  {
    return *s1 == * s2;
  }
};


#endif
