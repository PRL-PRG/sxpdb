#ifndef SXPDB_HASHER_H
#define SXPDB_HASHER_H

#include <iostream>

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

#endif
