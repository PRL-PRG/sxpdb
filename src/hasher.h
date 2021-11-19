#ifndef SXPDB_HASHER_H
#define SXPDB_HASHER_H

#include <iostream>
#include <memory>

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
