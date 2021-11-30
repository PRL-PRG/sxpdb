#include "hasher.h"


// from boost::hash_combine
void hash_combine(std::size_t& seed, std::size_t value) {
  seed ^= value + 0x9e3779b9 + (seed<<6) + (seed>>2);
}

bool operator==(const sexp_hash& h1, const sexp_hash& h2)
{
  return XXH128_isEqual(h1, h2);
}
