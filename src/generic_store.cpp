#include "generic_store.h"
#include "sha1.h"

#include <cassert>


GenericStore::GenericStore(const fs::path& config_path, std::shared_ptr<SourceRefs> source_locations) :
  DefaultStore(config_path), src_locs(source_locations) {
  set_kind("generic");
}



std::pair<const sexp_hash*, bool> GenericStore::add_value(SEXP val) {
  auto added =  DefaultStore::add_value(val);

  if(added.second) {
    //update metadata
    metadata[*added.first].n_calls++;
    assert(metadata[*added.first].size = buf.size());// buffer is still the same
    assert(metadata[*added.first].sexptype == TYPEOF(val));
  }

  return added;
}


SEXP GenericStore::get_metadata(SEXP val) const {
  const std::vector<std::byte>& buf = ser.serialize(val);

  sexp_hash key;
  sha1_context ctx;
  sha1_starts(&ctx);
  sha1_update(&ctx,  reinterpret_cast<uint8*>(const_cast<std::byte*>(buf.data())), buf.size());
  sha1_finish(&ctx, reinterpret_cast<uint8*>(key.data()));

  auto it = newly_seen.find(key);

  if(it == newly_seen.end()) {
    return R_NilValue;
  }

  auto it2 = metadata.find(key);
  assert(it2 != metadata.end());
  const metadata_t& meta = it2->second;


  const char*names[] = {"newly_seen", "size", "n", "type", ""};
  SEXP res = PROTECT(Rf_mkNamed(VECSXP, names));

  SEXP n_seen = PROTECT(Rf_ScalarLogical(it->second));
  SET_VECTOR_ELT(res, 0, n_seen);

  SEXP s_size = PROTECT(Rf_ScalarInteger(meta.size));
  SET_VECTOR_ELT(res, 1, s_size);

  SEXP n_calls = PROTECT(Rf_ScalarInteger(meta.n_calls));
  SET_VECTOR_ELT(res, 2, n_calls);

  SEXP s_type = PROTECT(Rf_ScalarInteger(meta.sexptype));
  SET_VECTOR_ELT(res, 3, s_type);

  UNPROTECT(5);

  return res;
}
