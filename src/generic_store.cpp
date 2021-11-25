#include "generic_store.h"
#include "sha1.h"

#include <cassert>


GenericStore::GenericStore(const fs::path& config_path, std::shared_ptr<SourceRefs> source_locations) :
  DefaultStore(config_path), src_locs(source_locations) {
  set_kind("generic");
  type = "any";

  // if it exists, the DefaultStore has already done the loading for the store and indices and we just need to load the metadata
  if(std::filesystem::exists(config_path)) {
      load_metadata();
  }
  else {
    index_name = config_path.filename().string() + "_index.bin";
    store_name = config_path.filename().string() + "_store.bin";
    metadata_name = config_path.filename().string() + "_meta.bin";
    n_values = 0;
    create();
  }
}


std::pair<const sexp_hash*, bool> GenericStore::add_value(SEXP val) {
  auto added =  DefaultStore::add_value(val);

  if(!added.second) {// value already seen
    //update metadata
    metadata[*added.first].n_calls++;
    assert(metadata[*added.first].size = ser.current_buf_size());// buffer is still the same
    assert(metadata[*added.first].sexptype == TYPEOF(val));
  }
  else {
    // not seen so add metadata
    metadata_t meta;
    meta.n_calls = 1;
    meta.size = ser.current_buf_size();
    meta.sexptype = TYPEOF(val);
    metadata[*added.first] = meta;
  }

  new_elements = true;// there is at least n_calls which has changed

  return added;
}

SEXP GenericStore::get_metadata(uint64_t idx) const {
  auto it = index.begin();
  std::advance(it, idx);
  sexp_hash key = it->first;

  auto it2 = metadata.find(key);
  if(it2 == metadata.end()) {
    return R_NilValue;
  }

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

  if(it2 == metadata.end()) {
    return R_NilValue;
  }


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


void GenericStore::write_metadata() {
  fs::path meta_path = fs::absolute(configuration_path).parent_path().append(metadata_name);
  std::ofstream meta_file(meta_path, std::fstream::out | std::fstream::binary | std::fstream::trunc);

  meta_file.exceptions(std::fstream::failbit);

  // we cannot just append the new ones, because n_calls might have changed
  // TODO: we could patch in n_calls though
  // it would require to store the offset of the metadata in the file, and then
  // seek to it to modify in place
  for(auto& it: metadata) {
    // write the hash
    meta_file.write(it.first.data(), it.first.size());
    // And now the metadata
    meta_file.write(reinterpret_cast<char*>(&it.second.n_calls), sizeof(it.second.n_calls));
    meta_file.write(reinterpret_cast<char*>(&it.second.size), sizeof(it.second.size));
    meta_file.write(reinterpret_cast<char*>(&it.second.sexptype), sizeof(it.second.sexptype));
  }
}

void GenericStore::load_metadata() {
  fs::path meta_path = fs::absolute(configuration_path).parent_path().append(metadata_name);
  std::ifstream meta_file(meta_path, std::fstream::out | std::fstream::binary);

  meta_file.exceptions(std::fstream::failbit);

  metadata.reserve(n_values);

  sexp_hash hash;
  assert(hash.size() == 20);

  metadata_t meta;

  for(uint64_t i = 0; i < n_values ; i++) {
    auto pos = meta_file.tellg();

    meta_file.read(hash.data(), hash.size());
    meta_file.read(reinterpret_cast<char*>(&meta.n_calls), sizeof(meta.n_calls));
    meta_file.read(reinterpret_cast<char*>(&meta.size), sizeof(meta.size));
    meta_file.read(reinterpret_cast<char*>(&meta.sexptype), sizeof(meta.sexptype));

    metadata[hash] = meta;

    bytes_read += meta_file.tellg() - pos;
    assert(meta_file.tellg() - pos == hash.size() + sizeof(meta.n_calls) + sizeof(meta.size) + sizeof(meta.sexptype));
  }
}


bool GenericStore::merge_in(GenericStore& other) {
  bool res = DefaultStore::merge_in(other);

  // Merge metadata
  for(auto& val : other.metadata) {
      auto it = metadata.find(val.first);
      if(it == metadata.end()) { // New value so we just add it
        metadata[val.first] = val.second;
      }
      else {
        // Check they are compatible
        if(it->second.sexptype != val.second.sexptype) {
          Rf_error("Two values with same hash with different types %s and %s",
                   Rf_type2char(it->second.sexptype),
                   Rf_type2char(val.second.sexptype));
        }
        if(it->second.size != val.second.size) {
          Rf_error("Two values with same hash with different sizes %s and %s",
                   it->second.size,
                   val.second.size);
        }

        // update the number of calls
        it->second.n_calls += val.second.n_calls;
        new_elements = true;
      }
  }


  return res;
}

bool GenericStore::merge_in(Store& store) {
  GenericStore* st = dynamic_cast<GenericStore*>(&store);
  if(st == nullptr) {
    Rf_warning("Cannot merge a store with kind %s with a store of kind", store_kind().c_str(), store.store_kind().c_str());
    return false;
  }

  return merge_in(*st);
}

GenericStore::~GenericStore() {
  if(new_elements || n_values == 0) {
    write_metadata();
    write_index();
    write_configuration();
  }
}
