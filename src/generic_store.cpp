#include "generic_store.h"
#include "sha1.h"
#include "xxhash.h"

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
  auto start = std::chrono::steady_clock::now();
  auto added =  DefaultStore::add_value(val);
  auto end = std::chrono::steady_clock::now();

  if(!added.second) {// value already seen
    metadata_t& meta = metadata[*added.first];
    // do not take into account the first insertion for the average
    meta.next_seen_dur += (std::chrono::duration_cast<std::chrono::nanoseconds>(end - start) - meta.next_seen_dur) / meta.n_calls;
    //update metadata
    meta.n_calls++;
    assert(metadata[*added.first].size = ser.current_buf_size());// buffer is still the same
    assert(metadata[*added.first].sexptype == TYPEOF(val));
  }
  else {
    // not seen so add metadata
    metadata_t meta;
    meta.n_calls = 1;
    meta.size = ser.current_buf_size();
    meta.sexptype = TYPEOF(val);
    meta.first_seen_dur = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    meta.next_seen_dur =  std::chrono::nanoseconds::zero();
    meta.n_merges = 0;
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


  const char*names[] = {"newly_seen", "size", "n", "type", "first_seen_dur", "next_seen_dur", "n_merges", ""};
  SEXP res = PROTECT(Rf_mkNamed(VECSXP, names));

  SEXP n_seen = PROTECT(Rf_ScalarLogical(it->second));
  SET_VECTOR_ELT(res, 0, n_seen);

  SEXP s_size = PROTECT(Rf_ScalarInteger(meta.size));
  SET_VECTOR_ELT(res, 1, s_size);

  SEXP n_calls = PROTECT(Rf_ScalarInteger(meta.n_calls));
  SET_VECTOR_ELT(res, 2, n_calls);

  SEXP s_type = PROTECT(Rf_ScalarInteger(meta.sexptype));
  SET_VECTOR_ELT(res, 3, s_type);

  SEXP first_seen_dur = PROTECT(Rf_ScalarInteger(meta.first_seen_dur.count()));
  SET_VECTOR_ELT(res, 4, first_seen_dur);

  SEXP next_seen_dur = PROTECT(Rf_ScalarInteger(meta.next_seen_dur.count()));
  SET_VECTOR_ELT(res, 5, next_seen_dur);

  SEXP n_merges = PROTECT(Rf_ScalarInteger(meta.n_merges));
  SET_VECTOR_ELT(res, 6, n_merges);

  UNPROTECT(8);

  return res;
}

SEXP GenericStore::get_metadata(SEXP val) const {
  const std::vector<std::byte>& buf = ser.serialize(val);

  sexp_hash key  = XXH3_128bits(buf.data(), buf.size());




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


  const char*names[] = {"newly_seen", "size", "n", "type", "first_seen_dur", "next_seen_dur", "n_merges", """"};
  SEXP res = PROTECT(Rf_mkNamed(VECSXP, names));

  SEXP n_seen = PROTECT(Rf_ScalarLogical(it->second));
  SET_VECTOR_ELT(res, 0, n_seen);

  SEXP s_size = PROTECT(Rf_ScalarInteger(meta.size));
  SET_VECTOR_ELT(res, 1, s_size);

  SEXP n_calls = PROTECT(Rf_ScalarInteger(meta.n_calls));
  SET_VECTOR_ELT(res, 2, n_calls);

  SEXP s_type = PROTECT(Rf_ScalarInteger(meta.sexptype));
  SET_VECTOR_ELT(res, 3, s_type);

  SEXP first_seen_dur = PROTECT(Rf_ScalarInteger(meta.first_seen_dur.count()));
  SET_VECTOR_ELT(res, 4, first_seen_dur);

  SEXP next_seen_dur = PROTECT(Rf_ScalarInteger(meta.next_seen_dur.count()));
  SET_VECTOR_ELT(res, 5, next_seen_dur);

  SEXP n_merges = PROTECT(Rf_ScalarInteger(meta.n_merges));
  SET_VECTOR_ELT(res, 6, n_merges);

  UNPROTECT(8);

  return res;
}


void GenericStore::write_metadata() {
  fs::path meta_path = fs::absolute(configuration_path).parent_path().append(metadata_name);
  std::ofstream meta_file(meta_path, std::fstream::out | std::fstream::binary | std::fstream::trunc);

  meta_file.exceptions(std::fstream::failbit);

  XXH128_canonical_t hash_buf;

  // we cannot just append the new ones, because n_calls might have changed
  // TODO: we could patch in n_calls though
  // it would require to store the offset of the metadata in the file, and then
  // seek to it to modify in place
  for(auto& it: metadata) {
    // write the hash
    meta_file.write(reinterpret_cast<const char*>(&it.first.low64), sizeof(it.first.low64));
    meta_file.write(reinterpret_cast<const char*>(&it.first.high64), sizeof(it.first.high64));
    // And now the metadata
    meta_file.write(reinterpret_cast<char*>(&it.second.n_calls), sizeof(it.second.n_calls));
    meta_file.write(reinterpret_cast<char*>(&it.second.size), sizeof(it.second.size));
    meta_file.write(reinterpret_cast<char*>(&it.second.sexptype), sizeof(it.second.sexptype));
    meta_file.write(reinterpret_cast<char*>(&it.second.first_seen_dur), sizeof(it.second.first_seen_dur));
    meta_file.write(reinterpret_cast<char*>(&it.second.next_seen_dur), sizeof(it.second.next_seen_dur));
    meta_file.write(reinterpret_cast<char*>(&it.second.n_merges), sizeof(it.second.n_merges));
  }
}

void GenericStore::load_metadata() {
  fs::path meta_path = fs::absolute(configuration_path).parent_path().append(metadata_name);
  std::ifstream meta_file(meta_path, std::fstream::out | std::fstream::binary);

  meta_file.exceptions(std::fstream::failbit);

  metadata.reserve(n_values);

  sexp_hash hash;

  metadata_t meta;

  for(uint64_t i = 0; i < n_values ; i++) {
    auto pos = meta_file.tellg();

    meta_file.read(reinterpret_cast<char*>(&hash.low64), sizeof(hash.low64));
    meta_file.read(reinterpret_cast<char*>(&hash.high64), sizeof(hash.high64));

    meta_file.read(reinterpret_cast<char*>(&meta.n_calls), sizeof(meta.n_calls));
    meta_file.read(reinterpret_cast<char*>(&meta.size), sizeof(meta.size));
    meta_file.read(reinterpret_cast<char*>(&meta.sexptype), sizeof(meta.sexptype));
    meta_file.read(reinterpret_cast<char*>(&meta.first_seen_dur), sizeof(meta.first_seen_dur));
    meta_file.read(reinterpret_cast<char*>(&meta.next_seen_dur), sizeof(meta.next_seen_dur));
    meta_file.read(reinterpret_cast<char*>(&meta.n_merges), sizeof(meta.n_merges));

    metadata[hash] = meta;

    bytes_read += meta_file.tellg() - pos;
    assert(meta_file.tellg() - pos == sizeof(hash) + sizeof(meta.n_calls) + sizeof(meta.size) + sizeof(meta.sexptype));
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
          Rf_error("Two values with same hash with different types %s and %s\n",
                   Rf_type2char(it->second.sexptype),
                   Rf_type2char(val.second.sexptype));
        }
        if(it->second.size != val.second.size) {
          Rf_error("Two values with same hash with different sizes %s and %s\n",
                   it->second.size,
                   val.second.size);
        }

        // update the number of calls
        it->second.n_calls += val.second.n_calls;
        // Update the number of merges
        it->second.n_merges++;
        // average the first seen time and the next seen time
        it->second.first_seen_dur = (it->second.first_seen_dur * it->second.n_merges +
          val.second.first_seen_dur * (val.second.n_merges + 1) ) /
            (it->second.n_merges + val.second.n_merges + 1);
        it->second.next_seen_dur = (it->second.next_seen_dur * (it->second.n_calls - it->second.n_merges) +
          val.second.first_seen_dur * (it->second.n_calls - val.second.n_merges - 1) ) /
            (it->second.n_calls + val.second.n_calls - it->second.n_merges - val.second.n_merges + 1);
        new_elements = true;
      }
  }


  return res;
}

bool GenericStore::merge_in(Store& store) {
  GenericStore* st = dynamic_cast<GenericStore*>(&store);
  if(st == nullptr) {
    Rf_warning("Cannot merge a store with kind %s with a store of kind %s\n", store_kind().c_str(), store.store_kind().c_str());
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
