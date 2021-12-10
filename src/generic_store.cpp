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
#ifdef SXPDB_TIMER_SER_HASH
  auto start = std::chrono::steady_clock::now();
#endif
  auto added =  DefaultStore::add_value(val);
#ifdef SXPDB_TIMER_SER_HASH
  auto end = std::chrono::steady_clock::now();
#endif

  if(!added.second) {// value already seen
    metadata_t& meta = metadata[*added.first];
#ifdef SXPDB_TIMER_SER_HASH
    // do not take into account the first insertion for the average
    meta.next_seen_dur += (std::chrono::duration_cast<std::chrono::nanoseconds>(end - start) - meta.next_seen_dur) / meta.n_calls;
#endif
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
    meta.length = Rf_length(val);
    meta.n_attributes = Rf_length(ATTRIB(val));
#ifdef SXPDB_TIMER_SER_HASH
    meta.first_seen_dur = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    meta.next_seen_dur =  std::chrono::nanoseconds::zero();
#endif
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


#ifndef NDEBUG
  auto it_dbg = debug_counters.find(key);
  assert(it_dbg != debug_counters.end());
  const debug_counters_t& debug_cnts = it_dbg->second;
#endif

#ifdef SXPDB_TIMER_SER_HASH
  const char*names[] = {"newly_seen", "size", "n", "type", "length", "n_attributes", "first_seen_dur",
                        "next_seen_dur", "n_merges",""};
#elif !defined(NDEBUG)
  const char*names[] = {"newly_seen", "size", "n", "type", "length", "n_attributes", "n_merges", "maybe_shared", "sexp_adr_optim", ""};
#else
  const char*names[] = {"newly_seen", "size", "n", "type",  "length", "n_attributes", "n_merges", ""};
#endif

  SEXP res = PROTECT(Rf_mkNamed(VECSXP, names));

  SEXP n_seen = PROTECT(Rf_ScalarLogical(it->second));
  SET_VECTOR_ELT(res, 0, n_seen);

  SEXP s_size = PROTECT(Rf_ScalarInteger(meta.size));
  SET_VECTOR_ELT(res, 1, s_size);

  SEXP n_calls = PROTECT(Rf_ScalarInteger(meta.n_calls));
  SET_VECTOR_ELT(res, 2, n_calls);

  SEXP s_type = PROTECT(Rf_ScalarInteger(meta.sexptype));
  SET_VECTOR_ELT(res, 3, s_type);

  SEXP s_length = PROTECT(Rf_ScalarInteger(meta.length));
  SET_VECTOR_ELT(res, 4, s_length);

  SEXP s_n_attrs = PROTECT(Rf_ScalarInteger(meta.n_attributes));
  SET_VECTOR_ELT(res, 5, s_n_attrs);

#ifdef SXPDB_TIMER_SER_HASH
  SEXP first_seen_dur = PROTECT(Rf_ScalarInteger(meta.first_seen_dur.count()));
  SET_VECTOR_ELT(res, 6, first_seen_dur);

  SEXP next_seen_dur = PROTECT(Rf_ScalarInteger(meta.next_seen_dur.count()));
  SET_VECTOR_ELT(res, 7, next_seen_dur);

  SEXP n_merges = PROTECT(Rf_ScalarInteger(meta.n_merges));
  SET_VECTOR_ELT(res, 8, n_merges);
#else
  SEXP n_merges = PROTECT(Rf_ScalarInteger(meta.n_merges));
  SET_VECTOR_ELT(res, 6, n_merges);
#endif

#if !defined(NDEBUG) && !defined(SXPDB_TIMER_SER_HASH)
  SEXP n_maybe_shared = PROTECT(Rf_ScalarInteger(debug_cnts.n_maybe_shared));
  SET_VECTOR_ELT(res, 7, n_maybe_shared);

  SEXP n_sexp_adr_optim = PROTECT(Rf_ScalarInteger(debug_cnts.n_sexp_address_opt));
  SET_VECTOR_ELT(res, 8, n_sexp_adr_optim);
#endif


#if defined(SXPDB_TIMER_SER_HASH) || !defined(NDEBUG)
  UNPROTECT(10);
#else
  UNPROTECT(8);
#endif

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

#ifndef NDEBUG
  auto it_dbg = debug_counters.find(key);
  assert(it_dbg != debug_counters.end());
  const debug_counters_t& debug_cnts = it_dbg->second;
#endif

#ifdef SXPDB_TIMER_SER_HASH
  const char*names[] = {"newly_seen", "size", "n", "type", "length", "n_attributes", "first_seen_dur",
                        "next_seen_dur", "n_merges",""};
#elif !defined(NDEBUG)
  const char*names[] = {"newly_seen", "size", "n", "type", "length", "n_attributes", "n_merges", "maybe_shared", "sexp_adr_optim", ""};
#else
  const char*names[] = {"newly_seen", "size", "n", "type",  "length", "n_attributes", "n_merges", ""};
#endif

  SEXP res = PROTECT(Rf_mkNamed(VECSXP, names));

  SEXP n_seen = PROTECT(Rf_ScalarLogical(it->second));
  SET_VECTOR_ELT(res, 0, n_seen);

  SEXP s_size = PROTECT(Rf_ScalarInteger(meta.size));
  SET_VECTOR_ELT(res, 1, s_size);

  SEXP n_calls = PROTECT(Rf_ScalarInteger(meta.n_calls));
  SET_VECTOR_ELT(res, 2, n_calls);

  SEXP s_type = PROTECT(Rf_ScalarInteger(meta.sexptype));
  SET_VECTOR_ELT(res, 3, s_type);

  SEXP s_length = PROTECT(Rf_ScalarInteger(meta.length));
  SET_VECTOR_ELT(res, 4, s_length);

  SEXP s_n_attrs = PROTECT(Rf_ScalarInteger(meta.n_attributes));
  SET_VECTOR_ELT(res, 5, s_n_attrs);

#ifdef SXPDB_TIMER_SER_HASH
  SEXP first_seen_dur = PROTECT(Rf_ScalarInteger(meta.first_seen_dur.count()));
  SET_VECTOR_ELT(res, 6, first_seen_dur);

  SEXP next_seen_dur = PROTECT(Rf_ScalarInteger(meta.next_seen_dur.count()));
  SET_VECTOR_ELT(res, 7, next_seen_dur);

  SEXP n_merges = PROTECT(Rf_ScalarInteger(meta.n_merges));
  SET_VECTOR_ELT(res, 8, n_merges);
#else
  SEXP n_merges = PROTECT(Rf_ScalarInteger(meta.n_merges));
  SET_VECTOR_ELT(res, 6, n_merges);
#endif

#if !defined(NDEBUG) && !defined(SXPDB_TIMER_SER_HASH)
  SEXP n_maybe_shared = PROTECT(Rf_ScalarInteger(debug_cnts.n_maybe_shared));
  SET_VECTOR_ELT(res, 7, n_maybe_shared);

  SEXP n_sexp_adr_optim = PROTECT(Rf_ScalarInteger(debug_cnts.n_sexp_address_opt));
  SET_VECTOR_ELT(res, 8, n_sexp_adr_optim);
#endif


#if defined(SXPDB_TIMER_SER_HASH) || !defined(NDEBUG)
  UNPROTECT(10);
#else
  UNPROTECT(8);
#endif

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
    meta_file.write(reinterpret_cast<char*>(&it.second.length), sizeof(it.second.length));
    meta_file.write(reinterpret_cast<char*>(&it.second.n_attributes), sizeof(it.second.n_attributes));
    meta_file.write(reinterpret_cast<char*>(&it.second.n_merges), sizeof(it.second.n_merges));
#ifdef SXPDB_TIMER_SER_HASH
    meta_file.write(reinterpret_cast<char*>(&it.second.first_seen_dur), sizeof(it.second.first_seen_dur));
    meta_file.write(reinterpret_cast<char*>(&it.second.next_seen_dur), sizeof(it.second.next_seen_dur));
#elif !defined(NDEBUG)
    auto debug_cnts = debug_counters[it.first];
    meta_file.write(reinterpret_cast<char*>(&debug_cnts.n_maybe_shared), sizeof(debug_cnts.n_maybe_shared));
    meta_file.write(reinterpret_cast<char*>(&debug_cnts.n_sexp_address_opt), sizeof(debug_cnts.n_sexp_address_opt));
#endif
  }
}

void GenericStore::load_metadata() {
  fs::path meta_path = fs::absolute(configuration_path).parent_path().append(metadata_name);
  std::ifstream meta_file(meta_path, std::fstream::out | std::fstream::binary);

  meta_file.exceptions(std::fstream::failbit);

  metadata.reserve(n_values);

  sexp_hash hash;

  metadata_t meta;

#ifndef NDEBUG
  debug_counters_t debug_cnts;
#endif

  for(uint64_t i = 0; i < n_values ; i++) {
    auto pos = meta_file.tellg();

    meta_file.read(reinterpret_cast<char*>(&hash.low64), sizeof(hash.low64));
    meta_file.read(reinterpret_cast<char*>(&hash.high64), sizeof(hash.high64));

    meta_file.read(reinterpret_cast<char*>(&meta.n_calls), sizeof(meta.n_calls));
    meta_file.read(reinterpret_cast<char*>(&meta.size), sizeof(meta.size));
    meta_file.read(reinterpret_cast<char*>(&meta.sexptype), sizeof(meta.sexptype));
    meta_file.read(reinterpret_cast<char*>(&meta.length), sizeof(meta.length));
    meta_file.read(reinterpret_cast<char*>(&meta.n_attributes), sizeof(meta.n_attributes));
    meta_file.read(reinterpret_cast<char*>(&meta.n_merges), sizeof(meta.n_merges));
#ifdef SXPDB_TIMER_SER_HASH
    meta_file.read(reinterpret_cast<char*>(&meta.first_seen_dur), sizeof(meta.first_seen_dur));
    meta_file.read(reinterpret_cast<char*>(&meta.next_seen_dur), sizeof(meta.next_seen_dur));
#elif !defined(NDEBUG)
    meta_file.read(reinterpret_cast<char*>(&debug_cnts.n_maybe_shared), sizeof(debug_cnts.n_maybe_shared));
    meta_file.read(reinterpret_cast<char*>(&debug_cnts.n_sexp_address_opt), sizeof(debug_cnts.n_sexp_address_opt));
    debug_counters[hash] = debug_cnts;
#endif

    metadata[hash] = meta;

    bytes_read += meta_file.tellg() - pos;
    //assert(meta_file.tellg() - pos == sizeof(hash) + sizeof(meta.n_calls) + sizeof(meta.size) + sizeof(meta.sexptype));
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
          Rf_error("Two values with same hash with different sizes %ld and %ld\n",
                   it->second.size,
                   val.second.size);
        }
        if(it->second.length != val.second.length) {
          Rf_error("Two values with same hash with different lengths %ld and %ld\n",
                   it->second.length,
                   val.second.length);
        }
        if(it->second.n_attributes != val.second.n_attributes) {
          Rf_error("Two values with same hash with different number of attributes %ld and %ld\n",
                   it->second.n_attributes,
                   val.second.n_attributes);
        }

        // update the number of calls
        it->second.n_calls += val.second.n_calls;
        // Update the number of merges
        it->second.n_merges++;
#ifdef SXPDB_TIMER_SER_HASH
        // average the first seen time and the next seen time
        it->second.first_seen_dur = (it->second.first_seen_dur * it->second.n_merges +
          val.second.first_seen_dur * (val.second.n_merges + 1) ) /
            (it->second.n_merges + val.second.n_merges + 1);
        it->second.next_seen_dur = (it->second.next_seen_dur * (it->second.n_calls - it->second.n_merges) +
          val.second.first_seen_dur * (it->second.n_calls - val.second.n_merges - 1) ) /
            (it->second.n_calls + val.second.n_calls - it->second.n_merges - val.second.n_merges + 1);
#endif
        new_elements = true;
      }
  }


  return res;
}

const std::vector<size_t> GenericStore::check(bool slow_check) {
  std::vector<size_t> errors;
  std::vector<std::byte> buf;
  buf.reserve(128); // the minimum serialized size is about 35 bytes.
  size_t idx = 0;
  for(auto it : index) {
    uint64_t offset = it.second;

    store_file.seekg(offset);
    uint64_t size = 0;
    store_file.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
    assert(size> 0);

    buf.resize(size);
    store_file.read(reinterpret_cast<char*>(buf.data()), size);

    // Try to unserialize
    // TODO: wrap it in a tryCatch...
    SEXP val = ser.unserialize(buf);

    // Compare with the metadata

    auto meta_it = metadata.find(it.first);

    bool error = false;

    if(meta_it != metadata.end()) {
        auto meta = meta_it->second;


        if(TYPEOF(val) != meta.sexptype) {
          Rf_warning("Types do not match for value %ld with hash low: %ld, high: %ld: %s versus %s\n", idx,
                     it.first.low64, it.first.high64, Rf_type2char(TYPEOF(val)), Rf_type2char(meta.sexptype));
          error= true;
        }

        if(buf.size() != meta.size) {
          Rf_warning("Sizes do not match for value %d with hash low: %ld, high: %ld: %ld versus %ld\n", idx,
                     it.first.low64, it.first.high64, buf.size(), meta.size);
          error= true;
        }

        if(Rf_length(val) != meta.length) {
          Rf_warning("Lengths do not match for value %d with hash low: %ld, high: %ld: %ld versus %ld\n", idx,
                     it.first.low64, it.first.high64, Rf_length(val), meta.length);
          error= true;
        }

        if(Rf_length(ATTRIB(val)) != meta.n_attributes) {
          Rf_warning("Number of attributes do not match for value %d with hash low: %ld, high: %ld: %ld versus %ld\n", idx,
                     it.first.low64, it.first.high64, Rf_length(ATTRIB(val)), meta.n_attributes);
          error= true;
        }


    } else {
      Rf_warning("Missing metadata for value %d with hash low: %ld, high: %ld\n", idx, it.first.low64, it.first.high64);
      error = true;
    }

    if(slow_check) {
      // try to serialize the data back and check whether the hashes are equal or not

      auto ser_buf = ser.serialize(val);
      sexp_hash ser_hash  = XXH3_128bits(ser_buf.data(), ser_buf.size());

      if(!(ser_hash == it.first)) {
          Rf_warning("Serialized and deserialized values do not match for value %ld with hash low: %ld, high: %ld", idx,  it.first.low64, it.first.high64);
          error = true;
      }
    }


    if(error) {
      errors.push_back(idx);
    }

    idx++;
  }

  return errors;
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
