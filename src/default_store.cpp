#include "default_store.h"

#include <unordered_map>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <locale>

#include <chrono>


#include <cassert>

#include "sha1.h"
#include "xxhash.h"


DefaultStore::DefaultStore(const fs::path& config_path) :
  Store(""),
  configuration_path(config_path), bytes_read(0), ser(32768),
  rand_engine(std::chrono::system_clock::now().time_since_epoch().count())
{

  if(std::filesystem::exists(config_path)) {
    Config config(configuration_path);
    type = config["type"];
    index_name = config["index"];
    store_name = config["store"];
    metadata_name = config["metadata"];
    n_values = std::stoul(config["nb_values"]);
    set_kind(config["kind"]);
    load();
  }
}

DefaultStore::DefaultStore(const fs::path& config_path, const std::string& type_) :
  Store("default"),
  configuration_path(config_path.parent_path().append(config_path.filename().string() + ".conf")), bytes_read(0), ser(256), type(type_),
  index_name(config_path.filename().string() + "_index.bin"), store_name(config_path.filename().string() + "_store.bin"),
  metadata_name(config_path.filename().string() + "_meta.bin"), n_values(0),
  rand_engine(std::chrono::system_clock::now().time_since_epoch().count()) {

  // it means we want to create the database!

  create();
}

void DefaultStore::create() {
  load();
  write_configuration();
}

DefaultStore::~DefaultStore() {
  if(new_elements || n_values == 0) {
    write_index();
    write_configuration();
  }
}

void DefaultStore::write_configuration() {
  //Save the configuration
  std::unordered_map<std::string, std::string> conf;
  conf["type"] = type;
  conf["index"] = index_name;
  conf["store"] = store_name;
  conf["metadata"] = metadata_name;
  conf["nb_values"] = std::to_string(n_values);
  conf["kind"] = store_kind();
  conf["compilation_time"] = std::string(__DATE__) + ":" + __TIME__;

  Config config(std::move(conf));
  config.write(configuration_path);
}

bool DefaultStore::load() {
  // We will just write at the end of the file (but might read before)
  fs::path index_path = fs::absolute(configuration_path).parent_path().append(index_name);
  fs::path store_path = fs::absolute(configuration_path).parent_path().append(store_name);

  index_file.open(index_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::app);//because we just add the new values
  store_file.open(store_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::app);

  std::ostringstream out;
  if(!index_file) {
    out << "Failed to open index file " << index_name << " at path " << index_path << std::endl;
  }
  if(!store_file) {
    out << "Failed to open store file " << store_name << " at path " << store_path << std::endl;
  }
  if(!index_file || !store_file)  {
    Rf_error(out.str().c_str());
  }

  index_file.exceptions(std::fstream::failbit);
  store_file.exceptions(std::fstream::failbit);

  newly_seen.reserve(n_values);

  for(auto& it : index) {
    newly_seen[it.first] = false;
  }

  load_index();


  return true;
}


void DefaultStore::load_index() {
  index.reserve(n_values);
  sexp_hash hash;
  uint64_t offset = 0;

  for(uint64_t i = 0; i < n_values ; i++) {
    index_file.read(reinterpret_cast<char*>(&hash.low64), sizeof(hash.low64));
    index_file.read(reinterpret_cast<char*>(&hash.high64), sizeof(hash.high64));

    index_file.read(reinterpret_cast<char*>(&offset), sizeof(uint64_t));

    index[hash] = offset;

    bytes_read += 20 + sizeof(uint64_t);
  }
}


void DefaultStore::write_index() {
  // Only write values that were newly discovered during this run
  // The old one can keep living happily at the beginning of the file!
  for(auto& it : index) {
    auto not_seen = newly_seen[it.first];
    if(not_seen) {
      index_file.write(reinterpret_cast<const char*>(&it.first.low64), sizeof(it.first.low64));
      index_file.write(reinterpret_cast<const char*>(&it.first.high64), sizeof(it.first.high64));
      index_file.write(reinterpret_cast<char*>(&it.second), sizeof(uint64_t));
      newly_seen[it.first] = false;// now written so should not be written next time
    }
  }
}

std::pair<const sexp_hash*, bool> DefaultStore::add_value(SEXP val) {
  sexp_hash* key = cached_hash(val);
  const std::vector<std::byte>*  buf = nullptr;

#ifndef NDEBUG
  bool sexp_address_optim = key != nullptr;
#endif

  if(key == nullptr) {
    const std::vector<std::byte>& buffer = ser.serialize(val);
    key = compute_cached_hash(val, buffer);
    buf= &buffer;
  }

#ifndef NDEBUG
  auto debug_it = debug_counters.find(*key);
  if(debug_it != debug_counters.end()) {
    debug_it->second.n_maybe_shared += maybe_shared(val);
    debug_it->second.n_sexp_address_opt += sexp_address_optim;
  }
  else {
    debug_counters_t d_counters;
    d_counters.n_maybe_shared = maybe_shared(val);
    d_counters.n_sexp_address_opt = sexp_address_optim;
    debug_counters.insert(std::make_pair(*key, d_counters));
  }
#endif


  auto it = index.find(*key);
  if(it == index.end()) { // the value is not in the database
    assert(buf != nullptr); // If the value was deemed as "cached", it means that it should be in the database already

    // add it to the index
    auto res = index.insert(std::make_pair(*key, store_file.tellp()));

    //write the value
    uint64_t size = buf->size();
    store_file.write(reinterpret_cast<char*>(&size), sizeof(uint64_t));
    store_file.write(reinterpret_cast<const char*>(buf->data()), buf->size());

    // new value in that session
    newly_seen[*key] = true;

    n_values++;
    new_elements = true;

    assert(index.size() == n_values);

    return std::make_pair(&res.first->first, true);
  }


  return std::make_pair(&it->first, false);
}



sexp_hash* const DefaultStore::cached_hash(SEXP val) const {
  // Check if we already know the address of that SEXP
  // R has copy semantics except for environments
  if(TYPEOF(val) == ENVSXP) {
    return nullptr;
  }

  // Don't even do a lookup in that case
  // if the tracing bit is not set, we have not seen the value for sure
  // if the value is not shared, then its content can be modified in place
  // so we should hash the value again anyway
  if(!maybe_shared(val) || RTRACE(val) == 0 ) {
    return nullptr;
  }

  auto it = sexp_adresses.find(val);

  if(it != sexp_adresses.end()) {
    return &it->second;
  }

  return nullptr;
}

const sexp_hash DefaultStore::compute_hash(SEXP val) const {
  const std::vector<std::byte>& buf = ser.serialize(val);

  sexp_hash ser_hash  = XXH3_128bits(buf.data(), buf.size());

  return ser_hash;
}

sexp_hash* const DefaultStore::compute_cached_hash(SEXP val, const std::vector<std::byte>& buf) const {
    sexp_hash ser_hash  = XXH3_128bits(buf.data(), buf.size());

    auto res = sexp_adresses.insert_or_assign(val, ser_hash);

    SET_RTRACE(val, 1);// set the tracing bit to show later that it is a value we actually touched

    return &res.first->second;
}


bool DefaultStore::have_seen(SEXP val) const {
  sexp_hash* key = cached_hash(val);

  if(key == nullptr) {
    *key = compute_hash(val);
  }

  return index.find(*key) == index.end();
}

SEXP DefaultStore::get_metadata(SEXP val) const {
  sexp_hash* key = cached_hash(val);

  if(key == nullptr) {
    *key = compute_hash(val);
  }

  auto it = newly_seen.find(*key);

  if(it == newly_seen.end()) {
    return R_NilValue;
  }

  auto it2 = index.find(*key);
  uint64_t offset = it2->second;

  store_file.seekg(offset);
  uint64_t size = 0;
  store_file.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
  assert(size> 0);

  const char*names[] = {"newly_seen", "size", ""};
  SEXP res = PROTECT(Rf_mkNamed(VECSXP, names));

  SEXP n_seen = PROTECT(Rf_ScalarLogical(it2->second));
  SET_VECTOR_ELT(res, 0, n_seen);

  SEXP s_size = PROTECT(Rf_ScalarInteger(size));
  SET_VECTOR_ELT(res, 1, s_size);

  UNPROTECT(3);

  return res;
}

SEXP DefaultStore::get_metadata(uint64_t idx) const {
  auto it = index.begin();
  std::advance(it, idx);
  uint64_t offset = it->second;

  auto it2 = newly_seen.find(it->first);

  if(it2 == newly_seen.end()) {
    return R_NilValue;
  }

  store_file.seekg(offset);
  uint64_t size = 0;
  store_file.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
  assert(size> 0);

  const char*names[] = {"newly_seen", "size", ""};
  SEXP res = PROTECT(Rf_mkNamed(VECSXP, names));

  SEXP n_seen = PROTECT(Rf_ScalarLogical(it2->second));
  SET_VECTOR_ELT(res, 0, n_seen);

  SEXP s_size = PROTECT(Rf_ScalarInteger(size));
  SET_VECTOR_ELT(res, 1, s_size);

  UNPROTECT(3);

  return res;
}


SEXP DefaultStore::get_value(uint64_t idx) {
  auto it = index.begin();
  std::advance(it, idx);
  uint64_t offset = it->second;

  store_file.seekg(offset);
  uint64_t size = 0;
  store_file.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
  assert(size> 0);

  std::vector<std::byte> buf(size);//or expose the internal buffer of the serializer?
  store_file.read(reinterpret_cast<char*>(buf.data()), size);

  return ser.unserialize(buf);
}

const std::vector<size_t> DefaultStore::check() {
  std::vector<std::byte> buf;
  buf.reserve(128); // the minimum serialized size is about 35 bytes.
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

  }

  return std::vector<size_t>();
}

SEXP const DefaultStore::map(const SEXP function) {
  std::vector<std::byte> buf;
  buf.reserve(128);

  // Build the call
  SEXP call = PROTECT(Rf_lang2(function, Rf_install("unserialized_sxpdb_value")));
  SEXP l = PROTECT(Rf_allocVector(VECSXP, nb_values()));

  // Prepare un environment where we will put the unserialized value
#if defined(R_VERSION) && R_VERSION >= R_Version(4, 1, 0)
  SEXP env = R_NewEnv(R_GetCurrentEnv(), TRUE, 1);
#else
  SEXP env = Rf_eval(Rf_lang1(Rf_install("new.env")), R_GetCurrentEnv());
#endif

  R_xlen_t i = 0;
  for(auto it : index) {
    uint64_t offset = it.second;

    store_file.seekg(offset);
    uint64_t size = 0;
    store_file.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
    assert(size> 0);

    buf.resize(size);
    store_file.read(reinterpret_cast<char*>(buf.data()), size);

    SEXP val = ser.unserialize(buf);// no need to protect as it is going to be bound just after

    // Update the argument for the next call
    Rf_defineVar(Rf_install("unserialized_sxpdb_value"), val, env);

    // Perform the call
    SEXP res = Rf_eval(call, env);


    SET_VECTOR_ELT(l, i, res);

    i++;
  }

  UNPROTECT(2);

  return l;
}

const sexp_hash& DefaultStore::get_hash(uint64_t idx) const {
  auto it = index.begin();
  std::advance(it, idx);

  return it->first;
}

SEXP DefaultStore::sample_value() {
  std::uniform_int_distribution<uint64_t> dist(0, n_values - 1);

  return get_value(dist(rand_engine));
}



bool DefaultStore::merge_in(DefaultStore& other) {
  if(other.sexp_type() != sexp_type()) {
    // we can only merge databases of the same type
    return false;
  }
  std::vector<std::byte> buf;
  for(auto& it: other.index) {
    //Check if the hash is not in the index
    if(index.find(it.first) == index.end()) {
      // read the value from other
      uint64_t offset = it.second;
      other.store_file.seekg(offset);
      uint64_t size = 0;
      other.store_file.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
      assert(size> 0);

      buf.resize(size);
      other.store_file.read(reinterpret_cast<char*>(buf.data()), size);

      // add it to the index
      index[it.first] = store_file.tellp();

      //write the value
      store_file.write(reinterpret_cast<char*>(&size), sizeof(uint64_t));
      store_file.write(reinterpret_cast<const char*>(buf.data()), buf.size());

      n_values++;
      new_elements = true;
      newly_seen[it.first] = true;

    }
  }
  assert(index.size() == n_values);
  write_index();

  return true;
}

bool DefaultStore::merge_in(Store& store) {
  DefaultStore* st = dynamic_cast<DefaultStore*>(&store);
  if(st == nullptr) {
    Rf_warning("Cannot merge a store with kind %s with a store of kind %s\n", store_kind().c_str(), store.store_kind().c_str());
    return false;
  }

  return merge_in(*st);
}


