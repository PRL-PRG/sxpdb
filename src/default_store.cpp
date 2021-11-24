#include "default_store.h"

#include <unordered_map>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <locale>

#include <chrono>


#include <cassert>

#include "sha1.h"


DefaultStore::DefaultStore(const fs::path& config_path) :
  Store(""),
  configuration_path(config_path), bytes_read(0), ser(256),
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

  Config config(std::move(conf));
  config.write(configuration_path);
}

bool DefaultStore::load() {
  // We will just write at the end of the file (but might read before)
  fs::path index_path = fs::absolute(configuration_path).parent_path().append(index_name);
  fs::path store_path = fs::absolute(configuration_path).parent_path().append(store_name);

  index_file.open(index_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::app);//because we just add the new values
  store_file.open(store_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::app);
  if(!index_file) {
    std::cerr << "Failed to open index file " << index_name << " at path " << index_path << std::endl;
  }
  if(!store_file) {
    std::cerr << "Failed to open store file " << store_name << " at path " << store_path << std::endl;
    exit(1);
  }
  if(!index_file) exit(1);

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
  assert(hash.size() == 20);
  size_t offset = 0;

  for(size_t i = 0; i < n_values ; i++) {
    index_file.read(hash.data(), hash.size());
    index_file.read(reinterpret_cast<char*>(&offset), sizeof(size_t));
    index[hash] = offset;

    bytes_read += 20 + sizeof(offset);
  }
}


void DefaultStore::write_index() {
  // Only write values that were newly discovered during this run
  // The old one can keep living happily at the beginning of the file!
  for(auto& it : index) {
    auto not_seen = newly_seen[it.first];
    if(not_seen) {
      index_file.write(it.first.data(), it.first.size());
      index_file.write(reinterpret_cast<char*>(&it.second), sizeof(size_t));
      newly_seen[it.first] = false;// now written so should not be written next time
    }
  }
}

std::pair<const sexp_hash*, bool> DefaultStore::add_value(SEXP val) {
  auto start = std::chrono::high_resolution_clock::now();

  sexp_hash* key = cached_hash(val);
  const std::vector<std::byte>*  buf = nullptr;

  if(key == nullptr) {
    const std::vector<std::byte>& buffer = ser.serialize(val);
    key = compute_cached_hash(val, buffer);
    buf= &buffer;
  }

  auto it = index.find(*key);
  if(it == index.end()) { // the value is not in the database
    assert(buf != nullptr); // If the value was deemed as "cached", it means that it should be in the database already

    // add it to the index
    auto res = index.insert(std::make_pair(*key, store_file.tellp()));

    //write the value
    size_t size = buf->size();
    store_file.write(reinterpret_cast<char*>(&size), sizeof(size_t));
    store_file.write(reinterpret_cast<const char*>(buf->data()), buf->size());

    // new value in that session
    newly_seen[*key] = true;

    n_values++;
    new_elements = true;

    assert(index.size() == n_values);

    // We only record the time of inserting the value (which means that if the value is already there...)
    auto end = std::chrono::high_resolution_clock::now();
    // Welford's algorithm for the average (numerically stable)
    add_time += (std::chrono::duration_cast<std::chrono::microseconds>(end - start) - add_time) / n_values;

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

  auto it = sexp_adresses.find(val);

  if(it != sexp_adresses.end() && RTRACE(val)) {
    return &it->second;
  }

  return nullptr;
}

const sexp_hash DefaultStore::compute_hash(SEXP val) const {
  const std::vector<std::byte>& buf = ser.serialize(val);

  sexp_hash ser_hash;
  sha1_context ctx;
  sha1_starts(&ctx);
  sha1_update(&ctx,  reinterpret_cast<uint8*>(const_cast<std::byte*>(buf.data())), buf.size());
  sha1_finish(&ctx, reinterpret_cast<uint8*>(ser_hash.data()));

  return ser_hash;
}

sexp_hash* const DefaultStore::compute_cached_hash(SEXP val, const std::vector<std::byte>& buf) const {
    sexp_hash ser_hash;
    sha1_context ctx;
    sha1_starts(&ctx);
    sha1_update(&ctx,  reinterpret_cast<uint8*>(const_cast<std::byte*>(buf.data())), buf.size());
    sha1_finish(&ctx, reinterpret_cast<uint8*>(ser_hash.data()));

    auto res = sexp_adresses.insert(std::make_pair(val, ser_hash));

    assert(res.second);

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
  size_t offset = it2->second;

  store_file.seekg(offset);
  size_t size = 0;
  store_file.read(reinterpret_cast<char*>(&size), sizeof(size_t));
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

SEXP DefaultStore::get_metadata(size_t idx) const {
  auto it = index.begin();
  std::advance(it, idx);
  size_t offset = it->second;

  auto it2 = newly_seen.find(it->first);

  if(it2 == newly_seen.end()) {
    return R_NilValue;
  }

  store_file.seekg(offset);
  size_t size = 0;
  store_file.read(reinterpret_cast<char*>(&size), sizeof(size_t));
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


SEXP DefaultStore::get_value(size_t idx) {
  auto it = index.begin();
  std::advance(it, idx);
  size_t offset = it->second;

  store_file.seekg(offset);
  size_t size = 0;
  store_file.read(reinterpret_cast<char*>(&size), sizeof(size_t));
  assert(size> 0);

  std::vector<std::byte> buf(size);//or expose the internal buffer of the serializer?
  store_file.read(reinterpret_cast<char*>(buf.data()), size);

  return ser.unserialize(buf);
}

const sexp_hash& DefaultStore::get_hash(size_t idx) const {
  auto it = index.begin();
  std::advance(it, idx);

  return it->first;
}

SEXP DefaultStore::sample_value() {
  std::uniform_int_distribution<size_t> dist(0, n_values - 1);

  return get_value(dist(rand_engine));
}

std::chrono::microseconds DefaultStore::avg_insertion_duration() const {
  return add_time;
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
      size_t offset = it.second;
      other.store_file.seekg(offset);
      size_t size = 0;
      other.store_file.read(reinterpret_cast<char*>(&size), sizeof(size_t));
      assert(size> 0);

      buf.resize(size);
      other.store_file.read(reinterpret_cast<char*>(buf.data()), size);

      // add it to the index
      index[it.first] = store_file.tellp();

      //write the value
      store_file.write(reinterpret_cast<char*>(&size), sizeof(size_t));
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
    Rf_warning("Cannot merge a store with kind %s with a store of kind", store_kind().c_str(), store.store_kind().c_str());
    return false;
  }

  return merge_in(*st);
}


