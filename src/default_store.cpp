#include "default_store.h"

#include <unordered_map>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <locale>
#include <cassert>
#include <chrono>

#include "sha1.h"

// from boost::hash_combine
void hash_combine(std::size_t& seed, std::size_t value) {
  seed ^= value + 0x9e3779b9 + (seed<<6) + (seed>>2);
}


DefaultStore::DefaultStore(const fs::path& config_path) :
  configuration_path(config_path), bytes_read(0), ser(256),
  rand_engine(std::chrono::system_clock::now().time_since_epoch().count())
{
  Config config(configuration_path);
  type = config["type"];
  index_name = config["index"];
  store_name = config["store"];
  metadata_name = config["metadata"];
  n_values = std::stoul(config["nb_values"]);
  load();
}

DefaultStore::DefaultStore(const fs::path& config_path, const std::string& type_) :
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
  write_configuration();
}

void DefaultStore::write_configuration() {
  //Save the configuration
  std::unordered_map<std::string, std::string> conf;
  conf["type"] = type;
  conf["index"] = index_name;
  conf["store"] = store_name;
  conf["metadata"] = metadata_name;
  conf["nb_values"] = std::to_string(n_values);

  Config config(std::move(conf));
  config.write(configuration_path);
}

bool DefaultStore::load() {
  // We will just write at the end of the file (but might read before)
  fs::path index_path = fs::absolute(configuration_path).parent_path().append(index_name);
  fs::path store_path = fs::absolute(configuration_path).parent_path().append(store_name);

  index_file.open(index_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::app);
  store_file.open(store_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::app);
  if(!index_file) {
    std::cerr << "Failed to open index file " << index_name << std::endl;
  }
  if(!store_file) {
    std::cerr << "Failed to open store file " << store_name << std::endl;
    exit(1);
  }
  if(!index_file) exit(1);

  index_file.exceptions(std::fstream::failbit);
  store_file.exceptions(std::fstream::failbit);

  load_index();

  return true;
}


void DefaultStore::load_index() {
  index.reserve(n_values);
  size_t start = 0;
  std::array<char, 20> hash;
  size_t offset = 0;

  for(size_t i = 0; i < n_values ; i++) {
    index_file.read(hash.data(), 20);
    index_file.read(reinterpret_cast<char*>(&offset), sizeof(offset));
    index[hash] = offset;

    bytes_read += 20 + sizeof(offset);
  }
}

void DefaultStore::load_metadata() {
  newly_seen.reserve(n_values);

  for(auto& it : index) {
    newly_seen[it.first] = false;
  }
}

void DefaultStore::write_index() {
  // Only write values that were newly discovered during this run
  // The old one can keep living happily at the beginning of the file!
  for(auto& it : index) {
    auto not_seen = newly_seen[it.first];
    if(not_seen) {
      index_file.write(it.first.data(), it.first.size());
      index_file.write(reinterpret_cast<char*>(&it.second), sizeof(it.second));
    }
  }
}

bool DefaultStore::add_value(SEXP val) {
  const std::vector<std::byte>& buf = ser.serialize(val);

  std::array<char, 20> key;
  sha1_context ctx;
  sha1_starts(&ctx);
  sha1_update(&ctx,  reinterpret_cast<uint8*>(const_cast<std::byte*>(buf.data())), buf.size());
  sha1_finish(&ctx, reinterpret_cast<uint8*>(key.data()));

  auto it = index.find(key);
  if(it == index.end()) { // the value is not in the database
    // add it to the index
    index[key] = store_file.tellp();

    //write the value
    size_t size = buf.size();
    store_file.write(reinterpret_cast<char*>(&size), sizeof(size_t));
    store_file.write(reinterpret_cast<const char*>(buf.data()), buf.size());

    // new value in that session
    newly_seen[key] = true;

    n_values++;

    return true;
  }

  return false;
}


bool DefaultStore::have_seen(SEXP val) const {
  const std::vector<std::byte>& buf = ser.serialize(val);

  std::array<char, 20> key;
  sha1_context ctx;
  sha1_starts(&ctx);
  sha1_update(&ctx,  reinterpret_cast<uint8*>(const_cast<std::byte*>(buf.data())), buf.size());
  sha1_finish(&ctx, reinterpret_cast<uint8*>(key.data()));

  return index.find(key) != index.end();
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

SEXP DefaultStore::sample_value() {
  std::uniform_int_distribution<size_t> dist(0, n_values - 1);

  return get_value(dist(rand_engine));
}


bool DefaultStore::merge_in(DefaultStore& other) {
  if(other.sexp_type() != sexp_type()) {
    // we can only merge databases of the same type
    return false;
  }

  for(auto& it: other.index) {
    //Check if the hash is not in the index
    if(index.find(it.first) == index.end()) {
      // read the value from other
      size_t offset = it.second;
      other.store_file.seekg(offset);
      size_t size = 0;
      other.store_file.read(reinterpret_cast<char*>(&size), sizeof(size_t));
      assert(size> 0);
      std::vector<std::byte> buf;
      other.store_file.read(reinterpret_cast<char*>(buf.data()), size);

      // add it to the index
      index[it.first] = store_file.tellp();

      //write the value
      store_file.write(reinterpret_cast<char*>(&size), sizeof(size_t));
      store_file.write(reinterpret_cast<const char*>(buf.data()), buf.size());

      n_values++;
      newly_seen[it.first] = true;

    }
  }

  return true;
}

SEXP DefaultStore::get_metadata(SEXP val) const {
  const std::vector<std::byte>& buf = ser.serialize(val);

  std::array<char, 20> key;
  sha1_context ctx;
  sha1_starts(&ctx);
  sha1_update(&ctx,  reinterpret_cast<uint8*>(const_cast<std::byte*>(buf.data())), buf.size());
  sha1_finish(&ctx, reinterpret_cast<uint8*>(key.data()));

  auto it = newly_seen.find(key);

  if(it == newly_seen.end()) {
    return R_NilValue;
  }

  const char*names[] = {"newly_seen"};
  SEXP res = PROTECT(Rf_mkNamed(VECSXP, names));
  SEXP n_seen = PROTECT(Rf_ScalarLogical(it->second));

  UNPROTECT(2);

  return res;
}
