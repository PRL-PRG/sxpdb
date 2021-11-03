#include "default_store.h"

#include <unordered_map>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <locale>

#include "sha1.h"

// from boost::hash_combine
void hash_combine(std::size_t& seed, std::size_t value) {
  seed ^= value + 0x9e3779b9 + (seed<<6) + (seed>>2);
}


DefaultStore::DefaultStore(const std::string& description_name) :
  description_name(description_name), bytes_read(0), ser(256)
{
  Config config(description_name);
  type = config["type"];
  index_name = config["index"];
  store_name = config["store"];
  metadata_name = config["metadata"];
  n_values = std::stoul(config["nb_values"]);
  load();
}


DefaultStore::~DefaultStore() {

  //Save the configuration
  std::unordered_map<std::string, std::string> conf;
  conf["type"] = "type";
  conf["index"] = index_name;
  conf["store"] = store_name;
  conf["metadata"] = metadata_name;
  conf["nb_values"] = std::to_string(n_values);

  Config config(std::move(conf));
  config.write(description_name);
}

bool DefaultStore::load() {

  // We will just write at the end of the file (but might read before)
  index_file.open(index_name, std::fstream::in| std::fstream::out | std::fstream::binary | std::fstream::app);
  store_file.open(store_name, std::fstream::in| std::fstream::out | std::fstream::binary | std::fstream::app);
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
  load_metadata();

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
  metadata.reserve(n_values);

  // Rather actually load these metadata from a proper file
  for(auto& it : index) {
    metadata[it.first] = std::make_pair(1, false);//placeholder
  }
}

void DefaultStore::write_index() {
  // Only write values that were newly discovered during this run
  // The old one can keep living happily at the beginning of the file!
  for(auto& it : index) {
    auto meta = metadata[it.first];
    if(meta.second) {
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
    size_t size = buf.size();
    store_file.write(reinterpret_cast<char*>(&size), sizeof(size_t));
    store_file.write(reinterpret_cast<const char*>(buf.data()), buf.size());

    // add it to the index
    index[key] = store_file.tellp();

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
