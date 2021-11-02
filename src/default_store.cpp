#include "default_store.h"

#include <unordered_map>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <locale>

// from boost::hash_combine
void hash_combine(std::size_t& seed, std::size_t value) {
  seed ^= value + 0x9e3779b9 + (seed<<6) + (seed>>2);
}

// trim from start (in place)
static inline void ltrim(std::string &s) {
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
    return !std::isspace(ch);
  }));
}

// trim from end (in place)
static inline void rtrim(std::string &s) {
  s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) {
    return !std::isspace(ch);
  }).base(), s.end());
}

// trim from both ends (in place)
static inline void trim(std::string &s) {
  ltrim(s);
  rtrim(s);
}



Description::Description(const std::string& filename) {
  std::ifstream description_file(filename);
  if(!description_file) {
    std::cerr << "No description file " << filename << std::endl;
    exit(1);
  }

  std::unordered_map<std::string, std::string> config;
  std::string line;
  while(std::getline(description_file, line)) {
    std::istringstream is_line(line);
    std::string key;
    if(std::getline(is_line, key, '=')) {
      std::string value;
      if (key[0] == '#')
        continue;

      if (std::getline(is_line, value))
      {
        trim(key);
        trim(value);
        config[key] = value;
      }
    }
  }
  type = config["type"];
  index_name = config["index"];
  store_name = config["store"];
  metadata_name = config["metadata"];
  nb_values = std::stoul(config["nb_values"]);
}

void Description::write(std::ostream& out) {
  out << "type=" << type << "\n";
  out << "index=" << index_name << "\n";
  out << "store=" << store_name << "\n";
  out << "metadata=" << metadata_name << "\n";
  out << "nb_values=" << nb_values << std::endl;
}

DefaultStore::DefaultStore(const std::string& description_name) :
  description_name(description_name), description(description_name), bytes_read(0)
{
  load();
}


DefaultStore::~DefaultStore() {
  std::ofstream description_file(description_name);
  description.write(description_file);
}

bool DefaultStore::load() {

  // We will just write at the end of the file (but might read before)
  index_file.open(description.index_name, std::fstream::in| std::fstream::out | std::fstream::binary | std::fstream::app);
  store_file.open(description.store_name, std::fstream::in| std::fstream::out | std::fstream::binary | std::fstream::app);
  if(!index_file) {
    std::cerr << "Failed to open index file " << description.index_name << std::endl;
  }
  if(!store_file) {
    std::cerr << "Failed to open store file " << description.store_name << std::endl;
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
  index.reserve(description.nb_values);
  size_t start = 0;
  std::array<char, 20> hash;
  size_t offset = 0;

  for(size_t i = 0; i < description.nb_values ; i++) {
    index_file.read(hash.data(), 20);
    index_file.read(reinterpret_cast<char*>(&offset), sizeof(offset));
    index[hash] = offset;
    bytes_read += 20 + sizeof(offset);
  }
}

void DefaultStore::load_metadata() {
  metadata.reserve(description.nb_values);

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
