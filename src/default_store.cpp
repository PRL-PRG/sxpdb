#include "default_store.h"

#include <unordered_map>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <locale>

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
  description_name(description_name), description(description_name)
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


}

