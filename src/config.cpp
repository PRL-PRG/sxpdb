#include "config.h"

#include <fstream>
#include <iostream>
#include <sstream>
#include <algorithm>

#define R_NO_REMAP
#include <Rinternals.h>

// trim from start (in place)
inline void ltrim(std::string &s) {
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
    return !std::isspace(ch);
  }));
}

// trim from end (in place)
inline void rtrim(std::string &s) {
  s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) {
    return !std::isspace(ch);
  }).base(), s.end());
}

// trim from both ends (in place)
inline void trim(std::string &s) {
  ltrim(s);
  rtrim(s);
}

Config::Config(const fs::path& path) {
  std::ifstream description_file(path);
  if(!description_file) {
   Rf_error("No configuration file %s\n", path.c_str());
  }

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
}

void Config::write(const fs::path& filename) {
  std::ofstream file(filename,std::ofstream::out | std::ofstream::trunc);

  if(!file) {
    Rf_error("Impossible to open for write config file %s\n", filename.c_str());
  }

  for(auto& it : config) {
    file << it.first << "=" << it.second << "\n";
  }
  file << std::flush;
}


Config::Config(std::unordered_map<std::string, std::string>&& config) : config(config){

}
