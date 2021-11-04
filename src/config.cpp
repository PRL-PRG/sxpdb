#include "config.h"

#include <fstream>
#include <iostream>
#include <sstream>
#include <algorithm>

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

Config::Config(const std::string& filename) {
  std::ifstream description_file(filename);
  if(!description_file) {
    std::cerr << "No description file " << filename << std::endl;
    exit(1);
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

void Config::write(const std::string& filename) {
  std::ofstream file(filename,std::ofstream::out | std::ofstream::trunc);

  if(!file) {
    std::cerr << "Impossible to open for write config file " << filename << std::endl;
  }

  for(auto& it : config) {
    file << it.first << "=" << it.second << "\n";
  }
  file << std::flush;
}


Config::Config(std::unordered_map<std::string, std::string>&& config) : config(config){

}
