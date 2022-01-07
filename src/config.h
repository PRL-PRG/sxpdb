#ifndef SXPDB_CONFIG_H
#define SXPDB_CONFIG_H

#include <string>
#include <unordered_map>
#include <filesystem>


namespace fs = std::filesystem;


inline void ltrim(std::string &s);
inline void rtrim(std::string &s);
inline void trim(std::string &s);

// For config style files
// key=value + comments
class Config {
private:
  std::unordered_map<std::string, std::string> config;
public:
  Config(const fs::path& path);
  Config(std::unordered_map<std::string, std::string>&& config);

  const std::string& get(const std::string& key) const;

  void write(const fs::path& filename);

  const std::string& operator[](const std::string& key) const { return config.at(key);}

};


#endif
