#include "source_ref.h"

#include "config.h"

#include <sstream>
#include <cassert>

SourceRefs::SourceRefs(fs::path config_path) {
  // load the configuration and srcrefs if they exist
  if(std::filesystem::exists(config_path)) {
    load_configuration();
    load_index();
    load_store();
  }else {
      //no packages and no srcrefs so far!
  }
}


void SourceRefs::write_configuration() {
  // names of the two auxiliary stores
  std::unordered_map<std::string, std::string> conf;
  conf["kind"] = "locations";
  conf["index"] = index_path.filename().string();
  conf["store"] = store_path.filename().string();
  conf["nb_packages"] = std::to_string(package_names.size());
  conf["nb_functions"] = std::to_string(function_names.size());
  conf["nb_arguments"] = std::to_string(argument_names.size());

  Config config(std::move(conf));
  config.write(config_path);
}

size_t SourceRefs::add_name(const std::string& name, std::unordered_map<std::string, size_t>& unique_names, std::vector<const std::string*>& ordering) {
  auto it = unique_names.find(name);
  if(it == unique_names.end()) {
    size_t idx = ordering.size() + 1;
    auto p = unique_names.insert(std::make_pair(name, idx));
    ordering.push_back(&p.first->first);// we need to the actual key in the set

    return idx;
  }

  return it->second;
}

bool SourceRefs::add_value(const std::array<char, 20>& key, const std::string& package_name, const std::string& function_name, const std::string& argument_name) {
  size_t pkg_id = add_name(package_name, pkg_names_u, package_names);
  size_t fun_id = add_name(function_name, function_names_u, function_names);
  size_t arg_id = add_name(argument_name, arg_names_u, argument_names);

  location_t loc;
  loc.package = pkg_id;
  loc.function = fun_id;
  loc.argument = arg_id;

  auto it = index.find(key);

  if(it != index.end()) {
    it->second.insert(loc);
  }
  else {
    index.emplace(std::make_pair(key, std::vector<location_t>(1, loc)));
  }

  return true;
}


void SourceRefs::write_store() {
  std::ofstream store_file(store_path, std::ofstream::trunc);
  // TODO: do no write again everything each time

  for(auto& pkg : package_names) {
    store_file << *pkg << "\n";
  }
  for(auto& func : function_names) {
    store_file << *func << "\n";
  }
  for(auto& arg : argument_names) {
    store_file << *arg << "\n";
  }
  store_file << std::flush;
}


void SourceRefs::load_store() {
  std::ifstream store_file(store_path);

  std::string buf;

  for(size_t i = 0; i < n_packages && std::getline(store_file, buf); i++) {
    add_name(buf, pkg_names_u, package_names);
  }

  for(size_t i = 0; i < n_functions && std::getline(store_file, buf); i++) {
    add_name(buf, function_names_u, function_names);
  }

  for(size_t i = 0; i < n_args && std::getline(store_file, buf); i++) {
    add_name(buf, arg_names_u, argument_names);
  }
}

void SourceRefs::write_index() {
  std::ofstream file(index_path, std::ofstream::trunc | std::ofstream::binary);

  for(auto& key: index) {
    const auto& source_locations = key->second;
    offsets[key] = file.tellp();

    size_t size = source_locations.size();
    file.write(reinterpret_cast<char*>(&size), sizeof(size_t));

    for(auto& loc : source_locations) {
      file.write(reinterpret_cast<char*>(&loc), sizeof(loc));
    }
  }
}

void SourceRefs::load_index() {
  // offsets should be initalized there
  assert(offsets.size() > 0);

  std::unordered_map<size_t, const std::array<char, 20>&> inv_offsets;
  inv_offsets.reserve(offsets.size());
  for(auto& it : offsets) {
    inv_offsets.insert(std::make_pair(it.second, it.first));
  }

  std::ifstream file(index_path, std::ofstream::binary);

  for(auto it : inv_offsets) { // Maybe we should sort the offset here? or use a map instead of an unordered_map?
    size_t size = 0;
    file.read(reinterpret_cast<char*>(&size), sizeof(size_t));
    assert(size > 0);

    std::unordered_set<location_t> locations;

    for(size_t i = 0; i < size; i++) {
      location_t loc;
      file.read(reinterpret_cast<char*>(&loc), sizeof(loc));
      locations.insert(loc);
    }

    index[it.second] = locations;
  }

  offsets.clear();
}


void SourceRefs::load_configuration() {
  Config conf(config_path);

  assert(conf["kind"] == "locations");

  index_path = conf["index"];
  store_path = conf["store"];
  n_packages = std::stoul(conf["nb_packages"]);
  n_functions = std::stoul(conf["nb_functions"]);
  n_args = std::stoul(conf["nb_arguments"]);
}

std::optional<const location_t&> SourceRefs::get_loc(const std::array<char, 20>& key) const {
  auto it = index.find(key);

  if(it == index.end()) {
    return std::optional<const location_t&>();
  }
  else {
    return  std::make_optional<const location_t&>(it->second);
  }
}

const std::string& SourceRefs::package_name(const std::array<char, 20>& key) const {
  auto loc = get_loc(key);

  if(loc) {
    return *package_names[loc->package];
  }
  else {
    return
  }

}
const std::string& function_name(const std::array<char, 20>& key) const;
const std::string& argument_name(const std::array<char, 20>& key) const;

const std::tuple<const std::string&, const std::string&, const std::string&> source_location(const std::array<char, 20>& key) const;

