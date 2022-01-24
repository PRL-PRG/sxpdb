#include "source_ref.h"

#include "config.h"

#include <sstream>
#include <cassert>


SourceRefs::SourceRefs(fs::path config_path) : config_path(config_path), pid(getpid()){
  // load the configuration and srcrefs if they exist
  if(std::filesystem::exists(config_path)) {
    load_configuration();
    load_index();
    load_store();
  }else {
      //no packages and no srcrefs so far!
      index_path = fs::absolute(config_path).parent_path().append("src_index.bin");
      offsets_path = fs::absolute(config_path).parent_path().append("src_offsets.bin");
      store_path = fs::absolute(config_path).parent_path().append("src_store.bin");
      n_values = 0;
      n_packages = 0;
      n_functions = 0;
      n_args = 0;
  }
}


void SourceRefs::write_configuration() {
  // names of the two auxiliary stores
  std::unordered_map<std::string, std::string> conf;
  conf["kind"] = "locations";
  conf["offsets"] = offsets_path.filename().string();
  conf["index"] = index_path.filename().string();
  conf["store"] = store_path.filename().string();
  conf["nb_values"] = std::to_string(n_values);
  conf["nb_packages"] = std::to_string(package_names.size());
  conf["nb_functions"] = std::to_string(function_names.size());
  conf["nb_arguments"] = std::to_string(argument_names.size());

  Config config(std::move(conf));
  config.write(config_path);
}


uint64_t SourceRefs::add_name(const std::string& name, unique_names_t& unique_names, ordered_names_t& ordering) {
  auto pname = std::make_shared<const std::string>(name);//Copy the string into a newly allocated string
  auto it = unique_names.find(pname);
  if(it == unique_names.end()) {
    uint64_t idx = ordering.size();// index, not size, but the size has not been increased yet
    unique_names[pname] = idx;
    ordering.push_back(pname);

    return idx;
  }

  return it->second;
}


bool SourceRefs::add_value(const sexp_hash& key, const std::string& package_name, const std::string& function_name, const std::string& argument_name) {
  uint64_t pkg_id = add_name(package_name, pkg_names_u, package_names);
  uint64_t fun_id = add_name(function_name, function_names_u, function_names);
  //we use a magic value for a return value
  uint64_t arg_id = (argument_name == "") ? return_value : add_name(argument_name, arg_names_u, argument_names);
  assert(argument_names.size() != return_value); // If there are too many different argument names, it could clash with the return value magic value

  location_t loc;
  loc.package = pkg_id;
  loc.function = fun_id;
  loc.argument = arg_id;

  auto it = index.find(key);

  bool r = true;//true if it is a new origin

  if(it != index.end()) {
    auto res = it->second.insert(loc);
    if(res.second) { // the location did not already exist
      new_elements = true;
    }
    else {
      r = false;
    }
  }
  else {
    index.emplace(key, robin_hood::unordered_set<location_t>({loc}));
    n_values++;
    new_elements = true;
  }


  return r;
}


void SourceRefs::write_store() {
  std::ofstream store_file(store_path, std::ofstream::trunc);
  store_file.exceptions(std::fstream::failbit);
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
  if(!store_file) {
    Rf_error("Impossible to open the origin store file %s\n", store_path.filename().c_str());
  }

  store_file.exceptions(std::fstream::failbit);

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
  file.exceptions(std::fstream::failbit);


  for(auto& it: index) {
    const auto& source_locations = it.second;
    offsets[it.first] = file.tellp();

    uint64_t size = source_locations.size();
    file.write(reinterpret_cast<char*>(&size), sizeof(uint64_t));

    for(auto& loc : source_locations) {
      file.write(reinterpret_cast<const char*>(&loc), sizeof(location_t));
    }
  }

  file.close();

  //Now write the offsets here
  std::ofstream offset_file(offsets_path, std::ofstream::trunc | std::ofstream::binary);
  offset_file.exceptions(std::fstream::failbit);

  for(auto it : offsets) {
    // no newly_seen here, as the offset could change
    offset_file.write(reinterpret_cast<const char*>(&it.first.low64), sizeof(it.first.low64));
    offset_file.write(reinterpret_cast<const char*>(&it.first.high64), sizeof(it.first.high64));
    offset_file.write(reinterpret_cast<char*>(&it.second), sizeof(uint64_t));
  }
}

void SourceRefs::load_index() {
  // Read the offsets from the offset file
  //open the file, read the offsets and populate the offsets hashmap
  std::ifstream offset_file(offsets_path, std::ofstream::binary);
  if(!offset_file) {
    Rf_error("Impossible to open the origin offsets file %s\n", offsets_path.filename().c_str());
  }

  offset_file.exceptions(std::fstream::failbit);

  sexp_hash hash;

  uint64_t offset = 0;

  offsets.reserve(n_values);

  for(size_t i = 0; i < n_values ; i++) {
    offset_file.read(reinterpret_cast<char*>(&hash.low64), sizeof(hash.low64));
    offset_file.read(reinterpret_cast<char*>(&hash.high64), sizeof(hash.high64));

    offset_file.read(reinterpret_cast<char*>(&offset), sizeof(uint64_t));

    offsets[hash] = offset;
  }

  assert(offset_file.tellg() == 0 || offsets.size() > 0);
  offset_file.close();


  // Now read the source location indices from the index

  std::ifstream file(index_path, std::ofstream::binary);
  if(!offset_file) {
    Rf_error("Impossible to open the origin index file %s\n", index_path.filename().c_str());
  }
  file.exceptions(std::fstream::failbit);

  // Maybe we should sort the offset here? or use a map instead of an unordered_map?
  // (after inverting the mapping hash -> offset)
  for(auto it : offsets) {
    uint64_t size = 0;
    file.seekg(it.second);// go the offset of that record
    file.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
    assert(size > 0);

    robin_hood::unordered_set<location_t> locations;

    for(uint64_t i = 0; i < size; i++) {
      location_t loc;
      file.read(reinterpret_cast<char*>(&loc), sizeof(loc));
      locations.insert(loc);
    }

    index[it.first] = locations;
  }

  offsets.clear();// necessary?
}


void SourceRefs::load_configuration() {
  Config conf(config_path);

  assert(conf["kind"] == "locations");

  index_path = fs::absolute(config_path).parent_path().append(conf["index"]);
  store_path = fs::absolute(config_path).parent_path().append(conf["store"]);
  offsets_path = fs::absolute(config_path).parent_path().append(conf["offsets"]);
  n_values = std::stoul(conf["nb_values"]);
  n_packages = std::stoul(conf["nb_packages"]);
  n_functions = std::stoul(conf["nb_functions"]);
  n_args = std::stoul(conf["nb_arguments"]);
}

const robin_hood::unordered_set<location_t> SourceRefs::get_locs(const sexp_hash& key) const {
  auto it = index.find(key);

  if(it != index.end()) {
    return it->second;
  }
  else {
    return robin_hood::unordered_set<location_t>();
  }
}

const SourceRefs::ordered_names_t SourceRefs::pkg_names(const sexp_hash& key) const {
  auto it = index.find(key);

  if(it == index.end()) {
    return ordered_names_t();
  }


  ordered_names_t res;
  res.reserve(it->second.size());

  for(auto loc : it->second) {
    res.push_back(package_names[loc.package]);
  }

  return res;
}


const std::vector<std::tuple<const std::string, const std::string, const std::string>> SourceRefs::source_locations(const sexp_hash& key) const {

  auto locs = get_locs(key);

  std::vector<std::tuple<const std::string, const std::string, const std::string>> res;
  res.reserve(locs.size());

  for(auto loc : locs) {
    res.push_back(std::make_tuple(*package_names[loc.package],
                                  *function_names[loc.function],
                                  (loc.argument == return_value) ? return_value_str : *argument_names[loc.argument]));
  }


  return res;
}

const SEXP SourceRefs::locations_sexp_cache() const {
  // Mirror the std::string to CHARSXP
  SEXP char_cache = PROTECT(Rf_allocVector(VECSXP, 3));

  R_xlen_t i = 0;
  SEXP package_cache = Rf_allocVector(STRSXP, nb_packages());
  SET_VECTOR_ELT(char_cache, 0, package_cache);
  for(auto& pkg_name : package_names) {
    SET_STRING_ELT(package_cache, i, Rf_mkChar(pkg_name->c_str()));
    i++;
  }

  i = 0;
  SEXP function_cache = Rf_allocVector(STRSXP, nb_functions());
  SET_VECTOR_ELT(char_cache, 1, function_cache);
  for(auto& fun_name : function_names) {
    SET_STRING_ELT(function_cache, i, Rf_mkChar(fun_name->c_str()));
    i++;
  }

  i= 0 ;
  SEXP argument_cache = Rf_allocVector(STRSXP, nb_arguments());
  SET_VECTOR_ELT(char_cache, 2, argument_cache);
  for(auto& arg_name : argument_names) {
    SET_STRING_ELT(argument_cache, i, Rf_mkChar(arg_name->c_str()));
    i++;
  }

  UNPROTECT(1);

  return char_cache;
}


SourceRefs::~SourceRefs() {
  if(new_elements || n_values == 0) {
    write_store();
    write_index();
    write_configuration();
  }
}


void SourceRefs::merge_in(SourceRefs& srcrefs) {
  for(auto& value : srcrefs.index) {
    const auto& locs = value.second;// set of locations
    for(auto& loc : locs ) {
      add_value(value.first, *srcrefs.package_names[loc.package],
                *srcrefs.function_names[loc.function],
                (loc.argument == return_value) ? return_value_str : *srcrefs.argument_names[loc.argument]);
    }
  }
}

