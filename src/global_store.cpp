#include "global_store.h"

#include "csv_file.h"
#include "utils.h"
#include <chrono>
#include <cassert>


GlobalStore::GlobalStore(const std::string& filename, bool _quiet = true) :
  Store("global"),
  configuration_path(fs::absolute(filename)), bytes_read(0), total_values(0),
  rand_engine(std::chrono::system_clock::now().time_since_epoch().count()),
  quiet(_quiet), pid(getpid())
{
  if(std::filesystem::exists(configuration_path)) {
    // Check the lock file
    bool to_check = false;
    fs::path lock_path = configuration_path.parent_path().append(".LOCK");
    if(std::filesystem::exists(lock_path)) {
      Rprintf("Database did not exit properly. Will perform check_db in slow mode.\n");
      to_check = true;
    }
    else {
      std::ofstream lock_file(lock_path);
      lock_file << std::chrono::system_clock::now().time_since_epoch().count() << std::endl;
    }

    // Load configuration files
    CSVFile config(configuration_path);
    //Layout:
    // Filename,Type,Number of values

    // Build the stores
    for(auto& row : config.get_rows()) {
      fs::path config_path = configuration_path.parent_path().append(row.at(0));

      if(row.at(3) == "locations") {
        src_refs = std::make_unique<SourceRefs>(config_path);
        if(!quiet) Rprintf("Loaded source locations at %s from %s packages\n", config_path.c_str(),  row.at(2).c_str());
        continue;
      }



      stores.push_back(std::make_unique<GenericStore>(config_path, src_refs));



      //Check if the types in the configuration file and in the CSV are coherent
      if(row.at(1) != stores.back()->sexp_type()) {
        Rf_error("Inconsistent types in the global configuration file and the store configuration file: %s vs %s\n",
          row.at(1).c_str(), stores.back()->sexp_type().c_str());
      }

      //Check if the kinds of stores in the configuration file and in the CSV are coherent
      if(row.at(3) != stores.back()->store_kind()) {
        Rf_error("Inconsistent kinds of stores in the global configuration file and the store configuration file: %s vs %s\n",
          row.at(3).c_str(),  stores.back()->store_kind().c_str());
      }

      types[row.at(1)] = stores.size() - 1;//index of the element that was just inserted

      size_t nb_values = std::stoul(row.at(2));

      if(nb_values != stores.back()->nb_values()) {
        Rf_error("Inconsistent number of values in the global configuration file and the store configuration file: %ld vs %ld\n",
          nb_values, stores.back()->nb_values());
      }

      if(!quiet) Rprintf("Loaded %s store at %s with type %s with %ld values.\n",
         row.at(3).c_str(), config_path.c_str(), row.at(1).c_str(), nb_values);

      total_values += nb_values;
    }

    if(to_check) {
      Rprintf("Checking the database in slow mode.\n");
      auto errors = check(true);
      if(errors.size() > 0 ) {
        Rf_error("The database is corrupted: %ld errors. Open it again with autorepair to try repairing it.\n", errors.size());
      }
      else {
        Rprintf("No errors found.\n");
      }
    }
  }
  else {
    if(!quiet) Rprintf("Creating new database %s\n", filename.c_str());

    create();
  }

  assert(types.count("any") > 0);

}

void GlobalStore::create() {
  // Just create a generic default store and a location store

  assert(stores.size() == 0);

  src_refs = std::make_shared<SourceRefs>(configuration_path.parent_path().append("locations"));

  stores.push_back(std::make_unique<GenericStore>(configuration_path.parent_path().append("generic"), src_refs));
  types["any"] = 0;

  write_configuration();
}

bool GlobalStore::merge_in(GlobalStore& gstore) {
  size_t new_total_values = 0;
  // merge the various stores
  for(auto&store : gstore.stores) {
    //look for a store in this that has the same type
    // merge
    // If there is not corresponding store, merge in the generic one
    auto it = types.find(store->sexp_type());
    size_t store_index = 0;
    if(it != types.end()) {
      store_index = it->second;
    }
    else {
      store_index = types["any"];
    }
    stores[store_index]->merge_in(*store);

    // update the counters
    new_total_values += stores[store_index]->nb_values();
  }

  // Update the configuration information
  assert(new_total_values >= total_values);
  total_values = new_total_values;

  // update the origin locations
  src_refs->merge_in(*gstore.src_refs);

  write_configuration();// might be redundant (or rather, the destructor might be redundant)

  return true;
}

bool GlobalStore::merge_in(Store& store) {
  GlobalStore* st = dynamic_cast<GlobalStore*>(&store);
  if(st == nullptr) {
    Rf_warning("Cannot merge a global store with a store of kind %s\n", store.store_kind().c_str());
    return false;
  }

  return merge_in(*st);
}

std::pair<const sexp_hash*, bool> GlobalStore::add_value(SEXP val) {
  // Ignore environments and closures
  if(TYPEOF(val) == ENVSXP || TYPEOF(val) == CLOSXP) {
    return std::make_pair(nullptr, false);
  }

  // Check if the instrumented program has not evily forked itself
  if(pid != getpid()) {
    return std::make_pair(nullptr, false);
  }

  // we assume there is at least a "any" store
  auto it = types.find(Rf_type2char(TYPEOF(val)));
  size_t store_index= 0;

  if(it != types.end()) {
    store_index = it->second;
  }
  else {
    store_index = types.at("any"); // so fail if there is no "any" type
    // or just return false to indicate that we could not store it?
  }

  auto added = stores[store_index]->add_value(val);

  if(added.second) {
    new_elements = true;
    total_values++;
  }

  return added;
}

std::pair<const sexp_hash*, bool> GlobalStore::add_value(SEXP val, const std::string& pkg_name, const std::string& func_name, const std::string& arg_name) {
  // Ignore environments
  if(TYPEOF(val) == ENVSXP || TYPEOF(val) == CLOSXP) {
    return std::make_pair(nullptr, false);
  }
  // Check if the instrumented program has not evily forked itself
  if(pid != getpid()) {
    return std::make_pair(nullptr, false);
  }

  auto hash = add_value(val);
  assert(hash.first != nullptr);
  src_refs->add_value(*hash.first, pkg_name, func_name, arg_name);

  return hash;
}

bool GlobalStore::add_origins(const sexp_hash& hash, const std::string& pkg_name, const std::string& func_name, const std::string& arg_name) {
  return src_refs->add_value(hash, pkg_name, func_name, arg_name);
}

bool  GlobalStore::have_seen(SEXP val) const {
  auto it = types.find(Rf_type2char(TYPEOF(val)));
  size_t store_index= 0;

  if(it != types.end()) {
    store_index = it->second;
  }
  else {
    store_index = types.at("any"); // so fail if there is no "any" type
    // or just return false to indicate that we could not store it?
  }

  return stores[store_index]->have_seen(val);
}

SEXP GlobalStore::get_metadata(SEXP val) const {
  auto it = types.find(Rf_type2char(TYPEOF(val)));
  size_t store_index= 0;

  if(it != types.end()) {
    store_index = it->second;
  }
  else {
    store_index = types.at("any"); // so fail if there is no "any" type
    // or just return false to indicate that we could not store it?
  }

  return stores[store_index]->get_metadata(val);
}

SEXP GlobalStore::get_metadata(uint64_t index) const {
  size_t store_index = 0;
  size_t values = stores[store_index]->nb_values(); // we assume there is at least one store

  while(values < index) {
    store_index++;
    if(store_index >= stores.size()) {
      break;
    }
    values += stores[store_index]->nb_values();
  }

  uint64_t index_in_store = index - (values - stores[store_index]->nb_values());

  return stores[store_index]->get_metadata(index_in_store);
}


SEXP GlobalStore::get_value(uint64_t index) {
  size_t store_index = 0;
  size_t values = stores[store_index]->nb_values(); // we assume there is at least one store

  while(values < index) {
    store_index++;
    if(store_index >= stores.size()) {
      break;
    }
    values += stores[store_index]->nb_values();
  }

  uint64_t index_in_store = index - (values - stores[store_index]->nb_values());

  // TODO: handle case when the value is out of bounds!!
  // Or just leave it to the default store to panic and handle it?

  return stores[store_index]->get_value(index_in_store);
}


const sexp_hash& GlobalStore::get_hash(uint64_t index) const {
  size_t store_index = 0;
  size_t values = stores[store_index]->nb_values();

  while(values < index) {
    store_index++;
    if(store_index >= stores.size()) {
      break;
    }
    values += stores[store_index]->nb_values();
  }

  uint64_t index_in_store = index - (values - stores[store_index]->nb_values());

  return stores[store_index]->get_hash(index_in_store);
}

const std::vector<size_t> GlobalStore::check(bool slow_check) {
  uint64_t values = 0;

  std::vector<size_t> errors;

  for(size_t store_index = 0; store_index < stores.size(); store_index++) {
    std::vector<size_t> store_errors = stores[store_index]->check(slow_check);

    errors.reserve(errors.size() + store_errors.size());
    for(size_t idx : store_errors) {
        errors.push_back(values + idx);
    }

    values += stores[store_index]->nb_values();
  }

  return errors;
}

const SEXP GlobalStore::map(const SEXP function) {

  if(stores.size() == 1) {
    return stores[0]->map(function);
  }

  // We will need to do some copying
  SEXP res = PROTECT(Rf_allocList(nb_values()));
  // Or rather reuse the largest store and extends its size? to do less copying?

  size_t start_i = 0;

  for(size_t store_index = 0; store_index < stores.size(); store_index++) {
    SEXP l = stores[store_index]->map(function);
    for(R_xlen_t i = 0 ; i < Rf_xlength(l) ; i++) {
      SET_VECTOR_ELT(res, start_i + i, VECTOR_ELT(l, i));
    }
    start_i += stores[store_index]->nb_values();
  }

  UNPROTECT(1);
  return res;
}

const SEXP GlobalStore::view_values()  {
  if(stores.size() == 1) {
    return stores[0]->view_values();
  }

  SEXP res = PROTECT(Rf_allocList(nb_values()));

  size_t start_i = 0;

  for(size_t store_index = 0; store_index < stores.size(); store_index++) {
    SEXP l = stores[store_index]->view_values();
    for(R_xlen_t i = 0 ; i < Rf_xlength(l) ; i++) {
      SET_VECTOR_ELT(res, start_i + i, VECTOR_ELT(l, i));
    }
    start_i += stores[store_index]->nb_values();
  }

  UNPROTECT(1);
  return res;
}

const SEXP GlobalStore::view_metadata() const {
  if(stores.size() == 1) {
    return stores[0]->view_metadata();
  }

  SEXP l = PROTECT(Rf_allocVector(VECSXP, stores.size()));


  for(size_t store_index = 0; store_index < stores.size(); store_index++) {
    SEXP res = stores[store_index]->view_metadata();
    SET_VECTOR_ELT(l, store_index, res);
  }

  SEXP df = PROTECT(bind_rows(l));

  UNPROTECT(2);
  return df;
}

const SEXP GlobalStore::view_origins() const {


  if(stores.size() == 1) {
    return stores[0]->view_origins(src_refs);
  }

  SEXP l = PROTECT(Rf_allocVector(VECSXP, stores.size()));


  for(size_t store_index = 0; store_index < stores.size(); store_index++) {
    SEXP res = stores[store_index]->view_origins(src_refs);
    SET_VECTOR_ELT(l, store_index, res);
  }

  SEXP df = PROTECT(bind_rows(l));

  UNPROTECT(2);
  return df;

}

const std::vector<std::tuple<const std::string, const std::string, const std::string>> GlobalStore::source_locations(uint64_t index) const {
  return source_locations(get_hash(index));
}

SEXP GlobalStore::sample_value() {
  //TODO: or seed it just at the beginning and have it as a class member?

  std::uniform_int_distribution<uint64_t> dist(0, total_values - 1);

  return get_value(dist(rand_engine));
}

SEXP GlobalStore::sample_value(const Description& d) {
  roaring::Roaring64Map index;
  if(d.type != UNIONTYPE) {
    index |= types_index[d.type];
  }
  else if(d.type == UNIONTYPE) {
    for(auto& desc : d.descriptions) {
      assert(desc.type != UNIONTYPE);
      index |= types_index[desc.type];
    }
  }

  if(d.has_class && d.has_class.value()) {
    index &= class_index;
  }
  else if(d.has_class && !d.has_class.value()) {
    roaring::Roaring64Map nonclass = class_index;
    nonclass.flip(std::max(index.minimum(), nonclass.minimum()), std::min(nonclass.maximum(), index.maximum()));

    index &= nonclass;
  }

  if(d.has_attributes && d.has_attributes.value()) {
    index &= attributes_index;
  }
  else if(d.has_attributes && !d.has_attributes.value()) {
    roaring::Roaring64Map nonattributes = attributes_index;
    nonattributes.flip(std::max(index.minimum(), nonattributes.minimum()), std::min(nonattributes.maximum(), index.maximum()));
    index &= nonattributes;
  }

  if(d.is_vector && d.is_vector.value()) {
    index &= vector_index;
  }
  else if(d.is_vector && !d.is_vector.value()) {
    roaring::Roaring64Map nonvector = vector_index;
    nonvector.flip(std::max(index.minimum(), nonvector.minimum()), std::min(nonvector.maximum(), index.maximum()));
    index &= nonvector;
  }

  if(d.has_na && d.has_na.value()) {
    index &= na_index;
  }
  else if(d.has_na && !d.has_na.value()) {
    roaring::Roaring64Map nonna = na_index;
    nonna.flip(std::max(index.minimum(), nonna.minimum()), std::min(nonna.maximum(), index.maximum()));
    index &= nonna;
  }

  //TODO: length, class names, n dimensions

  //TODO: better API: rather generate a sampler, which will hold the unioned/intersected indexes already, so as not to renegenerate it after
  // each request
  // Then we can optimize and shrink to fit

  std::uniform_int_distribution<uint64_t> dist(0, index.cardinality() - 1);

  uint64_t element;

  bool res = index.select(dist(rand_engine), &element);
  assert(res);

  return get_value(element);
}

void GlobalStore::build_indexes() {
  // TODO: we should really break the current class hierarchy architecture...
  // DefaultStore and GenericStore should simply be tables

  //TODO: use a build index function from the generic store...

  // we should have ANYSXP which should be an index of all the database

  index_generated = true;
}


void GlobalStore::write_configuration() {
  CSVFile file;
  std::vector<std::string> row(4);
  for(auto& store: stores) {
    row[0] = store->description_path().filename().string();
    row[1] = store->sexp_type();
    row[2] = std::to_string(store->nb_values());
    row[3] = store->store_kind();
    file.add_row(std::move(row));
  }

  // Source locations
  row[0] = src_refs->description_path().filename().string();
  row[1] = "str";
  row[2] = std::to_string(src_refs->nb_packages());
  row[3] = "locations";
  file.add_row(std::move(row));

  file.write(configuration_path);
}



GlobalStore::~GlobalStore() {
  if(pid == getpid()) {
    if(new_elements || total_values == 0) {
      write_configuration();
    }

    fs::path lock_path = configuration_path.parent_path().append(".LOCK");
    fs::remove(lock_path);
  }
}

