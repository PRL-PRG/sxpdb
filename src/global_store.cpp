#include "global_store.h"

#include "csv_file.h"
#include <chrono>
#include <cassert>


GlobalStore::GlobalStore(const std::string& filename) :
  Store("global"),
  configuration_path(fs::absolute(filename)), bytes_read(0), total_values(0),
  rand_engine(std::chrono::system_clock::now().time_since_epoch().count())
{


  if(std::filesystem::exists(configuration_path)) {
    // Load configuration files
    CSVFile config(configuration_path);
    //Layout:
    // Filename,Type,Number of values

    // Build the stores
    for(auto& row : config.get_rows()) {
      fs::path config_path = configuration_path.parent_path().append(row.at(0));

      if(row.at(3) == "locations") {
        std::cout << "Loading source locations at " << config_path << "from " << row.at(2) << "packages" << std::endl;
        src_refs = std::make_unique<SourceRefs>(config_path);
        continue;
      }

      std::cout << "Loading " << row.at(3) << "store at " << config_path << " with type " <<
        row.at(1) << " with " << row.at(2) << " values." << std::endl;

      stores.push_back(std::make_unique<DefaultStore>(config_path));


      //Check if the types in the configuration file and in the CSV are coherent
      if(row.at(1) != stores.back()->sexp_type()) {
        std::cerr << "Inconsistent types in the global configuration file and the store configuration file: " <<
          row.at(1) << " vs " << stores.back()->sexp_type() << std::endl;
          exit(1);
      }

      //Check if the kinds of stores in the configuration file and in the CSV are coherent
      if(row.at(3) != stores.back()->sexp_type()) {
        std::cerr << "Inconsistent kinds of stores in the global configuration file and the store configuration file: " <<
          row.at(3) << " vs " << stores.back()->store_kind() << std::endl;
        exit(1);
      }

      types[row.at(1)] = stores.size() - 1;//index of the element that was just inserted

      size_t nb_values = std::stoul(row.at(2));

      if(nb_values != stores.back()->nb_values()) {
        std::cerr << "Inconsistent number of values in the global configuration file and the store configuration file: " <<
          nb_values << " vs " << stores.back()->nb_values() << std::endl;
          exit(1);
      }

      total_values += nb_values;
    }
  }
  else {
    std::cerr << "Configuration file does not exist. Creating new database " << filename << std::endl;

    create();
  }

  assert(types.count("any") > 0);

}

void GlobalStore::create() {
  // Just create a generic default store and a location store

  assert(stores.size() == 0);

  stores.push_back(std::make_unique<DefaultStore>(configuration_path.parent_path().append("generic"), "any"));
  types["any"] = 0;

  src_refs = std::make_unique<SourceRefs>(configuration_path.parent_path().append("locations"));

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

  write_configuration();// might be redundant (or rather, the destructor might be redundant)

  return true;
}

bool GlobalStore::add_value(SEXP val) {
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

  bool added = stores[store_index]->add_value(val);

  if(added) {
    total_values++;
    return true;
  }

  return false;
}

bool GlobalStore::have_seen(SEXP val) const {
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

SEXP GlobalStore::get_metadata(size_t index) const {
  size_t store_index = 0;
  size_t values = stores[store_index]->nb_values(); // we assume there is at least one store

  while(values < index) {
    store_index++;
    if(store_index >= stores.size()) {
      break;
    }
    values += stores[store_index]->nb_values();
  }

  size_t index_in_store = index - (values - stores[store_index]->nb_values());

  return stores[store_index]->get_metadata(index_in_store);
}


SEXP GlobalStore::get_value(size_t index) {
  size_t store_index = 0;
  size_t values = stores[store_index]->nb_values(); // we assume there is at least one store

  while(values < index) {
    store_index++;
    if(store_index >= stores.size()) {
      break;
    }
    values += stores[store_index]->nb_values();
  }

  size_t index_in_store = index - (values - stores[store_index]->nb_values());

  // TODO: handle case when the value is out of bounds!!
  // Or just leave it to the default store to panic and handle it?

  return stores[store_index]->get_value(index_in_store);
}


SEXP GlobalStore::sample_value() {
  //TODO: or seed it just at the beginning and have it as a class member?

  std::uniform_int_distribution<size_t> dist(0, total_values - 1);

  return get_value(dist(rand_engine));
}

std::chrono::microseconds GlobalStore::avg_insertion_duration() const {
  std::chrono::microseconds avg_dur;

  for(auto& store : stores) {
    avg_dur += store->avg_insertion_duration() * store->nb_values();
  }
  return avg_dur / total_values;
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
  row[1] = "";
  row[2] = src_refs->nb_packages();
  row[3] = "locations";
  file.add_row(std::move(row));

  file.write(configuration_path);
}

GlobalStore::~GlobalStore() {
  write_configuration();
}

