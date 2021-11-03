#include "global_store.h"

#include "csv_file.h"
#include <chrono>

GlobalStore::GlobalStore(const std::string& filename) : configuration_name(filename), bytes_read(0), total_values(0),
  rand_engine(std::chrono::system_clock::now().time_since_epoch().count())
{
  // Load configuration files
  CSVFile config(filename);
  //Layout:
  // Filename,Type,Number of values


  // Build the stores
  for(auto& row : config.get_rows()) {
    std::cout << "Loading store at " << row.at(0) << " with type " <<
      row.at(1) << " with " << row.at(2) << " values." << std::endl;

    stores.push_back(std::make_unique<DefaultStore>(row.at(0)));


    //Check if the types in the configuration file and in the CSF are coherent
    if(row.at(1) != stores.back()->sexp_type()) {
      std::cerr << "Inconsistent types in the global configuration file and the store configuration file: " <<
        row.at(1) << " vs " << stores.back()->sexp_type() << std::endl;
    }

    types[row.at(1)] = stores.size() - 1;//index of the element that was just inserted

    size_t nb_values = std::stoul(row.at(2));

    if(nb_values != stores.back()->nb_values()) {
      std::cerr << "Inconsistent number of values in the global configuration file and the store configuration file: " <<
        nb_values << " vs " << stores.back()->nb_values() << std::endl;
    }

    total_values += nb_values;
  }

}

bool GlobalStore::merge_in(const std::string& filename) {
  //TODO: rather do it by merging with another store as argument?
  // Then, the store can access the other ones data and can do the kob...
}

bool GlobalStore::add_value(SEXP val) {
  // we assume there is at least a "any" store
  auto it = types.find(Rf_type2char(TYPEOF(val)));
  size_t store_index= 0;

  if(it != nullptr) {
    store_index = it->second;
  }
  else {
    store_index = types.at("any"); // so fail if there is no "any" type
    // or just return false to indicate that we could not store it?
  }

  stores[store_index]->add_value(val);

  return true;
}

bool GlobalStore::have_seen(SEXP val) const {
  auto it = types.find(Rf_type2char(TYPEOF(val)));
  size_t store_index= 0;

  if(it != nullptr) {
    store_index = it->second;
  }
  else {
    store_index = types.at("any"); // so fail if there is no "any" type
    // or just return false to indicate that we could not store it?
  }

  return stores[store_index]->have_seen(val);
}


SEXP GlobalStore::get_value(size_t index) const {
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


SEXP GlobalStore::sample_value() const {
  //TODO: or seed it just at the beginning and have it as a class member?

  std::uniform_int_distribution<size_t> dist(0, total_values - 1);

  return get_value(dist(rand_engine));
}

GlobalStore::~GlobalStore() {
  CSVFile file;
  std::vector<std::string> row(3);
  for(auto& store: stores) {
    row[0] = store->description_file();
    row[1] = store->sexp_type();
    row[2] = std::to_string(store->nb_values());
    file.add_row(std::move(row));
  }
  file.write(configuration_name);
}
