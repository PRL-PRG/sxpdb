#include "global_store.h"

#include "csv_file.h"

GlobalStore::GlobalStore(const std::string& filename) : configuration_name(filename), bytes_read(0), total_values(0) {
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

    size_t nb_values = std::stoul(row.at(2));

    if(nb_values != stores.back()->nb_values()) {
      std::cerr << "Inconsistent number of values in the global configuration file and the store configuration file: " <<
        nb_values << " vs " << stores.back()->nb_values() << std::endl;
    }

    total_values += nb_values;
  }

}
