#include "source_ref.h"

#include "csv_file.h"

#include <sstream>

SourceRefs::SourceRefs(fs::path config_path) {
  // load the configuration and srcrefs if they exist
  if(std::filesystem::exists(config_path)) {
    load_configuration();
    load_store();
  }else {

  }
}

void SourceRefs::write_configuration() {
  CSVFile file;

  std::vector<std::string> row(3);

  for(auto& package : packages) {
    if(package.second.offset < 0) {
      exit(1); // offset should be set
      // You should call write before write_configuration!!
    }
    row[0] = package.first;
    row[1] = package.second.offset;
    row[2] = package.second.srcrefs.size();

    file.add_row(std::move(row));
  }

  file.write(config_path);
}


void SourceRefs::write() {

  // TODO: should be smarter at no writing again what already is there
  std::ofstream src_refs_file(source_refs_path, std::ofstream::trunc | std::ofstream::binary);

  for(auto& package : packages) {
    package_t& package_infos = package.second;
    package_infos.offset = src_refs_file.tellp();// position of writing
    src_refs_file << package.first << ",";// name

    size_t size = package_infos.srcrefs.size();
    src_refs_file.write(reinterpret_cast<char*>(&size), sizeof(size_t));

    for(auto& srcref : package_infos.srcrefs) {
      src_refs_file << srcref.function_name << ",";
      src_refs_file << srcref.argument_name << ",";

      // Write the values
      size = srcref.values.size();
      src_refs_file.write(reinterpret_cast<char*>(&size), sizeof(size_t));
      for(auto& value : srcref.values) {
        src_refs_file << value.first << ",";

        size_t offset = value.second;
        src_refs_file.write(reinterpret_cast<char*>(&offset), sizeof(size_t));
      }
    }
  }
}

void SourceRefs::load_store() {
  std::ifstream src_refs_file(source_refs_path, std::ifstream::binary);

  // We assume that load_configuration has been called before

  for(auto&package : packages) {
    src_refs_file.seekg(package.second.offset);

    std::string buf;
    std::getline(src_refs_file, buf, ',');
    if(package.first != buf) {
      std::cerr << "Package name in the configuration file and in the store do not match: " << package.first << " vs " << buf << std::endl;
      exit(1);
    }


    size_t nb_srcrefs=0;
    src_refs_file.read(reinterpret_cast<char*>(&nb_srcrefs), sizeof(size_t));

    auto& srcrefs = package.second.srcrefs;
    srcrefs.reserve(nb_srcrefs);

    for(size_t i = 0; i < nb_srcrefs; i++) {
      src_ref_t srcref;
      std::getline(src_refs_file, srcref.function_name, ',');
      std::getline(src_refs_file, srcref.argument_name, ',');

      size_t nb_values=0;
      src_refs_file.read(reinterpret_cast<char*>(&nb_values), sizeof(size_t));

      srcref.values.reserve(nb_values);
      for(size_t j = 0; j < nb_values ; j++) {
          std::string store_name;
          std::getline(src_refs_file, store_name, ',');

          size_t offset = 0;
          src_refs_file.read(reinterpret_cast<char*>(&offset), sizeof(size_t));

          srcref.values.push_back(std::make_pair(store_name, offset));
      }
      srcrefs.push_back(srcref);
    }
  }
}

void SourceRefs::load_configuration() {
  CSVFile file(config_path);

  for(auto& row : file.get_rows()) {
    package_t package;
    package.offset = std::stoul(row.at(1));
    // package.srcrefs.reserve(std::stoul(row.at(2))); // we do it later during the loading of the store
    // But we could do it now and then check using capacity...
    packages[row.at(0)] = package;
  }
}
