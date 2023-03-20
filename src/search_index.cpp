#include "search_index.h"

#include "database.h"

#include "thread_pool.h"

#include <future>
#include <thread>
#include <chrono>
using namespace std::chrono_literals;

void SearchIndex::open_from_config(const fs::path& base_path, const Config& config) {
  last_computed = std::stoul(config["index_last_computed"]);
  index_generated = std::stoi(config["index_generated"]) != 0;

  if(!index_generated) {
    return;
  }


  types_index_path = base_path / config["types_index"];
  na_index_path = base_path / config["na_index"];
  class_index_path = base_path / config["class_index"];
  vector_index_path = base_path / config["vector_index"];
  attributes_index_path = base_path / config["vector_index"];
  lengths_index_path = base_path / config["lengths_index"];
  classnames_index_path = base_path / config["classnames_index"];
  packages_index_path = base_path / config["packages_index"];
  functions_index_path = base_path / config["functions_index"];


  if(!types_index_path.empty()) {
    for(int i = 0; i < 25; i++) {
      types_index[i] = read_index(types_index_path.parent_path() / (types_index_path.stem().string() + "_" + std::to_string(i) + ".ror"));
      new_elements = true;
    }
  }

  if(!na_index_path.empty()) {
    na_index = read_index(na_index_path);
    new_elements = true;
  }

  if(!class_index_path.empty()) {
    class_index = read_index(class_index_path);
    new_elements = true;
  }

  if(!vector_index_path.empty()) {
    vector_index = read_index(vector_index_path);
    new_elements = true;
  }

  if(!attributes_index_path.empty()) {
    attributes_index = read_index(attributes_index_path);
    new_elements = true;
  }

  if(!lengths_index_path.empty()) {
    for(int i = 0; i < nb_intervals; i++) {
      lengths_index[i] = read_index(lengths_index_path.parent_path() / (lengths_index_path.stem().string() + "_" + std::to_string(i) + ".ror"));
      new_elements = true;
    }
  }

  if(!classnames_index_path.empty()) {
    classnames_index.open(classnames_index_path);
    new_elements = true;
  }

  if(!packages_index_path.empty()) {
    int nb_packages = 0;
    // Count how many package indexes there are.
    for(auto dir : fs::directory_iterator(packages_index_path.parent_path())) {
      if(dir.is_regular_file()) {
        auto path = dir.path();
        if(path.extension() == ".ror") {
          std::string name = path.stem().string();
          // Check if it is a package index
          if(name.rfind("packages_index", 0) == 0) {
            nb_packages++;
          }
        }
      }
    }
    packages_index.resize(nb_packages);
    for(int i = 0; i < nb_packages; i++) {
      packages_index[i] = read_index(packages_index_path.parent_path() / (packages_index_path.stem().string() + "_" + std::to_string(i) + ".ror"));
      new_elements = true;
    }
  }

   if(!functions_index_path.empty()) {
    for(auto dir : fs::directory_iterator(functions_index_path.parent_path())) {
      if(dir.is_regular_file()) {
        auto path = dir.path();
        if(path.extension() == ".ror") {
          std::string name = path.stem().string();
          // Check if it is a function index
          if(name.rfind("functions_index", 0) == 0) {
            // Extract the max index of the function from the bin
            uint64_t nim_fun = std::stoul(name.substr(name.rfind('_') + 1));
            //TODO: we should actually replace the index instead of adding it (suggests to use a hashtable...)
            function_index.push_back({nim_fun, read_index(path)});
            new_elements = true;
          }
        }
      }
    }
  }

}


const std::vector<std::pair<std::string, roaring::Roaring64Map>> SearchIndex::build_indexes_static_meta(const Database& db, uint64_t start, uint64_t end) {
  std::vector<std::pair<std::string,  roaring::Roaring64Map>> results(SearchIndex::nb_sexptypes + 2 + SearchIndex::nb_intervals);
  int k = 0;
  for(k =0 ; k < SearchIndex::nb_sexptypes ; k++) {
    results[k].first = "type_index";
  }
  results[k].first = "vector_index";
  k++;
  results[k].first = "attributes_index";
  k++;
  int beg = k;
  for(;k < beg + SearchIndex::nb_intervals; k++) {
    results[k].first = "length_index";
  }

  for(uint64_t i = start; i < end; i++) {
    auto meta = db.static_meta.read(i);
    results[meta.sexptype].second.add(i);

    if(meta.length != 1) {
      results[SearchIndex::nb_sexptypes].second.add(i);
    }

    if(meta.n_attributes > 0) {
      results[SearchIndex::nb_sexptypes + 1].second.add(i);
    }

    auto it = std::lower_bound(SearchIndex::length_intervals.begin(), SearchIndex::length_intervals.end(), meta.length);
    int length_idx = SearchIndex::length_intervals.size() - 1;
    if(it != SearchIndex::length_intervals.end()) {
      length_idx = it - SearchIndex::length_intervals.begin();
    }

    results[SearchIndex::nb_sexptypes + 2  + length_idx].second.add(i);
  }

  for(auto& result : results) {
    result.second.runOptimize();
    result.second.shrinkToFit();
  }

  return results;
}


const std::vector<std::pair<std::string, roaring::Roaring64Map>> SearchIndex::build_indexes_values(const Database& db, uint64_t start, uint64_t end) {
  std::vector<std::pair<std::string, roaring::Roaring64Map>> results;
  results.push_back({"na_index",roaring::Roaring64Map()});



  for(uint64_t i = start; i < end ; i++) {
    const std::vector<std::byte>& buf = db.sexp_table.read(i);

    const sexp_view_t sexp_view = Serializer::unserialize_view(buf);

    if(find_na(sexp_view)) {
      results[0].second.add(i);
    }

  }

  for(auto& result : results) {
    result.second.runOptimize();
    result.second.shrinkToFit();
  }

  return results;
}

const std::vector<std::pair<std::string, roaring::Roaring64Map>> SearchIndex::build_values(const std::vector<std::vector<std::byte>>& bufs, uint64_t start) {
  std::vector<std::pair<std::string, roaring::Roaring64Map>> results;
  results.push_back({"na_index",roaring::Roaring64Map()});

  uint64_t i = start;
  for(const auto& buf : bufs) {
    const sexp_view_t sexp_view = Serializer::unserialize_view(buf);

    if(find_na(sexp_view)) {
      results[0].second.add(i);
    }

    i++;
  }

  for(auto& result : results) {
    result.second.runOptimize();
    result.second.shrinkToFit();
  }

  return results;
}

const std::vector<std::pair<std::string, roaring::Roaring64Map>> SearchIndex::build_indexes_classnames(const Database& db, ReverseIndex& classnames_index, uint64_t start, uint64_t end) {
  std::vector<std::pair<std::string, roaring::Roaring64Map>> results;
  results.push_back({"class_index",roaring::Roaring64Map()});


  // Class names
 classnames_index.prepare_indexes(db.classes.nb_classnames() + 1);
  for(uint64_t i = start; i < end; i++) {
    const std::vector<uint32_t>& class_ids =  db.classes.get_classnames(i);

    for(uint32_t class_id : class_ids) {
      classnames_index.add_property(i, class_id);
    }

    if(class_ids.size() > 0) {
      results[0].second.add(i);
    }
  }
  classnames_index.finalize_indexes();

  for(auto& result : results) {
    result.second.runOptimize();
    result.second.shrinkToFit();
  }

  return results;
}

const  std::vector<std::pair<std::string, std::vector<std::pair<uint32_t, roaring::Roaring64Map>>>>  SearchIndex::build_indexes_origins(const Database& db, uint64_t start, uint64_t end) {
    uint32_t nb_packages = db.origins.nb_packages() + 1;// we want to count the empty one
    uint32_t nb_functions = db.origins.nb_functions() + 1;
    std::vector<std::pair<std::string, std::vector<std::pair<uint32_t, roaring::Roaring64Map>>>> results;
    results.push_back({"packages_index", std::vector<std::pair<uint32_t, roaring::Roaring64Map>>(nb_packages)});
    results.push_back({"functions_index", std::vector<std::pair<uint32_t, roaring::Roaring64Map>>()});

    std::vector<roaring::Roaring64Map> funcs(nb_functions);

    for(uint64_t i = start; i < end ; i++) {
      auto locs = db.origins.get_locs(i);
      for(auto loc : locs) {
        results[0].second[loc.package].second.add(i);//packages
        funcs[loc.function].add(i);
      }
    }

    // Merge the function indexes into intervals
    // No more than 100 000 values per slot? Or 10 000?
    // for 400 packages, we had about 36 000 functions
    // and 39e6 unique values. So in average 1 000 unique values per
    // function, probably with outliers
    uint32_t j = 0;
    roaring::Roaring64Map current_index;
    for(const auto& fun : funcs) {
      if(current_index.cardinality() > 10000) {
        results[1].second.push_back({j, current_index});
        current_index.clear();
      }
      current_index |= fun;
      j++;
    }
    if(!current_index.isEmpty()) {
      results[1].second.push_back({j, current_index});
    }

    return results;
}





void SearchIndex::build_indexes(const Database& db) {
  // We dot no clear the indexes: indeed, we cannot remove values from the database

  thread_pool pool(std::thread::hardware_concurrency() - 1);

  auto results_meta_fut = pool.submit(build_indexes_static_meta, std::cref(db), last_computed, db.nb_values());
  auto results_classnames_fut = pool.submit(build_indexes_classnames, std::cref(db), std::ref(classnames_index), last_computed, db.nb_values());
  auto results_origins_fut = pool.submit(build_indexes_origins, std::cref(db), last_computed, db.nb_values());
  std::future<const std::vector<std::pair<std::string, roaring::Roaring64Map>>> results_value_fut = std::async( std::launch::deferred, build_indexes_values, std::cref(db), last_computed, db.nb_values());


  // Values
  std::vector<std::future<const std::vector<std::pair<std::string, roaring::Roaring64Map>>>> results_values_fut;

  const uint64_t chunk_size = fs::file_size(db.sexp_table.get_path()) / (std::thread::hardware_concurrency() - 1);
  const uint64_t elements_per_chunk = db.nb_values() / (std::thread::hardware_concurrency() - 1); // a rough estimate...
  if(elements_per_chunk == 0) {// it means that there are more cores than values in the db!
    elements_per_chunk - db.nb_values();
  }

  // TODO: we can now parallelize the read operation!
  std::vector<std::vector<std::byte>> bufs;
  bufs.reserve(elements_per_chunk);
  uint64_t size = 0;
  uint64_t start = 0;//TODO: check that start is actually the start of a sequence of bufs
  for(uint64_t i = 0; i < db.nb_values() ; i++) {
    const std::vector<std::byte>& buf = db.sexp_table.read(i);
    size += buf.size();
    bufs.push_back(buf);
    if(size >= chunk_size || i == db.nb_values() - 1) {
      results_values_fut.push_back(pool.submit(build_values, std::move(bufs), start));
      start = i + 1;
      bufs.clear();
    }
  }

#ifndef NDEBUG
  if (!db.is_quiet()) Rprintf("Building indexes in parallel.\n");
  std::future_status meta_status;
  std::future_status value_status;
  std::future_status classname_status;
  auto start_time = std::chrono::steady_clock::now();
  do {
    if(meta_status != std::future_status::ready) {
      meta_status = results_meta_fut.wait_for(333ms);
      if(meta_status == std::future_status::ready) {
        auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time);
        if (!db.is_quiet()) Rprintf("Computations on metadata have finished in %ld ms.\n", dur.count());
      }
    }
    if(classname_status != std::future_status::ready) {
      classname_status= results_classnames_fut.wait_for(333ms);
      if(classname_status == std::future_status::ready) {
        auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time);
        if (!db.is_quiet()) Rprintf("Computations on classnames have finished in %ld ms.\n", dur.count());
      }
    }
    if(value_status != std::future_status::ready) {
      // that last task could actually finish before another one in the vector....
      value_status = results_values_fut[results_values_fut.size() - 1].wait_for(333ms);
      if(value_status == std::future_status::ready) {
        auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time);
        if (!db.is_quiet()) Rprintf("Computations on values have finished in %ld ms.\n", dur.count());
      }
    }
  } while (meta_status != std::future_status::ready && value_status != std::future_status::ready && classname_status!= std::future_status::ready);
#endif

  for(auto& fut : results_values_fut) {
    auto results = fut.get();
    assert(results[0].first == "na_index");
    na_index |= results[0].second;
  }

  // Class names
  // classnames_index should be passed with std::ref if used with std::async
  auto results_classnames = results_classnames_fut.get();
  class_index |= results_classnames[0].second;

  auto results_meta = results_meta_fut.get();

  assert(results_meta.size() == types_index.size() + 2 + lengths_index.size());
  int i = 0;
  for(; i < types_index.size() ; i ++) {
    assert(results_meta[i].first == "type_index");
    types_index[i] |= results_meta[i].second;
  }
  assert(results_meta[i].first == "vector_index");
  vector_index |= results_meta[i].second;
  i++;
  assert(results_meta[i].first == "attributes_index");
  attributes_index |= results_meta[i].second;
  i++;
  for(int j = 0; j < lengths_index.size() ; j++) {
    assert(results_meta[j + i].first == "length_index");
    lengths_index[j] |= results_meta[j + i].second;
  }

  // Origins
  auto results_origins = results_origins_fut.get();
  // get package index
  assert(results_origins[0].first == "packages_index");
  packages_index.resize(results_origins[0].second.size());
  for(int i = 0; i < results_origins[0].second.size(); i++) {
      packages_index[i] = results_origins[0].second[i].second;
  }
  // get function index
  assert(results_origins[1].first == "functions_index");
  function_index.resize(results_origins[1].second.size());
  for(int i = 0; i < results_origins[1].second.size(); i++) {
      function_index[i] = results_origins[1].second[i];
  }


  types_index[ANYSXP].addRange(0, db.nb_values()); // [a, b[

  index_generated = true;
  last_computed = db.nb_values();
}

roaring::Roaring64Map SearchIndex::search_length(const Database& db, const roaring::Roaring64Map& bin_index, uint64_t precise_length) const {
  roaring::Roaring64Map precise_index;

  for(uint64_t i : bin_index) {
    const auto& meta = db.static_meta.read(i);

    if(meta.length == precise_length) {
      precise_index.add(i);
    }
  }

  return precise_index;
}

roaring::Roaring64Map SearchIndex::search_classname(const Database& db, const roaring::Roaring64Map& bin_index, uint32_t precise_classname) const {
  roaring::Roaring64Map precise_index;

  for(uint64_t i : bin_index) {
    const std::vector<uint32_t> class_ids = db.classes.get_classnames(i);

    if(std::find(class_ids.begin(), class_ids.end(), precise_classname) != class_ids.end()) {
      precise_index.add(i);
    }
  }

  return precise_index;
}

roaring::Roaring64Map SearchIndex::search_function(const Database& db, const roaring::Roaring64Map& fun_index, uint32_t precise_fun) const {
  roaring::Roaring64Map precise_index;

  for(uint64_t i : fun_index) {
    auto locs = db.origins.get_locs(i);
    for(auto loc : locs) {
      if(loc.function == precise_fun) {
        precise_index.add(i);
        break;
      }
    }
  }
  return precise_index;
}

SearchIndex::~SearchIndex() {
  // Write all the indexes
  if(pid == getpid() && write_mode && index_generated) {
    for(int i = 0 ; i < types_index.size() ; i++) {
      write_index(types_index_path.parent_path() / (types_index_path.stem().string()  + "_" + std::to_string(i) + ".ror"), types_index[i]);
    }

    write_index(na_index_path, na_index);
    write_index(class_index_path, class_index);
    write_index(vector_index_path, vector_index);
    write_index(attributes_index_path, attributes_index);

    for(int i = 0; i < lengths_index.size() ; i++) {
      write_index(lengths_index_path.parent_path() / (lengths_index_path.stem().string() + "_" + std::to_string(i) + ".ror"), lengths_index[i]);
    }

    classnames_index.write(classnames_index_path);

    for(int i = 0; i < packages_index.size() ; i++) {
      write_index(packages_index_path.parent_path() / (packages_index_path.stem().string() + "_" + std::to_string(i) + ".ror"), packages_index[i]);
    }

    for(int i = 0; i < function_index.size() ; i++) {
      write_index(functions_index_path.parent_path() / (functions_index_path.stem().string() + "_" + std::to_string(function_index[i].first) + ".ror"), function_index[i].second);
    }
  }
}


roaring::Roaring64Map read_index(const fs::path& path) {
  std::ifstream index_file(path, std::fstream::binary);
  if(!index_file) {
    Rf_error("Index file %s does not exist.\n", path.c_str());
  }

  //get length of file
  index_file.seekg(0, std::ios::end);
  size_t length = index_file.tellg();
  index_file.seekg(0, std::ios::beg);

  std::vector<char> buf;
  buf.reserve(length);
  std::copy( std::istreambuf_iterator<char>(index_file),
             std::istreambuf_iterator<char>(),
             std::back_inserter(buf) );

  return roaring::Roaring64Map::read(buf.data(), true);
}

void write_index(const fs::path& path, const roaring::Roaring64Map& index) {
  std::ofstream index_file(path, std::fstream::binary | std::fstream::trunc);

  if(!index_file) {
    Rf_error("Cannot create index file %s: %s.\n", path.c_str(), strerror(errno));
  }

  size_t size = index.getSizeInBytes();
  std::vector<char> buf(size);
  size_t written = index.write(buf.data(), true);

  if(size != written) {
    Rf_error("Incorrect number of bytes written for index %s: expected = %lu vs actual =%lu.\n", path.c_str(), size, written);
  }

  index_file.write(buf.data(), buf.size());
}
