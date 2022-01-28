#include "search_index.h"

#include "database.h"

#include <future>
#include <thread>

void SearchIndex::open_from_config(const fs::path& base_path, const Config& config) {
  types_index_path = base_path / config["types_index"];
  na_index_path = base_path / config["na_index"];
  class_index_path = base_path / config["class_index"];
  vector_index_path = base_path / config["vector_index"];
  attributes_index_path = base_path / config["vector_index"];
  lengths_index_path = base_path / config["lengths_index"];
  classnames_index_path = base_path / config["classnames_index"];

  last_computed = std::stoul(config["index_last_computed"]);
  index_generated = std::stoi(config["index_generated"]) != 0;


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

    if(meta.length == 1) {
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





void SearchIndex::build_indexes(const Database& db) {
  // We dot no clear the indexes: indeed, we cannot remove values from the database

  //Parallelize on the 3 independent files
  // without std::cref, would actually copy!
  // In another thread, does not work!
  // unserialize tries to allocate, probably
  std::future<const std::vector<std::pair<std::string, roaring::Roaring64Map>>> results_value_fut = std::async( std::launch::deferred, build_indexes_values, std::cref(db), last_computed, db.nb_values());

  std::future<const std::vector<std::pair<std::string, roaring::Roaring64Map>>> results_meta_fut = std::async( std::launch::async, build_indexes_static_meta, std::cref(db), last_computed, db.nb_values());

  // Class names

  // Class names
  // classnames_index should be passed with std::ref if used with std::async
  auto results_classnames = build_indexes_classnames(db, classnames_index, last_computed, db.nb_values());
  class_index = results_classnames[0].second;

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
  int start = i;
  for(; i < start + lengths_index.size() ; i++) {
    assert(results_meta[i].first == "length_index");
    lengths_index[i] |= results_meta[i].second;
  }

  auto results_values = results_value_fut.get();
  na_index = results_values[0].second;




  //type_indexes[ANYSXP].addRange(0, n_values - 1);//addRange does not exist for RoaringBitmap64!!
  // But we can use flip!!
  types_index[ANYSXP].flip(0, db.nb_values()); // [a, b[

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

SearchIndex::~SearchIndex() {
  // Write all the indexes
  if(pid == getpid()) {
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
