#include "search_index.h"

#include "database.h"

void SearchIndex::open_from_config(const fs::path& base_path, const Config& config) {
  types_index_path = base_path / config["types_index"];
  na_index_path = base_path / config["na_index"];
  class_index_path = base_path / config["class_index"];
  vector_index_path = base_path / config["vector_index"];
  attributes_index_path = base_path / config["vector_index"];
  lengths_index_path = base_path / config["lengths_index"];
  classnames_index_path = base_path / config["classnames_index"];

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


const std::vector<std::pair<std::string, roaring::Roaring64Map>> SearchIndex::build_index(const Database& db, uint64_t start, uint64_t end) {
  std::vector<std::pair<std::string,  roaring::Roaring64Map>> results(SearchIndex::nb_sexptypes + 4 + SearchIndex::nb_intervals);
  int k = 0;
  for(k =0 ; k < SearchIndex::nb_sexptypes ; k++) {
    results[k].first = "type_index";
  }
  results[k].first = "na_index";
  k++;
  results[k].first = "class_index";
  k++;
  results[k].first = "vector_index";
  k++;
  results[k].first = "attributes_index";
  k++;
  int beg = k;
  for(;k < beg + SearchIndex::nb_intervals; k++) {
    results[k].first = "length_index";
  }

  //TODO:
  // start at max(start, index.maximum())
  // Indeed, we cannot remove values from the database
  // And values are appended...

  // Static meta
  for(uint64_t i = start; i < end; i++) {
    auto meta = db.static_meta.read(i);
    results[meta.sexptype].second.add(i);

    if(meta.length == 1) {
      results[SearchIndex::nb_sexptypes + 2].second.add(i);
    }

    if(meta.n_attributes > 0) {
      results[SearchIndex::nb_sexptypes + 3].second.add(i);
    }


    int length_idx = std::lower_bound(SearchIndex::length_intervals.begin(), SearchIndex::length_intervals.end(), meta.length) - SearchIndex::length_intervals.begin();
    results[SearchIndex::nb_sexptypes + 4  + length_idx].second.add(i);
  }

  // from the values
  for(uint64_t i = start; i < end ; i++) {
    // TODO
    // We should add a mutex here, or create a specialized mutex enabled read
    std::vector<std::byte> buf = db.sexp_table.read(i);
    SEXP val = PROTECT(db.ser.unserialize(buf));// That might not be thread-safe either...

    if(find_na(val)) {
      results[SearchIndex::nb_sexptypes].second.add(i);
    }

    SEXP klass = Rf_getAttrib(val, R_ClassSymbol);
    if(klass != R_NilValue) {
      results[SearchIndex::nb_sexptypes + 1].second.add(i);
    }

    UNPROTECT(1);
  }

  return results;
}



void SearchIndex::build_indexes(const Database& db) {
  // We dot no clear the indexes: indeed, we cannot remove values from the database

  //TODO: only do it for the ones that are actually reading from the value store, because ifstream is not thread-safe.
  // So we also need to add a mutex... for the value store
  // And load the metadata into memory to build the index?
  // Then we can do concurrent accesses of it without problems...

  // Would be better if we could use the new execution policies of C++17, so we could do it at the level
  // of individual value and use work stealing



  std::vector<std::vector<std::pair<std::string, roaring::Roaring64Map>>> results;

  results.push_back(build_index(db, last_computed, db.nb_values()));

  for(const auto& indexes : results) {
    assert(indexes.size() == types_index.size() + 4 + lengths_index.size());
    int i = 0;
    for(; i < types_index.size() ; i ++) {
      assert(indexes[i].first == "type_index");
      types_index[i] |= indexes[i].second;
    }
    assert(indexes[i].first == "na_index");
    na_index |= indexes[i].second;
    i++;
    assert(indexes[i].first == "class_index");
    class_index |= indexes[i].second;
    i++;
    assert(indexes[i].first == "vector_index");
    vector_index |= indexes[i].second;
    i++;
    assert(indexes[i].first == "attributes_index");
    attributes_index |= indexes[i].second;
    i++;
    int start = i;
    for(; i < start + lengths_index.size() ; i++) {
      assert(indexes[i].first == "length_index");
      lengths_index[i] |= indexes[i].second;
    }
  }

  // Optimize all the indexes


  //type_indexes[ANYSXP].addRange(0, n_values - 1);//addRange does not exist for RoaringBitmap64!!
  // But we can use flip!!
  types_index[ANYSXP].flip(0, db.nb_values()); // [a, b[

  for(auto& ind : types_index) {
    ind.runOptimize();
    ind.shrinkToFit();
  }

  na_index.runOptimize();
  na_index.shrinkToFit();
  class_index.runOptimize();
  class_index.shrinkToFit();
  vector_index.runOptimize();
  vector_index.shrinkToFit();
  attributes_index.runOptimize();
  attributes_index.shrinkToFit();

  for(auto& ind : lengths_index) {
    ind.runOptimize();
    ind.shrinkToFit();
  }

  // Class names
  // from the classnames
  for(uint64_t i = 0; i < db.nb_values(); i++) {
    const std::vector<uint32_t>& class_ids =  db.classes.get_classnames(i);

    for(uint32_t class_id : class_ids) {
      classnames_index.add_property(i, class_id);
    }
  }
  classnames_index.finalize_indexes();


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
