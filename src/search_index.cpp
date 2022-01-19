#include "search_index.h"

#include "database.h"


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
    assert(indexes[i].first == "na_index");
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

