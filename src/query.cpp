#include "query.h"

#include "database.h"


void Query::update(const Database& db) {
  const SearchIndex& search_index = db.search_index;
  if(!search_index.is_initialized() && search_index.last_computed < db.nb_values()) {
    Rf_error("Please build/update the search indexes to be able to sample with a complex query.\n");
  }

  if(!init&& !quiet) {
    Rprintf("Building the search index for the query.\n");
  }
  else if(init && search_index.new_elements && !quiet) {
    Rprintf("Updating the search index for the query.\n");
  }
  if(type != UNIONTYPE) {
    index_cache |= search_index.types_index[type];
  }
  else if(type == UNIONTYPE) {
    for(auto& desc : queries) {
      assert(desc.type != UNIONTYPE);
      index_cache |= search_index.types_index[desc.type];
    }
  }

  if(has_class && has_class.value()) {
    index_cache &= search_index.class_index;
  }
  else if(has_class && !has_class.value()) {
    roaring::Roaring64Map nonclass = search_index.class_index;

    // range end excluded
    nonclass.flip(safe_minimum(index_cache), index_cache.maximum() + 1);

    index_cache &= nonclass;
  }

  if(has_attributes && has_attributes.value()) {
    index_cache &= search_index.attributes_index;
  }
  else if(has_attributes && !has_attributes.value()) {
    roaring::Roaring64Map nonattributes = search_index.attributes_index;
    nonattributes.flip(safe_minimum(index_cache), index_cache.maximum() + 1);
    index_cache &= nonattributes;
  }
  //TODO: union
  /*else if(d.type == UNIONTYPE) {
   roaring::Roaring64Map attrindex;

   for(auto& desc : d.queries) {

   }
  }
   */

  if(is_vector && is_vector.value()) {
    index_cache &= search_index.vector_index;
  }
  else if(is_vector && !is_vector.value()) {
    roaring::Roaring64Map nonvector = search_index.vector_index;
    nonvector.flip(safe_minimum(index_cache), index_cache.maximum() + 1);
    index_cache &= nonvector;
  }

  if(has_na && has_na.value()) {
    index_cache &= search_index.na_index;
  }
  else if(has_na && !has_na.value()) {
    roaring::Roaring64Map nonna = search_index.na_index;
    nonna.flip(safe_minimum(index_cache), index_cache.maximum() + 1);
    index_cache &= nonna;
  }

  if(length) {
    auto low_bound = std::lower_bound(search_index.length_intervals.begin(), search_index.length_intervals.end(), length.value());
    if(low_bound == search_index.length_intervals.end()) {
      // That should never happen
      Rf_error("Cannot find the search index for the provided length %lu.\n", length);
    }
    int length_idx = std::distance(SearchIndex::length_intervals.begin(), low_bound);
    // Check if the index_cache in that slot represents one length or several ones
    // Either it is the last slot or the length difference is > 1
    if( length_idx == SearchIndex::nb_intervals - 1 || SearchIndex::length_intervals.at(length_idx + 1) - SearchIndex::length_intervals.at(length_idx) > 1) {
      // we manually build an index_cache for the given length
      // it will perform a linear search but there should not be more than 10^2 elements in those slots anyway
      roaring::Roaring64Map precise_length_index = search_index.search_length(db, search_index.lengths_index[length_idx], length.value());
      index_cache &= precise_length_index;
    }
    else {
      index_cache &= search_index.lengths_index[length_idx];
    }
  }

  for(const std::string& class_name : class_names) {
    std::optional<uint32_t> class_id = db.classes.get_class_id(class_name);

    if(class_id.has_value()) {
      auto res = search_index.classnames_index.get_index(*class_id);
      if(res.second) {
        index_cache &= res.first;
      }
      else {// we need to refine and search inside the bin
        auto precise_classname_index = search_index.search_classname(db, res.first, *class_id);
        index_cache &= precise_classname_index;
      }
    }
  }
  //index_cache.runOptimize(); // Maybe not worth it...
}
