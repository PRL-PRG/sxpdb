#include "sxpdb.h"

#include "database.h"

#include <algorithm>
#include <filesystem>
#include <chrono>
#include <cassert>

#define EMPTY_ORIGIN_PART ""

SEXP open_db(SEXP filename, SEXP mode_, SEXP quiet) {
  Database* db = nullptr;

  auto mode = Database::OpenMode::Read;

  if(TYPEOF(mode_) == LGLSXP) {
    mode = Rf_asLogical(mode_) ? Database::OpenMode::Write : Database::OpenMode::Read;
  }
  else if (TYPEOF(mode_) == STRSXP && Rf_length(mode_) > 0 && std::string(CHAR(STRING_ELT(mode_, 0))) == "merge") {
    mode = Database::OpenMode::Merge;
  }

  try{
    db = new Database(CHAR(STRING_ELT(filename, 0)), mode, Rf_asLogical(quiet));
  }
  catch(std::exception& e) {
    Rf_error("Error opening the database %s : %s\n", CHAR(STRING_ELT(filename, 0)), e.what());
  }
  if(db == nullptr) {
    Rf_error("Could not allocate memory for database %s", CHAR(STRING_ELT(filename, 0)));
  }
  SEXP db_ptr = PROTECT(R_MakeExternalPtr(db, Rf_install("sxpdb"), R_NilValue));

  // ASk to close the database when R session is closed.
  R_RegisterCFinalizerEx(db_ptr, (R_CFinalizer_t) close_db, TRUE);

  UNPROTECT(1);

  return db_ptr;
}


SEXP close_db(SEXP sxpdb) {

  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }

  Database* db = static_cast<Database*>(ptr);
  delete db;


  R_ClearExternalPtr(sxpdb);

  return R_NilValue;
}



SEXP add_val(SEXP sxpdb, SEXP val) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  try {

    auto hash = db->add_value(val, "", "", "", 0);

    if(hash.first != nullptr) {
      SEXP hash_s = PROTECT(Rf_allocVector(RAWSXP, sizeof(XXH128_hash_t)));
      Rbyte* bytes= RAW(hash_s);

      XXH128_canonicalFromHash(reinterpret_cast<XXH128_canonical_t*>(bytes), *hash.first);

      UNPROTECT(1);

      return hash_s;
    }
  }
  catch(std::exception& e) {
    Rf_error("Error adding value into the database: %s\n", e.what());
  }

  return R_NilValue;
}

SEXP add_val_origin_(SEXP sxpdb, SEXP val,
                     const char* package_name, const char* function_name, const char* argument_name,
                     uint64_t call_id) {

  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }

  if (package_name == nullptr) {
    package_name = EMPTY_ORIGIN_PART;
  }
  if (function_name == nullptr) {
    function_name = EMPTY_ORIGIN_PART;
  }
  if (argument_name == nullptr) {
    argument_name = EMPTY_ORIGIN_PART;
  }

  Database* db = static_cast<Database*>(ptr);

  try {

    auto hash = db->add_value(val, package_name, function_name, argument_name, call_id);

    if(hash.first != nullptr) {
      SEXP hash_s = PROTECT(Rf_allocVector(RAWSXP, sizeof(XXH128_hash_t)));
      Rbyte* bytes= RAW(hash_s);

      XXH128_canonicalFromHash(reinterpret_cast<XXH128_canonical_t*>(bytes), *hash.first);

      UNPROTECT(1);

      return hash_s;
    }
  }
  catch(std::exception& e) {
    Rf_error("Error adding value from package %s, function %s and argument %s, with call id %lu, into the database: %s\n",
             package_name, function_name, argument_name, call_id, e.what());
  }

  return R_NilValue;
}

SEXP add_origin_(SEXP sxpdb, const void* hash, const char* package_name, const char* function_name, const char* argument_name) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }

  if (package_name == nullptr) {
    package_name = EMPTY_ORIGIN_PART;
  }
  if (function_name == nullptr) {
    function_name = EMPTY_ORIGIN_PART;
  }
  if (argument_name == nullptr) {
    argument_name = EMPTY_ORIGIN_PART;
  }

  Database* db = static_cast<Database*>(ptr);

  auto idx = db->get_index(*static_cast<const sexp_hash*>(hash));
  if(!idx.has_value()) {
    Rf_error("Cannot add origin to value with given hash: no value with such a hash in the database.\n");
  }
  try {
    db->add_origin(*idx, package_name, function_name, argument_name);
  }
  catch(std::exception& e) {
    Rf_error("Error adding value from package %s, function %s and argument %s, into the database: %s\n",
             package_name, function_name, argument_name, e.what());
  }

  return R_NilValue;
}

SEXP add_origin(SEXP sxpdb, SEXP hash, SEXP package, SEXP function, SEXP argument) {
  const char *package_name = EMPTY_ORIGIN_PART;
  const char *function_name = EMPTY_ORIGIN_PART;
  const char *argument_name = EMPTY_ORIGIN_PART;

  if(TYPEOF(package) == STRSXP) {
    package_name = CHAR(STRING_ELT(package, 0));
  }
  else if (TYPEOF(package) == SYMSXP) {
    package_name = CHAR(PRINTNAME(package));
  }

  if(TYPEOF(function) == STRSXP) {
    function_name = CHAR(STRING_ELT(function, 0));
  }
  else if (TYPEOF(function) == SYMSXP) {
    function_name = CHAR(PRINTNAME(function));
  }

  if(TYPEOF(argument) == STRSXP) {
    // if empty string or NA, treat it as a return value
    SEXP arg_sexp = STRING_ELT(argument, 0);
    argument_name = (arg_sexp == NA_STRING) ? EMPTY_ORIGIN_PART : CHAR(arg_sexp);
  }
  else if (TYPEOF(argument) == SYMSXP) {
    argument_name = CHAR(PRINTNAME(argument));// a symbol cannot be NA
  }


  sexp_hash h = XXH128_hashFromCanonical(reinterpret_cast<XXH128_canonical_t*>(RAW(hash)));

  return add_origin_(sxpdb, &h, package_name, function_name, argument_name);
}

SEXP add_val_origin(SEXP sxpdb, SEXP val, SEXP package, SEXP function, SEXP argument, SEXP call_id) {
  const char *package_name = EMPTY_ORIGIN_PART;
  const char *function_name = EMPTY_ORIGIN_PART;
  const char *argument_name = EMPTY_ORIGIN_PART;

  if(TYPEOF(package) == STRSXP) {
    package_name = CHAR(STRING_ELT(package, 0));
  }
  else if (TYPEOF(package) == SYMSXP) {
    package_name = CHAR(PRINTNAME(package));
  }

  if(TYPEOF(function) == STRSXP) {
    function_name = CHAR(STRING_ELT(function, 0));
  }
  else if (TYPEOF(function) == SYMSXP) {
    function_name = CHAR(PRINTNAME(function));
  }

  if(TYPEOF(argument) == STRSXP) {
    // if empty string or NA, treat it as a return value
    SEXP arg_sexp = STRING_ELT(argument, 0);
    argument_name = (arg_sexp == NA_STRING) ? EMPTY_ORIGIN_PART : CHAR(arg_sexp);
  }
  else if (TYPEOF(argument) == SYMSXP) {
    argument_name = CHAR(PRINTNAME(argument));// a symbol cannot be NA
  }

  return add_val_origin_(sxpdb, val, package_name, function_name, argument_name, Rf_asInteger(call_id));
}

SEXP get_origins(SEXP sxpdb, SEXP hash_s) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }

  Database* db = static_cast<Database*>(ptr);

  sexp_hash hash;
  assert(Rf_length(hash_s) == sizeof(sexp_hash));
  hash = XXH128_hashFromCanonical(reinterpret_cast<XXH128_canonical_t*>(RAW(hash_s)));

  auto idx = db->get_index(hash);

  std::vector<std::tuple<std::string, std::string, std::string>> src_locs;
  if(idx.has_value()) {
    src_locs = db->source_locations(*idx);
  }

  const char*names[] = {"pkg", "fun", "param", ""};
  SEXP origs = PROTECT(Rf_mkNamed(VECSXP, names));

  SEXP packages = PROTECT(Rf_allocVector(STRSXP, src_locs.size()));
  SEXP functions = PROTECT(Rf_allocVector(STRSXP, src_locs.size()));
  SEXP arguments = PROTECT(Rf_allocVector(STRSXP, src_locs.size()));


  int i = 0;
  for(auto& loc : src_locs) {
    SET_STRING_ELT(packages, i, std::get<0>(loc) == "" ? NA_STRING : Rf_mkChar(std::get<0>(loc).data()));
    SET_STRING_ELT(functions, i, std::get<1>(loc) == "" ? NA_STRING : Rf_mkChar(std::get<1>(loc).data()));
    SET_STRING_ELT(arguments, i, std::get<2>(loc) == "" ? NA_STRING : Rf_mkChar(std::get<2>(loc).data()));
    i++;
  }

  SET_VECTOR_ELT(origs, 0, packages);
  SET_VECTOR_ELT(origs, 1, functions);
  SET_VECTOR_ELT(origs, 2, arguments);

  // make it a dataframe
  Rf_classgets(origs, Rf_mkString("data.frame"));// no need to protect because it is directly inserted

  // the data frame needs to have row names but we can put a special value for this attribute
  // see .set_row_names
  SEXP row_names = PROTECT(Rf_allocVector(INTSXP, 2));
  INTEGER(row_names)[0] = NA_INTEGER;
  INTEGER(row_names)[1] = -src_locs.size();

  Rf_setAttrib(origs, R_RowNamesSymbol, row_names);

  UNPROTECT(5);

  return origs;
}

SEXP get_origins_idx(SEXP sxpdb, SEXP idx) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }

  Database* db = static_cast<Database*>(ptr);


  auto src_locs = db->source_locations(Rf_asInteger(idx));

  const char*names[] = {"pkg", "fun", "param", ""};
  SEXP origs = PROTECT(Rf_mkNamed(VECSXP, names));

  SEXP packages = PROTECT(Rf_allocVector(STRSXP, src_locs.size()));
  SEXP functions = PROTECT(Rf_allocVector(STRSXP, src_locs.size()));
  SEXP arguments = PROTECT(Rf_allocVector(STRSXP, src_locs.size()));


  int i = 0;
  for(auto& loc : src_locs) {
    SET_STRING_ELT(packages, i, std::get<0>(loc) == "" ? NA_STRING : Rf_mkChar(std::get<0>(loc).data()));
    SET_STRING_ELT(functions, i, std::get<1>(loc) == "" ? NA_STRING : Rf_mkChar(std::get<1>(loc).data()));
    SET_STRING_ELT(arguments, i, std::get<2>(loc) == "" ? NA_STRING : Rf_mkChar(std::get<2>(loc).data()));
    i++;
  }

  SET_VECTOR_ELT(origs, 0, packages);
  SET_VECTOR_ELT(origs, 1, functions);
  SET_VECTOR_ELT(origs, 2, arguments);

  // make it a dataframe
  Rf_classgets(origs, Rf_mkString("data.frame"));// no need to protect because it is directly inserted

  // the data frame needs to have row names but we can put a special value for this attribute
  // see .set_row_names
  SEXP row_names = PROTECT(Rf_allocVector(INTSXP, 2));
  INTEGER(row_names)[0] = NA_INTEGER;
  INTEGER(row_names)[1] = -src_locs.size();

  Rf_setAttrib(origs, R_RowNamesSymbol, row_names);

  UNPROTECT(5);

  return origs;
}


SEXP have_seen(SEXP sxpdb, SEXP val) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  std::optional<uint64_t> idx = db->have_seen(val);
  if(idx.has_value()) {
    SEXP res = PROTECT(Rf_ScalarInteger(*idx));

    UNPROTECT(1);
    return res;
  }
  else {
    return R_NilValue;
  }
}


SEXP sample_val(SEXP sxpdb, SEXP query_ptr) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  if(Rf_isNull(query_ptr)) {
    return db->sample_value();
  }
  else {
    void* ptr = R_ExternalPtrAddr(query_ptr);
    if(ptr== nullptr) {
      Rf_warning("Query does not exist.\n");
      return R_NilValue;
    }
    Query* query = static_cast<Query*>(ptr);

    return db->sample_value(*query);
  }
}

SEXP sample_index(SEXP sxpdb, SEXP query_ptr) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  if(Rf_isNull(query_ptr)) {
    auto idx = db->sample_index();
    if(idx) {
      return Rf_ScalarInteger(*idx);
    }
    else {
      return R_NilValue;
    }
  }
  else {
    void* ptr = R_ExternalPtrAddr(query_ptr);
    if(ptr== nullptr) {
      Rf_warning("Query does not exist.\n");
      return R_NilValue;
    }
    Query* query = static_cast<Query*>(ptr);

    auto idx = db->sample_index(*query);
    if(idx) {
      return Rf_ScalarInteger(*idx);
    }
    else {
      return R_NilValue;
    }
  }
}

SEXP sample_similar(SEXP sxpdb, SEXP vals, SEXP multiple, SEXP relax) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  Query d;

  // should we consider vals as a list or as the result of list(...)?
  if(Rf_asLogical(multiple) == TRUE) {
    if(TYPEOF(vals) != VECSXP) {
      Rf_error("Should be a  list but is a %s.\n", Rf_type2char(TYPEOF(vals)));
    }

    if(Rf_length(vals) == 0) {
      return R_NilValue;
    }

    Query d = Query::from_value(VECTOR_ELT(vals, 0));
    for(int i = 1; i < Rf_length(vals) ; i++) {
      d = Query::unify(d, Query::from_value(VECTOR_ELT(vals, i)));
    }
  }
  else {

    d = Query::from_value(vals);

    std::string relax_param = "";
    for(int i = 0 ; i < Rf_length(relax) ; i++) {
      relax_param = CHAR(STRING_ELT(relax, i));
      if(relax_param == "na") {
        d.relax_na();
      }
      else if (relax_param == "vector") {
        d.relax_vector();
      }
      else if(relax_param == "length") {
        d.relax_length();
      }
      else if(relax_param == "attributes") {
        d.relax_attributes();
      }
      else if(relax_param == "ndims") {
        d.relax_ndims();
      }
      else if(relax_param == "class") {
        d.relax_class();
      }
      else if(relax_param == "type") {
        d.relax_type();
      }
      else if(relax_param == "keep_type") {
        d.relax_attributes();
        d.relax_class();
        d.relax_na();
        d.relax_ndims();
        d.relax_vector();
        d.relax_length();
      }
      else if(relax_param == "keep_class") {
        d.relax_attributes();
        d.relax_na();
        d.relax_ndims();
        d.relax_vector();
        d.relax_length();
        d.relax_type();
      }
    }
  }

  return db->sample_value(d);
}


SEXP get_val(SEXP sxpdb, SEXP i) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  int index = Rf_asInteger(i);
  return db->get_value(index);
}

SEXP merge_db(SEXP sxpdb1, SEXP sxpdb2) {
  void* ptr1 = R_ExternalPtrAddr(sxpdb1);
  if(ptr1== nullptr) {
    return R_NilValue;
  }
  Database* db1 = static_cast<Database*>(ptr1);

  void* ptr2 = R_ExternalPtrAddr(sxpdb2);
  if(ptr2== nullptr) {
    return R_NilValue;
  }
  Database* db2 = static_cast<Database*>(ptr2);

  uint64_t nb_new_values = 0;
  try{
    db1->parallel_merge_in(*db2, 150);
  }
  catch(std::exception& e) {
    Rf_error("Error merging database %s into %s: %s\n", db2->configuration_path().c_str(), db1->configuration_path().c_str(), e.what());
  }

  return Rf_ScalarInteger(db1->nb_values());
}

SEXP size_db(SEXP sxpdb) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  return Rf_ScalarInteger(db->nb_values());
}

SEXP get_meta(SEXP sxpdb, SEXP val) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  std::optional<uint64_t> idx = db->have_seen(val);

  if(idx.has_value()) {
    return db->get_metadata(*idx);
  }
  else {
    return R_NilValue;
  }
}

SEXP get_meta_idx(SEXP sxpdb, SEXP idx) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  return db->get_metadata(Rf_asInteger(idx));
}


SEXP path_db(SEXP sxpdb) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  // This returns the path of the description file
  // so we just need to get the dirname
  auto path = std::filesystem::absolute(db->configuration_path().parent_path());

  SEXP res = PROTECT(Rf_mkString(path.c_str()));

  UNPROTECT(1);

  return res;
}

SEXP check_db(SEXP sxpdb, SEXP slow) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  std::vector<size_t> errors = db->check(Rf_asLogical(slow));

  SEXP res = PROTECT(Rf_allocVector(INTSXP, errors.size()));

  std::copy_n(errors.begin(), errors.size(), INTEGER(res));

  UNPROTECT(1);

  return res;
}

SEXP map_db(SEXP sxpdb, SEXP fun, SEXP query_ptr) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  if(Rf_isNull(query_ptr)) {
    return db->map(fun);
  }
  else {
    void* ptr = R_ExternalPtrAddr(query_ptr);
    if(ptr== nullptr) {
      Rf_warning("Query does not exist.\n");
      return R_NilValue;
    }
    Query* query = static_cast<Query*>(ptr);

    return db->map(*query, fun);
  }
}

SEXP filter_index_db(SEXP sxpdb, SEXP fun, SEXP query_ptr) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  if(Rf_isNull(query_ptr)) {
    return db->filter_index(fun);
  }
  else {
    void* ptr = R_ExternalPtrAddr(query_ptr);
    if(ptr== nullptr) {
      Rf_warning("Query does not exist.\n");
      return R_NilValue;
    }
    Query* query = static_cast<Query*>(ptr);

    return db->filter_index(*query, fun);
  }
}


SEXP view_db(SEXP sxpdb, SEXP query_ptr) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  if(Rf_isNull(query_ptr)) {
    return db->view_values();
  }
  else {
    void* ptr = R_ExternalPtrAddr(query_ptr);
    if(ptr== nullptr) {
      Rf_warning("Query does not exist.\n");
      return R_NilValue;
    }
    Query* query = static_cast<Query*>(ptr);
    return db->view_values(*query);
  }
}

SEXP view_metadata(SEXP sxpdb, SEXP query_ptr) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  if(Rf_isNull(query_ptr)) {
    return db->view_metadata();
  }
  else {
    void* ptr = R_ExternalPtrAddr(query_ptr);
    if(ptr== nullptr) {
      Rf_warning("Query does not exist.\n");
      return R_NilValue;
    }
    Query* query = static_cast<Query*>(ptr);

    return db->view_metadata(*query);
  }
}

SEXP view_call_ids(SEXP sxpdb, SEXP query) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  return db->view_call_ids();
}

SEXP view_db_names(SEXP sxpdb, SEXP query) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  return db->view_db_names();
}


SEXP view_origins(SEXP sxpdb, SEXP query_ptr) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  if(Rf_isNull(query_ptr)) {
    return db->view_origins();
  }
  else {
    void* ptr = R_ExternalPtrAddr(query_ptr);
    if(ptr== nullptr) {
      Rf_warning("Query does not exist.\n");
      return R_NilValue;
    }
    Query* query = static_cast<Query*>(ptr);

    return db->view_origins(*query);
  }
}

SEXP build_indexes(SEXP sxpdb) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);
  db->build_indexes();

  return R_NilValue;
}


SEXP write_mode(SEXP sxpdb) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  return Rf_ScalarLogical(db->rw_mode());
}

SEXP values_from_origins(SEXP sxpdb, SEXP pkg, SEXP fun) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Database* db = static_cast<Database*>(ptr);

  std::string pkg_name = CHAR(STRING_ELT(pkg, 0));
  std::string fun_name = CHAR(STRING_ELT(fun, 0));

  return db->values_from_origin(pkg_name, fun_name);
}

SEXP query_from_value(SEXP value) {
  Query* query = new Query();
  *query = Query::from_value(value); // populate from the value

  SEXP query_ptr = PROTECT(R_MakeExternalPtr(query, Rf_install("query"), R_NilValue));

  // ASk to close the database when R session is closed.
  R_RegisterCFinalizerEx(query_ptr, (R_CFinalizer_t) close_query, TRUE);

  UNPROTECT(1);

  return query_ptr;
}

SEXP close_query(SEXP query) {
  void* ptr = R_ExternalPtrAddr(query);
  if(ptr== nullptr) {
    return R_NilValue;
  }

  Query* q = static_cast<Query*>(ptr);
  delete q;


  R_ClearExternalPtr(query);

  return R_NilValue;
}

SEXP relax_query(SEXP query_ptr, SEXP relax) {
  void* ptr = R_ExternalPtrAddr(query_ptr);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  Query* query = static_cast<Query*>(ptr);

  uint64_t n_before = query->nb_values();

  std::string relax_param = "";
  for(int i = 0 ; i < Rf_length(relax) ; i++) {
    relax_param = CHAR(STRING_ELT(relax, i));
    if(relax_param == "na") {
      query->relax_na();
    }
    else if (relax_param == "vector") {
      query->relax_vector();
    }
    else if(relax_param == "length") {
      query->relax_length();
    }
    else if(relax_param == "attributes") {
      query->relax_attributes();
    }
    else if(relax_param == "ndims") {
      query->relax_ndims();
    }
    else if(relax_param == "class") {
      query->relax_class();
    }
    else if(relax_param == "type") {
      query->relax_type();
    }
    else if(relax_param == "keep_type") {
      query->relax_attributes();
      query->relax_class();
      query->relax_na();
      query->relax_ndims();
      query->relax_vector();
      query->relax_length();
    }
    else if(relax_param == "keep_class") {
      query->relax_attributes();
      query->relax_na();
      query->relax_ndims();
      query->relax_vector();
      query->relax_length();
      query->relax_type();
    }
  }

  return Rf_ScalarLogical(query->nb_values() != n_before);
}


SEXP is_query_empty(SEXP query_ptr) {
  void* ptr = R_ExternalPtrAddr(query_ptr);
  if(ptr== nullptr) {
    Rf_warning("Query does not exist.\n");
    return R_NilValue;
  }
  Query* query = static_cast<Query*>(ptr);

  return Rf_ScalarLogical(query->nb_values() == 0);
}

SEXP extptr_tag(SEXP ptr) {
  return EXTPTR_TAG(ptr);
}


SEXP merge_all_dbs(SEXP db_paths, SEXP output_path, SEXP in_parallel) {
  fs::path db_path = fs::absolute(CHAR(STRING_ELT(output_path, 0))) / "cran_db";

  Rprintf("Starting merging\n");

    //Removes the db if it exists
  fs::remove_all(db_path);

  // Create the db directory 
  fs::create_directory(db_path);

  bool parallel = Rf_asLogical(in_parallel);

  // Open in write mode, and not quiet
  Database db(db_path / "sxpdb", Database::OpenMode::Write, false);


  // Progress bar setup
  int barWidth = 70;
  double progress = 0;

  size_t nb_paths = Rf_length(db_paths);

  // Some lists to store stats about the merging
  SEXP paths_c = PROTECT(Rf_allocVector(STRSXP, nb_paths));
  SEXP size_before_column = PROTECT(Rf_allocVector(INTSXP, nb_paths));
  SEXP added_values_column = PROTECT(Rf_allocVector(INTSXP, nb_paths));
  SEXP small_db_size_column = PROTECT(Rf_allocVector(INTSXP, nb_paths));
  SEXP small_db_bytes_column = PROTECT(Rf_allocVector(INTSXP, nb_paths));
  SEXP duration_column = PROTECT(Rf_allocVector(INTSXP, nb_paths));
  SEXP error_c = PROTECT(Rf_allocVector(STRSXP, nb_paths));

  int* s_before_c = INTEGER(size_before_column);
  int* added_v_c = INTEGER(added_values_column);
  int* small_db_s_c = INTEGER(small_db_size_column);
  int* small_db_b_c = INTEGER(small_db_bytes_column);
  int* dur_c = INTEGER(duration_column);

  for(size_t i = 0; i < nb_paths ; i++) {
    fs::path small_db_path = fs::path(CHAR(STRING_ELT(db_paths, i)));

    fs::path lock_path = small_db_path / ".LOCK";

    try {
      if(fs::exists(lock_path)) {
        // the small db is corrupted, so do not try to open
        throw std::runtime_error("Database is corrupted: lock file is present.\n");
      }

      std::cout << "[";

      uint64_t size_before = db.nb_values();
      SET_STRING_ELT(paths_c, i, STRING_ELT(db_paths, i));
      s_before_c[i] = size_before;
      std::chrono::microseconds elapsed = std::chrono::microseconds::zero();
      auto start = std::chrono::system_clock::now();
      Database small_db(small_db_path / "sxpdb", Database::OpenMode::Merge, false);
      uint64_t small_db_size = small_db.nb_values();


      if(small_db_size > 0) {
        if(parallel) {
          db.parallel_merge_in(small_db, 150);
        }
        else {
          db.merge_in(small_db);
        }
      }

      auto end = std::chrono::system_clock::now();
      elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

      added_v_c[i] = db.nb_values() - size_before;
      small_db_s_c[i] = small_db_size;
      small_db_b_c[i] = directory_size(fs::path(CHAR(STRING_ELT(db_paths, i))));
      dur_c[i] = elapsed.count();
      SET_STRING_ELT(error_c, i, NA_STRING);
    }
    catch(std::exception& e) {
      added_v_c[i] = 0;
      small_db_s_c[i] = NA_INTEGER;
      small_db_b_c[i] = NA_INTEGER;
      dur_c[i] = 0;
      SET_STRING_ELT(error_c, i, Rf_mkChar(e.what()));
    }

    // Progress bar update
    progress = (double) i / (double) nb_paths;
    int pos = barWidth * progress;
    for (int j = 0; j < barWidth; ++j) {
        if (j < pos) std::cout << "=";
        else if (j == pos) std::cout << ">";
        else std::cout << " ";
    }
    std::cout << "] " << int(progress * 100.0) << " %\r";
    std::cout.flush();

  }
  std::cout << std::endl;

  SEXP df = PROTECT(create_data_frame({
    {"path", paths_c},
    {"db_size_before", size_before_column},
    {"added_values", added_values_column},
    {"small_db_size", small_db_size_column},
    {"small_db_bytes", small_db_bytes_column},
    {"duration", duration_column},
    {"error", error_c}
  }));

  UNPROTECT(8);

  return df;

}