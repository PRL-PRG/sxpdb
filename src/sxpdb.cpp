#include "sxpdb.h"

#include "global_store.h"

#include <algorithm>
#include <filesystem>
#include <cassert>

#define EMPTY_ORIGIN_PART ""

SEXP open_db(SEXP filename, SEXP quiet) {
  GlobalStore* db = nullptr;
  try{
    db = new GlobalStore(CHAR(STRING_ELT(filename, 0)), Rf_asLogical(quiet));
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

  GlobalStore* db = static_cast<GlobalStore*>(ptr);
  delete db;


  R_ClearExternalPtr(sxpdb);

  return R_NilValue;
}



SEXP add_val(SEXP sxpdb, SEXP val) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  auto hash = db->add_value(val);

  if(hash.first != nullptr) {
    SEXP hash_s = PROTECT(Rf_allocVector(RAWSXP, sizeof(XXH128_hash_t)));
    Rbyte* bytes= RAW(hash_s);

    XXH128_canonicalFromHash(reinterpret_cast<XXH128_canonical_t*>(bytes), *hash.first);

    UNPROTECT(1);

    return hash_s;
  }

  return R_NilValue;
}

SEXP add_val_origin_(SEXP sxpdb, SEXP val,
                     const char* package_name, const char* function_name, const char* argument_name) {

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

  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  try {

    auto hash = db->add_value(val, package_name, function_name, argument_name);

    if(hash.first != nullptr) {
      SEXP hash_s = PROTECT(Rf_allocVector(RAWSXP, sizeof(XXH128_hash_t)));
      Rbyte* bytes= RAW(hash_s);

      XXH128_canonicalFromHash(reinterpret_cast<XXH128_canonical_t*>(bytes), *hash.first);

      UNPROTECT(1);

      return hash_s;
    }
  }
  catch(std::exception& e) {
    Rf_error("Error adding value from package %s, function %s and argument %s, into the database: %s\n",
             package_name, function_name, argument_name, e.what());
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

  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  try {
    return Rf_ScalarLogical(db->add_origins(*static_cast<const sexp_hash*>(hash), package_name, function_name, argument_name));
  }
  catch(std::exception& e) {
    Rf_error("Error adding value from package %s, function %s and argument %s, into the database: %s\n",
             package_name, function_name, argument_name, e.what());
  }

  return Rf_ScalarLogical(FALSE);
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

SEXP add_val_origin(SEXP sxpdb, SEXP val, SEXP package, SEXP function, SEXP argument) {
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

  return add_val_origin_(sxpdb, val, package_name, function_name, argument_name);
}

SEXP get_origins(SEXP sxpdb, SEXP hash_s) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }

  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  sexp_hash hash;
  assert(Rf_length(hash_s) == sizeof(sexp_hash));
  hash = XXH128_hashFromCanonical(reinterpret_cast<XXH128_canonical_t*>(RAW(hash_s)));

  auto src_locs = db->source_locations(hash);

  const char*names[] = {"pkg", "fun", "param", ""};
  SEXP origs = PROTECT(Rf_mkNamed(VECSXP, names));

  SEXP packages = PROTECT(Rf_allocVector(STRSXP, src_locs.size()));
  SEXP functions = PROTECT(Rf_allocVector(STRSXP, src_locs.size()));
  SEXP arguments = PROTECT(Rf_allocVector(STRSXP, src_locs.size()));


  int i = 0;
  for(auto& loc : src_locs) {
    SET_STRING_ELT(packages, i, std::get<0>(loc) == "" ? NA_STRING : Rf_mkChar(std::get<0>(loc).c_str()));
    SET_STRING_ELT(functions, i, std::get<1>(loc) == "" ? NA_STRING : Rf_mkChar(std::get<1>(loc).c_str()));
    SET_STRING_ELT(arguments, i, std::get<2>(loc) == "" ? NA_STRING : Rf_mkChar(std::get<2>(loc).c_str()));
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

  GlobalStore* db = static_cast<GlobalStore*>(ptr);


  auto src_locs = db->source_locations(Rf_asInteger(idx));

  const char*names[] = {"pkg", "fun", "param", ""};
  SEXP origs = PROTECT(Rf_mkNamed(VECSXP, names));

  SEXP packages = PROTECT(Rf_allocVector(STRSXP, src_locs.size()));
  SEXP functions = PROTECT(Rf_allocVector(STRSXP, src_locs.size()));
  SEXP arguments = PROTECT(Rf_allocVector(STRSXP, src_locs.size()));


  int i = 0;
  for(auto& loc : src_locs) {
    SET_STRING_ELT(packages, i, std::get<0>(loc) == "" ? NA_STRING : Rf_mkChar(std::get<0>(loc).c_str()));
    SET_STRING_ELT(functions, i, std::get<1>(loc) == "" ? NA_STRING : Rf_mkChar(std::get<1>(loc).c_str()));
    SET_STRING_ELT(arguments, i, std::get<2>(loc) == "" ? NA_STRING : Rf_mkChar(std::get<2>(loc).c_str()));
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
  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  SEXP res = PROTECT(Rf_ScalarLogical(db->have_seen(val)));

  UNPROTECT(1);
  return res;
}


SEXP sample_val(SEXP sxpdb) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);


  return db->sample_value();
}

SEXP sample_similar(SEXP sxpdb, SEXP val) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  Description d = Description::description_from_value(val);

  return db->sample_value(d);
}


SEXP get_val(SEXP sxpdb, SEXP i) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  int index = Rf_asInteger(i);
  return db->get_value(index);
}

SEXP merge_db(SEXP sxpdb1, SEXP sxpdb2) {
  void* ptr1 = R_ExternalPtrAddr(sxpdb1);
  if(ptr1== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db1 = static_cast<GlobalStore*>(ptr1);

  void* ptr2 = R_ExternalPtrAddr(sxpdb2);
  if(ptr2== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db2 = static_cast<GlobalStore*>(ptr2);

  try{
    db1->merge_in(*db2);
  }
  catch(std::exception& e) {
    Rf_error("Error merging database %s into %s: %s\n", db2->description_path().c_str(), db1->description_path().c_str(), e.what());
  }

  return Rf_ScalarInteger(db1->nb_values());
}

SEXP size_db(SEXP sxpdb) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  return Rf_ScalarInteger(db->nb_values());
}

SEXP get_meta(SEXP sxpdb, SEXP val) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  return db->get_metadata(val);
}

SEXP get_meta_idx(SEXP sxpdb, SEXP idx) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  return db->get_metadata(Rf_asInteger(idx));
}


SEXP path_db(SEXP sxpdb) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  // This returns the path of the description file
  // so we just need to get the dirname
  auto path = std::filesystem::absolute(db->description_path().parent_path());

  SEXP res = PROTECT(Rf_mkString(path.c_str()));

  UNPROTECT(1);

  return res;
}

SEXP check_db(SEXP sxpdb, SEXP slow) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  std::vector<size_t> errors = db->check(Rf_asLogical(slow));

  SEXP res = PROTECT(Rf_allocVector(INTSXP, errors.size()));

  std::copy_n(errors.begin(), errors.size(), INTEGER(res));

  UNPROTECT(1);

  return res;
}

SEXP map_db(SEXP sxpdb, SEXP fun) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  return db->map(fun);
}




SEXP view_db(SEXP sxpdb) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  return db->view_values();
}

SEXP view_metadata(SEXP sxpdb) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  return db->view_metadata();
}


SEXP view_origins(SEXP sxpdb) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  return db->view_origins();
}

SEXP build_indexes(SEXP sxpdb) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);
  db->build_indexes();

  return R_NilValue;
}


SEXP get_integer_real(SEXP sxpdb) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  return db->get_integer_real();
}
