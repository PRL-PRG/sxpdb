#include "sxpdb.h"

#include "global_store.h"

#include <algorithm>
#include <cassert>


SEXP open_db(SEXP filename) {
  GlobalStore* db = new GlobalStore(CHAR(STRING_ELT(filename, 0)));
  if(db == nullptr) {
    Rf_error("Could not allocate memory for database %s", CHAR(STRING_ELT(filename, 0)));
    exit(1);
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
    SEXP hash_s = PROTECT(Rf_allocVector(RAWSXP, hash.first->size()));
    Rbyte* bytes= RAW(hash_s);

    std::copy_n(hash.first->begin(), hash.first->size(),bytes);

    UNPROTECT(1);

    return hash_s;
  }

  return R_NilValue;
}

SEXP add_val_origin(SEXP sxpdb, SEXP val, SEXP package, SEXP function, SEXP argument) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  // TODO: also handle symbols

  const char* package_name = CHAR(STRING_ELT(package, 0));
  const char* function_name = CHAR(STRING_ELT(function, 0));

  // if empty string or NA, treat it as a return value
  SEXP arg_sexp = STRING_ELT(argument, 0);
  std::string argument_name = (arg_sexp == NA_STRING) ? "" : CHAR(arg_sexp);

  auto hash = db->add_value(val, package_name, function_name, argument_name);

  if(hash.first != nullptr) {
    SEXP hash_s = PROTECT(Rf_allocVector(RAWSXP, hash.first->size()));
    Rbyte* bytes= RAW(hash_s);

    std::copy_n(hash.first->begin(), hash.first->size(),bytes);

    UNPROTECT(1);

    return hash_s;
  }

  return R_NilValue;
}

SEXP val_origins(SEXP sxpdb, SEXP hash_s) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }

  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  sexp_hash hash;
  assert(Rf_length(hash_s) == hash.size());
  std::copy_n(RAW(hash_s), Rf_length(hash_s), hash.begin());

  auto src_locs = db->source_locations(hash);

  const char*names[] = {"package", "function", "argument", ""};
  SEXP origs = PROTECT(Rf_mkNamed(VECSXP, names));

  SEXP packages = PROTECT(Rf_allocVector(STRSXP, src_locs.size()));
  SEXP functions = PROTECT(Rf_allocVector(STRSXP, src_locs.size()));
  SEXP arguments = PROTECT(Rf_allocVector(STRSXP, src_locs.size()));




  int i = 0;
  for(auto& loc : src_locs) {
    SET_STRING_ELT(packages, i, Rf_mkChar(std::get<0>(loc).c_str()));
    SET_STRING_ELT(functions, i, Rf_mkChar(std::get<1>(loc).c_str()));
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

  db1->merge_in(*db2);

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


SEXP avg_insertion_duration(SEXP sxpdb) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = static_cast<GlobalStore*>(ptr);

  return Rf_ScalarReal(db->avg_insertion_duration().count());
}
