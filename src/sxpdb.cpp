#include "sxpdb.h"

#include "global_store.h"

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

  GlobalStore* db = reinterpret_cast<GlobalStore*>(ptr);
  delete db;


  R_ClearExternalPtr(sxpdb);

  return R_NilValue;
}



SEXP add_val(SEXP sxpdb, SEXP val) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = reinterpret_cast<GlobalStore*>(ptr);

  if(db->add_value(val)) {
    return val;
  }

  return R_NilValue;
}


SEXP have_seen(SEXP sxpdb, SEXP val) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = reinterpret_cast<GlobalStore*>(ptr);

  SEXP res = PROTECT(Rf_ScalarLogical(db->have_seen(val)));

  UNPROTECT(1);
  return res;
}


SEXP sample_val(SEXP sxpdb) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = reinterpret_cast<GlobalStore*>(ptr);


  return db->sample_value();
}


SEXP get_val(SEXP sxpdb, SEXP i) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = reinterpret_cast<GlobalStore*>(ptr);

  int index = Rf_asInteger(i);
  return db->get_value(index);
}

SEXP merge_db(SEXP sxpdb1, SEXP sxpdb2) {
  void* ptr1 = R_ExternalPtrAddr(sxpdb1);
  if(ptr1== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db1 = reinterpret_cast<GlobalStore*>(ptr1);

  void* ptr2 = R_ExternalPtrAddr(sxpdb2);
  if(ptr2== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db2 = reinterpret_cast<GlobalStore*>(ptr2);

  db1->merge_in(*db2);

  return Rf_ScalarInteger(db1->nb_values());
}

SEXP size_db(SEXP sxpdb) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = reinterpret_cast<GlobalStore*>(ptr);

  return Rf_ScalarInteger(db->nb_values());
}

SEXP get_meta(SEXP sxpdb, SEXP val) {
  void* ptr = R_ExternalPtrAddr(sxpdb);
  if(ptr== nullptr) {
    return R_NilValue;
  }
  GlobalStore* db = reinterpret_cast<GlobalStore*>(ptr);

  return db->get_metadata(val);
}
