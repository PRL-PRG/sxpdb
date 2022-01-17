#include "database.h"

Database:: Database(const fs::path& config_, bool write_mode_, bool quiet_) :
  config_path(config_),  write_mode(write_mode_),   quiet(quiet_),
  rand_engine(std::chrono::system_clock::now().time_since_epoch().count()),
  pid(getpid()),
  ser(32768)
{
  fs::path sexp_table_path = config_path.parent_path() / "sexp_table.bin";
  fs::path hashes_path = config_path.parent_path() / "hashes_table.bin";
  fs::path runtime_meta_path = config_path.parent_path() / "runtime_meta.bin";
  fs::path static_meta_path = config_path.parent_path() / "static_meta.bin";
  fs::path debug_counters_path = config_path.parent_path() / "debug_counters.bin";

  fs::path lock_path = config_path.parent_path() / ".LOCK";

  bool to_check = false;

  if(std::filesystem::exists(config_path)) {
    // Check the lock file
    if(std::filesystem::exists(lock_path) && write_mode) {
      Rprintf("Database did not exit properly. Will perform check_db in slow mode.\n");
      Rprintf("It might also be because the database is also open in write mode from another process.\n");
      to_check = true;
    }

    Config config(config_path);

    const int vmajor = std::stoi(config["major"]);
    const int vminor = std::stoi(config["minor"]);
    const int vpatch = std::stoi(config["patch"]);
    const int vdevelopment = std::stoi(config["devel"]);

    // Breaking change for the db are major version numbers
    // Or if we are still in development mode, any change in development number
    if(vmajor != version_major || (vmajor == 0 && version_major == 0 && vminor == 0 &&
       version_minor == 0 && vpatch == 0 && version_patch && vdevelopment != version_development)) {
        Rf_error("The database was created with version %d.%d.%d.%d of the library, which is not compatible with the loaded version %d.%d.%d.%d\n",
                 vmajor, vminor, vpatch, vdevelopment,
                 version_major, version_minor, version_patch, version_development);
    }

    sexp_table_path = config["sexp_table"];
    hashes_path = config["hashes_table"];
    runtime_meta_path = config["runtime_meta"];
    static_meta_path = config["static_meta"];

    if(config.has_key("debug_counters")) {
      debug_counters_path = config["debug_counters"];
    }
    else {
#ifndef NDEBUG
      Rf_warning("No debug counters in the database. Did you create the database in debug mode?\n");
#else
      R_ShowMessage("The database does not contain debug informations.\n");
#endif
    }

    // The search indexes
    if(!quiet) Rprintf("Loading search indexes.\n");
    search_index.open_from_config(config);

    nb_total_values = std::stoul(config["nb_values"]);
  }
  else {
    if(!quiet) Rprintf("Creating new database at %s.\n", config_path.parent_path().c_str());

    // Set up paths for the tables
    sexp_table_path = config_path.parent_path() / "sexp_table";
    hashes_path = config_path.parent_path() / "hashes";
    runtime_meta_path = config_path.parent_path() / "runtime_meta";
    static_meta_path = config_path.parent_path() / "static_meta";
    debug_counters_path = config_path.parent_path() / "debug_counters";

    //This will also set-up the paths for the search index
    write_configuration();
  }

  // We can now create the lock file if we are in write mode
  if(!to_check && write_mode) {
    std::ofstream lock_file(lock_path);
    lock_file << std::chrono::system_clock::now().time_since_epoch().count() << std::endl;
  }

  sexp_table.open(sexp_table_path);
  hashes.open(hashes_path);
  runtime_meta.open(runtime_meta_path);
  static_meta.open(static_meta_path);
  debug_counters.open(debug_counters_path);

  if(!quiet) Rprintf("Loading origins.\n");
  origins.open(config_path.parent_path());

  // Check if the number of values in tables are coherent
  if(sexp_table.nb_values() == nb_total_values) {
    Rf_error("Inconsistent number of values in the global configuration file and"
                "in the sexp table: %lu vs %lu\n", nb_total_values, sexp_table.nb_values());
  }

  if(hashes.nb_values() != nb_total_values) {
    Rf_error("Inconsistent number of values in the global configuration file and"
               "in the hashes table: %lu vs %lu\n", nb_total_values, hashes.nb_values());
  }

  if(runtime_meta.nb_values() != nb_total_values) {
    Rf_error("Inconsistent number of values in the global configuration file and"
               "in the runtime_meta table: %lu vs %lu\n", nb_total_values, runtime_meta.nb_values());
  }

  if(static_meta.nb_values() != nb_total_values) {
    Rf_error("Inconsistent number of values in the global configuration file and"
               "in the static_meta table: %lu vs %lu\n", nb_total_values, hashes.nb_values());
  }

  if(origins.nb_values() != nb_total_values) {
    Rf_error("Inconsistent number of values in the global configuration file and"
               "in the origin tables: %lu vs %lu.\n", nb_total_values, origins.nb_values());
  }

  if(debug_counters.nb_values() != 0 && debug_counters.nb_values() != nb_total_values) {
    Rf_error("Inconsistent number of values in the global configuration file and"
               "in the debug counters tables: %lu vs %lu.\n", nb_total_values, debug_counters.nb_values());
  }

  if(to_check) {
    Rprintf("Checking the database in slow mode.\n");
    auto errors = check(true);
    if(errors.size() > 0 ) {
      Rf_error("The database is corrupted: %ld errors. Open it again with autorepair to try repairing it.\n", errors.size());
    }
    else {
      Rprintf("No errors found.\n");
    }
  }

  if(write_mode) {
    if(!quiet) Rprintf("Loading runtime changing metadata into memory.\n");
    runtime_meta.load_all();

    if(!quiet) Rprintf("Loading debug counters into memory.\n");
    debug_counters.load_all();
  }

  if(!quiet) Rprintf("Loading hashes into memory.\n");
  // Load the hashes and build the hash map
  const std::vector<sexp_hash>& hash_vec = hashes.memory_view();

  if(!quiet) Rprintf("Allocating memory for the hash table.\n");
  sexp_index.reserve(hashes.nb_values());
  if(!quiet) Rprintf("Building the hash table.\n");
  for(uint64_t i = 0; i < hash_vec.size() ; i++) {
    sexp_index.insert({&hash_vec[i], i});
  }

  if(!quiet) {
    Rprintf("Loaded database at %s with %ld unique values, from %lu packages, %lu functions and %lu parameters.\n",
            config_path.parent_path().c_str(), nb_total_values,
            origins.nb_packages(),
            origins.nb_functions(),
            origins.nb_parameters());
  }

}


Database::~Database() {
  if(pid == getpid()) {
    if(new_elements || nb_total_values == 0) {
      write_configuration();
    }

    // Remove the LOCK to witness that the database left without problems
    if(write_mode) {
      fs::path lock_path = config_path.parent_path() / ".LOCK";
      fs::remove(lock_path);
    }
  }
}

void Database::write_configuration() {
  std::unordered_map<std::string, std::string> conf;

  conf["major"] = std::to_string(version_major);
  conf["minor"] = std::to_string(version_minor);
  conf["patch"] = std::to_string(version_patch);
  conf["devel"] = std::to_string(version_development);
  conf["nb_values"] = std::to_string(nb_total_values);

  conf["sexp_table"] = sexp_table.get_path().string();
  conf["hashes_table"] = hashes.get_path().string();
  conf["runtime_meta"] = runtime_meta.get_path().string();
  conf["static_meta"] = static_meta.get_path().string();
#ifndef NDEBUG
  conf["debug_counters"] = debug_counters.get_path().string();
#endif
  conf["compilation_time"] = std::string(__DATE__) + ":" + __TIME__;

  // The search indexes
  search_index.add_paths_config(conf, config_path);


  Config config(std::move(conf));
  config.write(config_path);
}

std::optional<uint64_t> Database::have_seen(SEXP val) const {
  std::optional<sexp_hash> key;
  // if we are in write mode, we can bother looking into the cache of SEXP
  // if it is in, it means we already saw the value
  if(write_mode) {
    auto res = cached_sexp(val);
    if(res) {
      return *res;
    }
  }

  if(!key) {
    key = compute_hash(val);
  }

  auto res = sexp_index.find(&key.value());

  if(res == sexp_index.end()) {
    return {};
  }
  else {
    return res->second;
  }
}


const sexp_hash& Database::get_hash(uint64_t index) const {
  return hashes.read(index);
}

std::optional<uint64_t> Database::get_index(const sexp_hash& h) const {
  auto it = sexp_index.find(&h);
  return (it != sexp_index.end()) ? std::optional<uint64_t>(it->second) : std::nullopt;
}

const SEXP Database::get_value(uint64_t index) const {
  std::vector<std::byte> buf;
  sexp_table.read_in(index, buf);

  SEXP res = ser.unserialize(buf);

  return res;
}

const SEXP Database::get_metadata(uint64_t index) const {
  if(index >= nb_total_values) {
    return R_NilValue;
  }

  std::vector<const char*> names = {"type", "length", "n_attributes", "n_dims", "size", "n_calls", "n_merges"};

  auto s_meta = static_meta.read(index);
  auto d_meta = runtime_meta.read(index);

  // We might not have debug information at all
  if(debug_counters.nb_values() > 0) {
    names.insert(names.end(), {"maybed_shared", "sexp_addr_optim"});
  }
  // are our indexes ready? we just check if they are empty or not
  if(!search_index.na_index.isEmpty()) {
    names.push_back("is_na");
  }
  if(!search_index.class_index.isEmpty()) {
    names.push_back("has_class");
  }
  // Add the other indexes...

  names.push_back("");

  SEXP res = PROTECT(Rf_mkNamed(VECSXP, names.data()));

  // Static meta
  SET_VECTOR_ELT(res, 0, Rf_ScalarInteger(s_meta.sexptype));
  SET_VECTOR_ELT(res, 1, Rf_ScalarInteger(s_meta.length));
  SET_VECTOR_ELT(res, 2, Rf_ScalarInteger(s_meta.n_attributes));
  SET_VECTOR_ELT(res, 3, Rf_ScalarInteger(s_meta.n_dims));
  SET_VECTOR_ELT(res, 4, Rf_ScalarInteger(s_meta.size));

  //Runtime meta
  SET_VECTOR_ELT(res, 5, Rf_ScalarInteger(d_meta.n_calls));
  SET_VECTOR_ELT(res, 6, Rf_ScalarInteger(d_meta.n_merges));

  //Debug counters
  if(debug_counters.nb_values() > 0) {
    auto debug_cnt = debug_counters.read(index);

    SET_VECTOR_ELT(res, 7, Rf_ScalarInteger(debug_cnt.n_maybe_shared));
    SET_VECTOR_ELT(res, 8, Rf_ScalarInteger(debug_cnt.n_sexp_address_opt));
  }

  if(!search_index.na_index.isEmpty()) {
    SET_VECTOR_ELT(res, 9, Rf_ScalarLogical(search_index.na_index.contains(index)));
  }
  if(!search_index.class_index.isEmpty()) {
    SET_VECTOR_ELT(res, 10, Rf_ScalarLogical(search_index.class_index.contains(index)));
  }

  UNPROTECT(1);

  return res;
}

const std::vector<std::tuple<const std::string_view, const std::string_view, const std::string_view>> Database::source_locations(uint64_t index) const {
  return origins.source_locations(index);
}

std::optional<uint64_t>  Database::cached_sexp(SEXP val) const {
  if(TYPEOF(val) == ENVSXP) {
    return {};
  }

  // Don't even do a lookup in that case
  // if the tracing bit is not set, we have not seen the value for sure
  // if the value is not shared, then its content can be modified in place
  // so we should hash the value again anyway
  if(!maybe_shared(val) || RTRACE(val) == 0 ) {
    return {};
  }

  auto it = sexp_addresses.find(val);

  if(it != sexp_addresses.end()) {
    return it->second;
  }

  return {};
}

const sexp_hash Database::compute_hash(SEXP val) const {
  const std::vector<std::byte>& buf = ser.serialize(val);

  sexp_hash ser_hash  = XXH3_128bits(buf.data(), buf.size());

  return ser_hash;
}

const sexp_hash Database::compute_hash(SEXP val, const std::vector<std::byte>& buf) const {
  sexp_hash ser_hash  = XXH3_128bits(buf.data(), buf.size());

  return ser_hash;
}

void Database::cache_sexp(SEXP val, uint64_t index) {
  SET_RTRACE(val, 1);
  sexp_addresses.insert_or_assign(val, index);
}


const SEXP Database::sample_value() {
  if(nb_total_values > 0) {
    std::uniform_int_distribution<uint64_t> dist(0, nb_total_values - 1);

    return get_value(dist(rand_engine));
  }

  return R_NilValue;
}

const SEXP Database::sample_value(Query& query, uint64_t n) {
  if(new_elements || !query.is_initialized()) {
    query.update(search_index);
  }

  auto index = query.sample(rand_engine);

  if(index) {
    return get_value(*index);
  }
  else {
    return R_NilValue;
  }
}

const SEXP Database::view_metadata() const {
  // "type", "length", "n_attributes", "n_dims", "size", "n_calls", "n_merges"
  // maybe_shared, address_optim, (if debug counters)
  // is_na, has_class, if they have been created

  // TODO: rewrite with async to process separately the various tables

  int n_to_protect = 7;
  SEXP s_type = PROTECT(Rf_allocVector(INTSXP, nb_total_values));
  SEXP s_length = PROTECT(Rf_allocVector(INTSXP, nb_total_values));
  SEXP n_attributes = PROTECT(Rf_allocVector(INTSXP, nb_total_values));
  SEXP n_dims = PROTECT(Rf_allocVector(INTSXP, nb_total_values));
  SEXP s_size = PROTECT(Rf_allocVector(INTSXP, nb_total_values));

  SEXP n_calls = PROTECT(Rf_allocVector(INTSXP, nb_total_values));
  SEXP n_merges = PROTECT(Rf_allocVector(INTSXP, nb_total_values));

  SEXP n_maybe_shared = R_NilValue;
  SEXP n_sexp_address_opt = R_NilValue;
  if(debug_counters.nb_values() > 0) {
    n_maybe_shared = PROTECT(Rf_allocVector(INTSXP, nb_total_values));
    n_sexp_address_opt = PROTECT(Rf_allocVector(INTSXP, nb_total_values));;
    n_to_protect += 2;
  }

  SEXP b_is_na = R_NilValue;
  SEXP b_has_class = R_NilValue;
  if(!search_index.na_index.isEmpty()) {
    b_is_na = PROTECT(Rf_allocVector(LGLSXP, nb_total_values));
    n_to_protect++;
  }
  if(!search_index.class_index.isEmpty()) {
    b_has_class = PROTECT(Rf_allocVector(LGLSXP, nb_total_values));;
    n_to_protect++;
  }

  //Static metadata

  int* s_type_it = INTEGER(s_type);
  int* s_length_it = INTEGER(s_length);
  int* n_attr_it = INTEGER(n_attributes);
  int* n_dims_it = INTEGER(n_dims);
  int* s_size_it = INTEGER(s_size);

  for(uint64_t i = 0 ; i < nb_total_values ; i++) {
    const static_meta_t& meta = static_meta.read(i);

    s_type_it[i] = meta.sexptype;
    s_length_it[i] = meta.length;
    n_attr_it[i] = meta.n_attributes;
    n_dims_it[i] = meta.n_dims;
    assert(meta.size < std::numeric_limits<int>::max() / 2);// R integers are on 31 bits
    s_size_it[i] = meta.size;
  }

  // Runtime metadata
  int* n_calls_it = INTEGER(n_calls);
  int* n_merges_it = INTEGER(n_merges);

  for(uint64_t i = 0 ; i < nb_total_values ; i++) {
    const runtime_meta_t& meta = runtime_meta.read(i);

    n_calls_it[i] = meta.n_calls;
    n_merges_it[i] = meta.n_merges;
  }

  // Debug counters
  if(debug_counters.nb_values() > 0) {
    int* n_shared_it = INTEGER(n_maybe_shared);
    int* n_opt_it = INTEGER(n_sexp_address_opt);

    for(uint64_t i = 0; i < nb_total_values ; i++) {
      const debug_counters_t& cnts = debug_counters.read(i);

      n_shared_it[i] = cnts.n_maybe_shared;
      n_opt_it[i] = cnts.n_sexp_address_opt;
    }
  }

  // Indexes
  if(b_is_na != R_NilValue) {
    int* is_na_it = LOGICAL(b_is_na);
    std::fill_n(is_na_it, nb_total_values, FALSE);
    for(uint64_t idx : search_index.na_index) {
      is_na_it[idx] = TRUE;
    }
  }
  if(b_has_class != R_NilValue) {
    int* has_class_it = LOGICAL(b_has_class);
    std::fill_n(has_class_it, nb_total_values, FALSE);
    for(uint64_t idx : search_index.class_index) {
      has_class_it[idx] = TRUE;
    }
  }


  // Build the result dataframe

  std::vector<std::pair<std::string, SEXP>> columns = {
    {"type", s_type},
    {"length", s_length},
    {"n_attributes", n_attributes},
    {"n_dims", n_dims},
    {"size", s_size},
    {"n_calls", n_calls},
    {"n_merges", n_merges}
  };

  if(debug_counters.nb_values() > 0) {
    columns.insert(columns.end(), {
      {"n_maybe_shared", n_maybe_shared},
      {"n_sexp_addr_opt", n_sexp_address_opt}
      });
  }
  if(b_is_na != R_NilValue) {
    columns.push_back({"is_na", b_is_na});
  }
  if(b_has_class != R_NilValue) {
    columns.push_back({"has_class", b_has_class});
  }

  SEXP df = PROTECT(create_data_frame(columns));

  UNPROTECT(n_to_protect + 1);

  return df;
}

const SEXP Database::view_metadata(Query& query) const  {
  if(new_elements || !query.is_initialized()) {
    query.update(search_index);
  }

  auto index = query.view();
  uint64_t index_size = index.cardinality();

  // "type", "length", "n_attributes", "n_dims", "size", "n_calls", "n_merges"
  // maybe_shared, address_optim, (if debug counters)
  // is_na, has_class, if they have been created

  // TODO: rewrite with async to process separately the various tables

  int n_to_protect = 7;
  SEXP s_type = PROTECT(Rf_allocVector(INTSXP, index_size));
  SEXP s_length = PROTECT(Rf_allocVector(INTSXP, index_size));
  SEXP n_attributes = PROTECT(Rf_allocVector(INTSXP, index_size));
  SEXP n_dims = PROTECT(Rf_allocVector(INTSXP, index_size));
  SEXP s_size = PROTECT(Rf_allocVector(INTSXP, index_size));

  SEXP n_calls = PROTECT(Rf_allocVector(INTSXP, index_size));
  SEXP n_merges = PROTECT(Rf_allocVector(INTSXP, index_size));

  SEXP n_maybe_shared = R_NilValue;
  SEXP n_sexp_address_opt = R_NilValue;
  if(debug_counters.nb_values() > 0) {
    n_maybe_shared = PROTECT(Rf_allocVector(INTSXP, index_size));
    n_sexp_address_opt = PROTECT(Rf_allocVector(INTSXP, index_size));;
    n_to_protect += 2;
  }

  SEXP b_is_na = R_NilValue;
  SEXP b_has_class = R_NilValue;
  if(!search_index.na_index.isEmpty()) {
    b_is_na = PROTECT(Rf_allocVector(LGLSXP, index_size));
    n_to_protect++;
  }
  if(!search_index.class_index.isEmpty()) {
    b_has_class = PROTECT(Rf_allocVector(LGLSXP, index_size));;
    n_to_protect++;
  }

  //Static metadata

  int* s_type_it = INTEGER(s_type);
  int* s_length_it = INTEGER(s_length);
  int* n_attr_it = INTEGER(n_attributes);
  int* n_dims_it = INTEGER(n_dims);
  int* s_size_it = INTEGER(s_size);

  uint64_t j = 0;
  for(uint64_t i : index) {
    const static_meta_t& meta = static_meta.read(i);

    s_type_it[j] = meta.sexptype;
    s_length_it[j] = meta.length;
    n_attr_it[j] = meta.n_attributes;
    n_dims_it[j] = meta.n_dims;
    assert(meta.size < std::numeric_limits<int>::max() / 2);// R integers are on 31 bits
    s_size_it[j] = meta.size;

    j++;
  }

  // Runtime metadata
  int* n_calls_it = INTEGER(n_calls);
  int* n_merges_it = INTEGER(n_merges);

  j = 0;
  for(uint64_t i : index) {
    const runtime_meta_t& meta = runtime_meta.read(i);

    n_calls_it[j] = meta.n_calls;
    n_merges_it[j] = meta.n_merges;

    j++;
  }

  // Debug counters
  if(debug_counters.nb_values() > 0) {
    int* n_shared_it = INTEGER(n_maybe_shared);
    int* n_opt_it = INTEGER(n_sexp_address_opt);

    j = 0;
    for(uint64_t i : index) {
      const debug_counters_t& cnts = debug_counters.read(i);

      n_shared_it[j] = cnts.n_maybe_shared;
      n_opt_it[j] = cnts.n_sexp_address_opt;

      j++;
    }
  }

  // Indexes
  if(b_is_na != R_NilValue) {
    int* is_na_it = LOGICAL(b_is_na);
    std::fill_n(is_na_it, index_size, FALSE);

    j= 0 ;
    for(uint64_t idx : index) {
      if(search_index.na_index.contains(idx)) {
        is_na_it[j] = TRUE;
      }
      j++;
    }
  }
  if(b_has_class != R_NilValue) {
    int* has_class_it = LOGICAL(b_has_class);
    std::fill_n(has_class_it, index_size, FALSE);

    j = 0;
    for(uint64_t idx : index ) {
      if(search_index.class_index.contains(idx)) {
        has_class_it[j] = TRUE;
      }
      j++;
    }
  }


  // Build the result dataframe

  std::vector<std::pair<std::string, SEXP>> columns = {
    {"type", s_type},
    {"length", s_length},
    {"n_attributes", n_attributes},
    {"n_dims", n_dims},
    {"size", s_size},
    {"n_calls", n_calls},
    {"n_merges", n_merges}
  };

  if(debug_counters.nb_values() > 0) {
    columns.insert(columns.end(), {
      {"n_maybe_shared", n_maybe_shared},
      {"n_sexp_addr_opt", n_sexp_address_opt}
    });
  }
  if(b_is_na != R_NilValue) {
    columns.push_back({"is_na", b_is_na});
  }
  if(b_has_class != R_NilValue) {
    columns.push_back({"has_class", b_has_class});
  }

  SEXP df = PROTECT(create_data_frame(columns));

  UNPROTECT(n_to_protect + 1);

  return df;
}

const SEXP Database::view_values() const {
  std::vector<std::byte> buf;
  buf.reserve(128);

  SEXP l = PROTECT(Rf_allocVector(VECSXP, nb_values()));

  for(uint64_t i = 0 ; i < nb_total_values ; i++) {
    sexp_table.read_in(i, buf);
    SEXP val = ser.unserialize(buf);
    SET_VECTOR_ELT(l, i, val);
  }

  UNPROTECT(1);

  return l;
}

const SEXP Database::view_values(Query& query) const {
  if(new_elements || !query.is_initialized()) {
    query.update(search_index);
  }

  auto index = query.view();
  uint64_t index_size = index.cardinality();

  std::vector<std::byte> buf;
  buf.reserve(128);

  SEXP l = PROTECT(Rf_allocVector(VECSXP, nb_values()));

  uint64_t j = 0;
  for(uint64_t i : index) {
    sexp_table.read_in(i, buf);
    SEXP val = ser.unserialize(buf);
    SET_VECTOR_ELT(l, j, val);
    j++;
  }

  UNPROTECT(1);

  return l;
}


const SEXP Database::view_origins() const {
  SEXP pkg_cache = PROTECT(origins.package_cache());
  SEXP fun_cache = PROTECT(origins.function_cache());
  SEXP param_cache = PROTECT(origins.parameter_cache());

  SEXP dfs = PROTECT(Rf_allocVector(VECSXP, nb_total_values));


  for(uint64_t i = 0 ; i < nb_total_values ; i++) {
    auto locs = origins.get_locs(i);

    SEXP value_idx = PROTECT(Rf_allocVector(INTSXP, locs.size()));
    int* val_idx = INTEGER(value_idx);
    SEXP packages = PROTECT(Rf_allocVector(STRSXP, locs.size()));
    SEXP functions = PROTECT(Rf_allocVector(STRSXP, locs.size()));
    SEXP params = PROTECT(Rf_allocVector(STRSXP, locs.size()));

    R_xlen_t j = 0;
    for(auto& loc : locs) {
      val_idx[j] = i;
      SET_STRING_ELT(packages, j, STRING_ELT(pkg_cache, loc.package));
      SET_STRING_ELT(functions, j, STRING_ELT(fun_cache, loc.function));
      SET_STRING_ELT(params, j, loc.param == return_value ? NA_STRING : STRING_ELT(param_cache, loc.param));
      j++;
    }

    SEXP df = PROTECT(create_data_frame({
      {"id", value_idx},
      {"pkg", packages},
      {"fun", functions},
      {"param", params}
    }));
    SET_VECTOR_ELT(dfs, i, df);
    UNPROTECT(5);
  }

  SEXP origs = PROTECT(bind_rows(dfs));

  UNPROTECT(5);
  return origs;
}

const SEXP Database::view_origins(Query& query) const {
  if(new_elements || !query.is_initialized()) {
    query.update(search_index);
  }

  auto index = query.view();
  uint64_t index_size = index.cardinality();

  //TODO: we should actually cache it
  SEXP pkg_cache = PROTECT(origins.package_cache());
  SEXP fun_cache = PROTECT(origins.function_cache());
  SEXP param_cache = PROTECT(origins.parameter_cache());

  SEXP dfs = PROTECT(Rf_allocVector(VECSXP, index_size));

  uint64_t k = 0;
  for(uint64_t i : index) {
    auto locs = origins.get_locs(i);

    SEXP value_idx = PROTECT(Rf_allocVector(INTSXP, locs.size()));
    int* val_idx = INTEGER(value_idx);
    SEXP packages = PROTECT(Rf_allocVector(STRSXP, locs.size()));
    SEXP functions = PROTECT(Rf_allocVector(STRSXP, locs.size()));
    SEXP params = PROTECT(Rf_allocVector(STRSXP, locs.size()));

    R_xlen_t j = 0;
    for(auto& loc : locs) {
      val_idx[j] = i;
      SET_STRING_ELT(packages, j, STRING_ELT(pkg_cache, loc.package));
      SET_STRING_ELT(functions, j, STRING_ELT(fun_cache, loc.function));
      SET_STRING_ELT(params, j, loc.param == return_value ? NA_STRING : STRING_ELT(param_cache, loc.param));
      j++;
    }

    SEXP df = PROTECT(create_data_frame({
      {"id", value_idx},
      {"pkg", packages},
      {"fun", functions},
      {"param", params}
    }));
    SET_VECTOR_ELT(dfs, k, df);
    UNPROTECT(5);
    k++;
  }

  SEXP origs = PROTECT(bind_rows(dfs));

  UNPROTECT(5);
  return origs;
}

const SEXP Database::map(const SEXP function) {
  std::vector<std::byte> buf;
  buf.reserve(128);

  // Build the call
  SEXP call = PROTECT(Rf_lang2(function, Rf_install("unserialized_sxpdb_value")));
  SEXP l = PROTECT(Rf_allocVector(VECSXP, nb_values()));

  // Prepare un environment where we will put the unserialized value
#if defined(R_VERSION) && R_VERSION >= R_Version(4, 1, 0)
  SEXP env = R_NewEnv(R_GetCurrentEnv(), TRUE, 1);
#else
  SEXP env = Rf_eval(Rf_lang1(Rf_install("new.env")), R_GetCurrentEnv());
  assert(TYPEOF(env) == ENVSXP);
#endif

  SEXP unserialized_sxpdb_value = Rf_install("unserialized_sxpdb_value");

  for(uint64_t i = 0 ; i < nb_total_values ; i++) {
    sexp_table.read_in(i, buf);

    SEXP val = PROTECT(ser.unserialize(buf));// no need to protect as it is going to be bound just after
    // or not?

    // Update the argument for the next call
    Rf_defineVar(unserialized_sxpdb_value, val, env);

    // Perform the call
    SEXP res = Rf_eval(call, env);


    SET_VECTOR_ELT(l, i, res);

    UNPROTECT(1);
  }

  UNPROTECT(2);

  return l;
}


const SEXP Database::map(Query& query, const SEXP function) {
  if(new_elements || !query.is_initialized()) {
    query.update(search_index);
  }

  auto index = query.view();
  uint64_t index_size = index.cardinality();

  std::vector<std::byte> buf;
  buf.reserve(128);

  // Build the call
  SEXP call = PROTECT(Rf_lang2(function, Rf_install("unserialized_sxpdb_value")));
  SEXP l = PROTECT(Rf_allocVector(VECSXP, index_size));

  // Prepare un environment where we will put the unserialized value
#if defined(R_VERSION) && R_VERSION >= R_Version(4, 1, 0)
  SEXP env = R_NewEnv(R_GetCurrentEnv(), TRUE, 1);
#else
  SEXP env = Rf_eval(Rf_lang1(Rf_install("new.env")), R_GetCurrentEnv());
  assert(TYPEOF(env) == ENVSXP);
#endif

  SEXP unserialized_sxpdb_value = Rf_install("unserialized_sxpdb_value");

  uint64_t j = 0;
  for(uint64_t i : index) {
    sexp_table.read_in(i, buf);

    SEXP val = PROTECT(ser.unserialize(buf));// no need to protect as it is going to be bound just after
    // or not?

    // Update the argument for the next call
    Rf_defineVar(unserialized_sxpdb_value, val, env);

    // Perform the call
    SEXP res = Rf_eval(call, env);


    SET_VECTOR_ELT(l, j, res);

    UNPROTECT(1);

    j++;
  }

  UNPROTECT(2);

  return l;
}

void Database::add_origin(uint64_t index, const std::string& pkg_name, const std::string& func_name, const std::string& param_name) {
  origins.add_origin(index, pkg_name, func_name, param_name);
}

std::pair<const sexp_hash*, bool> Database::add_value(SEXP val) {
  // Ignore environments and closures
  if(TYPEOF(val) == ENVSXP || TYPEOF(val) == CLOSXP) {
    return std::make_pair(nullptr, false);
  }

  // Check if the instrumented program has not evily forked itself
  if(pid != getpid()) {
    return std::make_pair(nullptr, false);
  }

  std::optional<uint64_t> idx = cached_sexp(val);
  const std::vector<std::byte>*  buf = nullptr;
  std::optional<sexp_hash> key;

  bool sexp_address_optim = idx.has_value();

  if(!idx) {
    const std::vector<std::byte>& buffer = ser.serialize(val);
    key  = compute_hash(val, buffer);
    buf = &buffer;
    idx = get_index(*key);

    // if the hash is already known, idx will get the index of the value
  }


  // If idx has a value, it means that we have already seen the SEXP
  if(idx.has_value()) {
    // We need to modify the runtime metadata and the debug counters
    auto meta = runtime_meta.read(*idx);
    meta.n_calls++;
    runtime_meta.write(*idx, meta);

#ifndef NDEBUG
  auto debug_cnts = debug_counters.read(*idx);
  debug_cnts.n_sexp_address_opt += sexp_address_optim;
  debug_cnts.n_maybe_shared+= maybe_shared(val);
  debug_counters.write(*idx, debug_cnts);
#endif

  }
  else {
    assert(buf != nullptr);
    // We have to add the value to the sexp table and hashes
    // and create the metadata and the debug counters
    idx = nb_total_values;
    sexp_table.append(*buf);

    // Add hash in the table of hashes
    hashes.append(*key);
    // Add it also in the hashmap
    auto res = sexp_index.insert({&hashes.memory_view().back(), *idx});
    assert(res.second);// it should be a new value

    // Static meta
    static_meta_t s_meta;
    s_meta.sexptype = TYPEOF(val);
    s_meta.size = buf->size();
    s_meta.length = Rf_xlength(val);
    s_meta.n_attributes = Rf_xlength(ATTRIB(val));

    SEXP dims = Rf_getAttrib(val, R_DimSymbol);
    s_meta.n_dims = Rf_length(dims);

    if(dims != R_NilValue) {
      if(s_meta.n_dims <= 1) {
        s_meta.n_rows = 0;
      }
      else if(s_meta.n_dims == 2) {
        s_meta.n_rows = Rf_nrows(val);
      }
      else {
        s_meta.n_rows= INTEGER(dims)[0];
      }
    }
    if(Rf_isFrame(val)) {
      s_meta.n_rows = s_meta.length == 0 ? 0 : Rf_xlength(VECTOR_ELT(val, 0));
    }
    static_meta.append(s_meta);

    // runtime meta
    runtime_meta_t r_meta;
    r_meta.n_calls = 0;
    runtime_meta.append(r_meta);

    // debug counters
#ifndef NDEBUG
    debug_counters_t debug_cnts;
    debug_cnts.n_sexp_address_opt = sexp_address_optim;
    debug_cnts.n_maybe_shared = maybe_shared(val);
    debug_counters.append(debug_cnts);
#endif

    // origins
    origins.append_empty_origin();

    nb_total_values++;
    new_elements= true;
    assert(nb_total_values == sexp_table.nb_values());
    assert(nb_total_values == static_meta.nb_values());
    assert(nb_total_values == runtime_meta.nb_values());
    assert(nb_total_values == origins.nb_values());
  }

  // Now we can cache the index if it was not before
  if(!sexp_address_optim) {
    cache_sexp(val, *idx);
  };

  return {&hashes.read(*idx), buf != nullptr};
}

std::pair<const sexp_hash*, bool> Database::add_value(SEXP val, const std::string& pkg_name, const std::string& func_name, const std::string& param_name) {
  auto res = add_value(val, pkg_name, func_name, param_name);
  assert(nb_total_values > 0);
  origins.add_origin(nb_total_values - 1, pkg_name, func_name, param_name);

  return res;
}


uint64_t Database::merge_in(Database& other) {
  uint64_t old_total_values = nb_total_values;

  sexp_hash key;
  for(uint64_t i = 0; i < other.nb_values(); i++) {
     hashes.read_in(i, key);

   // This will take time
    auto has_hash = other.sexp_addresses.find(&key);

    // It is not present in the target db
    if(has_hash == has_hash.end()) {
      uint64_t other_idx = has_hash->second;

      sexp_table.append(other.sexp_table.read(other_idx));

      static_meta.append(other.static_meta.read(other_idx));

      runtime_meta.append(other.runtime_meta.read(other_idx));

      //TODO: handle merging of origins
      // Pre-merge the origin tables (package, function, and parameters names?)
#ifndef NDEBUG
      // TODO: check if there are debug counters in the other database
    debug_counters.append(other.debug_counters.read(other_idx));
#endif

    // TODO: update the hash table

    nb_total_values++;

    }
    else {
    // It is present in the target db: we just have to update the runtime metadata
    // the debug counters,
    // and the possible new origins

    }
  }

  assert(nb_total_values >= old_total_values);

  assert(sexp_table.nb_values() == nb_total_values());

  return nb_total_values - old_total_values;
}
