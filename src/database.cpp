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

bool Database::have_seen(SEXP val) const {
  std::optional<sexp_hash> key;
  // if we are in write mode, we can bother looking into the cache of SEXP
  if(write_mode) {
    key = cached_hash(val);
  }

  if(!key) {
    key = compute_hash(val);
  }

  return sexp_index.find(&key.value()) != sexp_index.end();
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

const std::optional<std::reference_wrapper<sexp_hash>>  Database::cached_hash(SEXP val) const {
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

const sexp_hash& Database::compute_cached_hash(SEXP val, const std::vector<std::byte>& buf) const {
  sexp_hash ser_hash  = XXH3_128bits(buf.data(), buf.size());

  auto res = sexp_addresses.insert_or_assign(val, ser_hash);

  SET_RTRACE(val, 1);// set the tracing bit to show later that it is a value we actually touched

  return res.first->second;
}


const SEXP Database::sample_value() const {
  if(nb_total_values > 0) {
    std::uniform_int_distribution<uint64_t> dist(0, nb_total_values - 1);

    return get_value(dist(rand_engine));
  }

  return R_NilValue;
}

const SEXP Database::sample_value(Query& query) const {
  if(new_elements || !query.is_initialized()) {
    query.update(*this);
  }

  return get_value(query.sample());
}
