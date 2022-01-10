#include "database.h"


Database:: Database(const fs::path& config_, bool write_mode_, bool quiet_) :
  config_path(config_),  write_mode(write_mode_),   quiet(quiet_),
  rand_engine(std::chrono::system_clock::now().time_since_epoch().count()),
  pid(getpid())
{
  fs::path sexp_table_path = config_path.parent_path().append("sexp_table.bin");
  fs::path hashes_path = config_path.parent_path().append("hashes_table.bin");
  fs::path runtime_meta_path = config_path.parent_path().append("runtime_meta.bin");
  fs::path static_meta_path = config_path.parent_path().append("static_meta.bin");
  fs::path debug_counters_path = config_path.parent_path().append("debug_counters.bin");

  fs::path lock_path = config_path.parent_path().append(".LOCK");

  bool to_check = false;

  if(std::filesystem::exists(config_path)) {
    // Check the lock file
    if(std::filesystem::exists(lock_path)) {
      Rprintf("Database did not exit properly. Will perform check_db in slow mode.\n");
      to_check = true;
    }

    Config config(config_path);

    const int vmajor = std::stoi(config["major"]);
    const int vminor = std::stoi(config["minor"]);
    const int vpatch = std::stoi(config["patch"]);
    const int vdevelopment = std::stoi(config["devlopment"]);

    if(vmajor != version_major || (vmajor == 0 && version_major == 0 && vminor != version_minor)) {
        Rf_error("The database was created with version %d.%d.%d of the library, which is not compatible with the loaded version %d.%d.%d.\n",
                 vmajor, vminor, vpatch,
                 version_major, version_minor, version_patch);
    }

    fs::path sexp_table_path = config["sexp_table"];
    fs::path hashes_path = config["hashes_table"];
    fs::path runtime_meta_path = config["runtime_meta"];
    fs::path static_meta_path = config["static_meta"];
#ifndef NDEBUG
    if(config.has_key("debug_counters")) {
      fs::path debug_counters_path = config["debug_counters"];
    }
    else {
      Rf_warning("No debug counters in the database.\n");
    }
#endif

    // The search indexes
    search_index.open_from_config(config);

    nb_total_values = std::stoul(config["nb_values"]);
  }
  else {
    if(!quiet) Rprintf("Creating new database at %s.\n", config_path.parent_path().c_str());

    // Write a first configuration file
    // TODO

    // Path for the tables are already set-up by default and tables will hold the paths themselves
  }

  // We can now create the lock file
  if(!to_check) {
    std::ofstream lock_file(lock_path);
    lock_file << std::chrono::system_clock::now().time_since_epoch().count() << std::endl;
  }

  sexp_table.open(sexp_table_path);
  hashes.open(hashes_path);
  runtime_meta.open(runtime_meta_path);
  static_meta.open(static_meta_path);
#ifndef NDEBUG
  debug_counters.open(debug_counters_path);
#endif

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
    debug_counters.load_all();
  }

  if(!quiet) {
    Rprintf("Loaded database at %s with %ld values.\n", config_path.parent_path().c_str(), nb_total_values);
  }

}


Database::~Database() {
  if(pid == getpid()) {
    if(new_elements || nb_total_values == 0)
      write_configuration();
    }

    // Remove the LOCK to witness that the database left without problems
    fs::path lock_path = config_path.parent_path().append(".LOCK");
    fs::remove(lock_path);
  }
}
