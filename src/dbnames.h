#ifndef SXPDB_DB_NAMES_H
#define SXPDB_DB_NAMES_H

#include <vector>
#include <filesystem>
#include <unistd.h>

#include "table.h"

#include "robin_hood.h"


class DBNames {
private:
    pid_t pid;
    bool write_mode = true;
    uint64_t last_written = 0;
    bool new_dbnames = false;

    mutable std::vector<uint32_t> current_ids;

    std::vector<robin_hood::unordered_set<uint32_t>> dbs;
    UniqTextTable db_names;

    std::unique_ptr<VSizeTable<std::vector<uint32_t>>> dbs_table;

    fs::path base_path = "";
public:
    DBNames() : pid(getpid()) {}

    DBNames(const fs::path& base_path_, bool write = true) : pid(getpid()), write_mode(write) {
        open(base_path_, write);
    }

    void open(const fs::path& base_path_, bool write = true) {
        write_mode = write;
        base_path = fs::absolute(base_path_);
        
        // open the table only in write mode or
        // if it already exists
        if(write_mode || fs::exists(base_path / "dbs.bin")) {
            dbs_table = std::make_unique<VSizeTable<std::vector<uint32_t>>>(base_path / "dbs.bin", write_mode);

            db_names.open(base_path / "dbnames.bin", write_mode);
        }
       

        // Populate db names in write_mode
        // In other mode, we just directly read from the db_table if it exists
        if(write_mode) {
            dbs.clear();
            dbs.resize(dbs_table->nb_values());

            for(uint64_t i = 0; i < dbs_table->nb_values() ; i++) {
                const std::vector<uint32_t>& names = dbs_table->read(i);
                assert(names.size() > 0);
                
                // 0 is the place holder for no db
                if(names.size() != 1 || names[0] != 0) {
                    dbs[i].reserve(names.size());
                    dbs[i].insert(dbs[i].end(), names.begin()< names.end());
                }
            }
            dbs_table.reset();
        }

        if(db_names.nb_values() == 0) {
            db_names.append("");
        }

        last_written = dbs.size();// or the number of value in the table, before?
    }

    void add_dbname(uint64_t index, const std::string& dbname) {
        assert(write_mode);
        assert(pid == getpid());

        if(index != dbs.size()) {
            Rf_error("Cannot add a db name for a value that was not recorded in the main table."
                    " Last index is %lu, but the index of that new db name is %lu.\n",
                    dbs.size(), index);
        }

        uint32_t db_id = db_names.append_index(dbname);

        if(index < dbs.size()) {
            auto res = dbs[index].insert(db_id);
            if(res.second) {
                new_dbnames = true;
            }
        }
        else if(index == dbs.size()) {
            robin_hood::unordered_set<uint32_t> ids{db_id};
            dbs.push_back(ids);
            new_dbnames = true;
        }
    }

    const std::vector<uint32_t>& get_dbs(uint64_t index) const {
        if(write_mode) {
            if(index < dbs.size()) {
                current_ids.clear();
                auto ids_set = dbs[index];
                current_ids.insert(current_ids.end(), ids_set.begin(), ids_set.end());

                return current_ids;
            }
            else {
                current_ids.clear();
                return current_ids;
            }
        }
        else {
            return dbs_table->read(index);
        }
    }

    const std::string& dbname(uint32_t i) const { return db_names.read(i); }

    const fs::path& get_base_path() const { return base_path; }

    uint32_t nb_dbnames() const { return db_names.nb_values(); }

    const SEXP dbnames_cache() const { return db_names.to_sexp(); }

    virtual ~DBNames() {
        // TODO
        if(write_mode && pid == getpid() && new_dbnames) {
            fs::rename(base_path / "dbs.bin",  base_path / "dbs-old.bin");
            fs::rename(base_path / "dbs.conf", base_path / "dbs-old.conf");
            fs::rename(base_path / "dbs_offsets.bin",  base_path / "dbs_offsets-old.bin");
            fs::rename(base_path / "dbs_offsets.conf", base_path / "dbs_offsets-old.conf");

            VSizeTable<std::vector<uint32_t>> dbs_table(base_path / "dbs.bin");

            for(const auto& ids : dbs) {
                current_ids.clear();
                current_ids.insert(current_ids.end(), ids.begin(), ids.end());
                dbs_table.append(current_ids);
            }
            dbs_table.flush();

            fs::remove(base_path / "dbs-old.bin");
            fs::remove(base_path / "dbs-old.conf");
            fs::remove(base_path / "dbs_offsets-old.bin");
            fs::remove(base_path / "dbs_offsets-old.conf");
        }
    }

    uint64_t nb_values() const {
        if(write_mode && last_written > 0 ) {
            return dbs.size();
        }
        else if(write_mode && last_written == 0) {
            return 0;
        }
        else {
            return dbs_table->nb_values();
        }
    }
};

#endif