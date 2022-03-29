
#ifndef SXPDB_CALL_IDS_H
#define SXPDB_CALL_IDS_H

#include <vector>
#include <filesystem>
#include <unistd.h>

#include "table.h"



class CallIds {
private:
    pid_t pid;

    std::unique_ptr<VSizeTable<std::vector<uint64_t>>> call_ids_table;
    std::vector<std::vector<uint64_t>> call_ids; // call_ids should be unique at the level of a file.
    // provided there are less than 2^64 calls in one file.

    bool write_mode = false;
    bool new_call_ids = false;

    fs::path base_path = "";
public:
    CallIds() : pid(getpid()) {}
    CallIds(const fs::path& base_path_, bool write = true) : pid(getpid()), write_mode(write) {
        open(base_path_);
    }

    void open(const fs::path& base_path_, bool write = true) {
        write_mode = write;
        base_path = fs::absolute(base_path_);

        call_ids_table = std::make_unique<VSizeTable<std::vector<uint64_t>>>(base_path / "call_ids.bin", write_mode);

        if(write_mode) {
            call_ids.clear();
            call_ids.resize(call_ids_table->nb_values());

            for(uint64_t i = 0 ; i < call_ids_table->nb_values() ; i++) {
                const std::vector<uint64_t>& calls = call_ids_table->read(i);
                assert(calls.size() > 0);
                call_ids[i].reserve(calls.size());
                std::copy_n(calls.begin(), calls.size(), call_ids[i].begin());
            }

            // No need anymore for the table, until the destructor
            call_ids_table.reset();
        }
    }

    void add_call_id(uint64_t index, uint64_t call_id) {
        assert(write_mode);
        assert(pid == getpid());

        if(index > call_ids.size()) {
              Rf_error("Cannot add a call id for a value that was not recorded in the main table."
                   " Last index is %lu, but the index of that new call id is %lu.\n",
                   call_ids.size(), index);
        }

        if(index < call_ids.size()) {
            call_ids[index].push_back(call_id);
        }
        else if( index == call_ids.size()) {
            call_ids.emplace_back(1, call_id);
        }

        new_call_ids = true;
    }

    const std::vector<uint64_t>& get_call_ids(uint64_t index) const {
        if(write_mode) {
            if(index < call_ids.size()) {
                return call_ids[index];
            }
            else {
                Rf_error("Trying to retrieve a call id for a value that does not exist. %lu values but %lu index.\n",
                 call_ids.size(), index);
            }
        }
        else {
            return call_ids_table->read(index);
        }
    }

    const fs::path& get_base_path() const { return base_path; }

    virtual ~CallIds() {
        if(write_mode && pid == getpid() && new_call_ids) {
               fs::rename(base_path / "call_ids.bin", base_path / "call_ids-old.bin");
            fs::rename(base_path / "call_ids.conf", base_path / "call_ids-old.conf");
            fs::rename(base_path / "call_ids_offsets.bin", base_path / "call_ids_offsets-old.bin");
            fs::rename(base_path / "call_ids_offsets.conf", base_path / "call_ids_offsets-old.conf");

            VSizeTable<std::vector<uint64_t>> call_ids_table(base_path / "call_ids.bin");

            for(const auto& call_ids : call_ids) {
                call_ids_table.append(call_ids);
            }
            call_ids_table.flush();


            fs::remove(base_path / "call_ids-old.bin");
            fs::remove(base_path / "call_ids-old.conf");
            fs::remove(base_path / "call_ids_offsets-old.bin");
            fs::remove(base_path / "call_ids_offsets-old.conf");
        }
    }

    uint64_t nb_values() const {
        if(write_mode) {
            return call_ids.size();
        }
        else {
            return call_ids_table->nb_values();
        }
    }
};

#endif