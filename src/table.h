#ifndef SXPDB_TABLE_H
#define SXPDB_TABLE_H

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include <Rversion.h>

#include <vector>
#include <cstdint>
#include <iostream>
#include <fstream>
#include <cstddef>
#include <filesystem>
#include <unordered_map>
#include <string>
#include <cassert>
#include <cstdio>
#include <unistd.h>

#include "config.h"

namespace fs = std::filesystem;


template<typename T>
class Table;

template<typename T>
class TableIterator {
public:
  TableIterator(Table<T>& table) : source_(table) {}
  virtual const T& operator*() const = 0;
private:
  Table<T>& source_;
  uint64_t index_ = 0;
};

//TODO:
// Add an iterator interface
// Add conversion to an R vector (or even better, support for altrep)
template<typename T>
class Table {
protected:
  uint64_t n_values = 0;
  bool in_memory = false;
  friend class TableIterator<T>;

  bool new_elements = false;

  fs::path file_path;
  pid_t pid;
public:
  Table(fs::path path) : pid(getpid()) {
    open(path);
  }

  Table() : pid(getpid()) {}

  virtual void open(fs::path path) {
    //Check if there is already a config file
    if(fs::exists(path)) {
      Config config(path);
      fs::path conf_path = config["path"];
      file_path = fs::absolute(fs::path(conf_path));
      n_values = std::stoul(config["nb_values"]);
    }
    else {
      std::unordered_map<std::string, std::string> conf;
      fs::path ext = "bin";
      file_path = path.replace_extension(ext);
      conf["path"] = path.filename().string();
      conf["nb_values"] = "0";
      Config config(std::move(conf));
      config.write(path);
    }
  }

  ~Table() {
    if(new_elements && pid == getpid()) {
      std::unordered_map<std::string, std::string> conf;
      fs::path path = file_path.stem();// without the .bin extension
      conf["path"] = file_path.string();
      conf["nb_values"] = std::to_string(n_values);
      Config config(std::move(conf));
      config.write(path);
    }
  }

  virtual void append(const T& value) = 0;
  virtual void append(const std::vector<T>& values) = 0;
  virtual const T& read(uint64_t index) = 0;
  virtual void read_in(uint64_t index,  T& value) = 0;
  virtual void write(uint64_t index, const T& value) = 0;
  //Should all the table be loaded in memory?
  // If yes, writes and reads will be performed in memory and
  // could be materialized to disk only at destruction time
  // depending on how the loading is performed.
  virtual void load_all() = 0;

  virtual uint64_t nb_values() const {return n_values; };

  //virtual TableIterator<T> begin() = 0;
  //virtual TableIterator<T> end() = 0;

  virtual const fs::path& get_path() const {return file_path;}
};

//Table for fixed size elements
template<typename T>
class FSizeTable : public Table<T> {
private:
  using Table<T>::file_path;
  using Table<T>::n_values;
  using Table<T>::in_memory;
  using Table<T>::new_elements;
  using Table<T>::pid;
  std::fstream file;
  std::vector<T> store;
  uint64_t last_written_index = 0;
  bool only_append = true;
  T data;
public:
  FSizeTable(fs::path path) : Table<T>(path) {
    open(path);
  }

  FSizeTable() :Table<T>() {}

  void open(fs::path path) override {
    Table<T>::open(path);
    // check if the size is coherent
    uint64_t n_values_file = fs::file_size(file_path) / sizeof(T);
    if(n_values != n_values_file) {
      Rf_warning("Number of values in config file and file do not match for table %s: %lu vs %lu.\n", path.c_str(), n_values, n_values_file);
    }

    file.open(file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);

    if(!file) {
      Rf_error("Impossible to open the table file at %s\n", file_path.c_str());
    }

    file.exceptions(std::fstream::failbit);
  }


  void append(const T& value) override {
    if(in_memory) {
      store.push_back(value);
    }
    else {
      file.write(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    n_values++;
  }

  void append(const std::vector<T>& values) override {
    if(in_memory) {
        store.insert(store.end(), values.begin(), values.end());
    }
    else {
      file.write(reinterpret_cast<const char*>(values.data()), sizeof(T) * values.size());
    }
    n_values += values.size();
  }

  const T& read(uint64_t index) override {
    if(in_memory) {
      return store[index];
    }
    else {
      file.seekg(index);
      file.read(reinterpret_cast<char*>(&data), sizeof(T));
      return data;
    }
  }

  void read_in(uint64_t index, T& value) override {
    if(in_memory) {
      value = store[index];
    }
    else {
      file.seekg(index);
      file.read(reinterpret_cast<char*>(&value), sizeof(T));
    }
  }

  void write(uint64_t index, const T& value) override {
    if(in_memory) {
      store[index] = value;
    }
    else {
      file.seekp(index * sizeof(T));
      file.write(reinterpret_cast<const char*>(&value), sizeof(value));
      // seek back to the end
      file.seekp(0, std::ios_base::end);
    }
    only_append = false;
  }

  void load_all() override {
    store.resize(n_values);
    file.read(reinterpret_cast<char*>(store.data()), n_values * sizeof(T));
    in_memory = true;
    last_written_index = n_values - 1;// all the values up to that index are already in the file

    // Now we can close the backing file a
    // and open it only when when materializing the data back on disk
    file.close();
  }

  const std::vector<T>& memory_view() {
    if(!in_memory) {
      load_all();
    }

    return store;
  }

  virtual ~FSizeTable() {
    uint64_t nb_new_values = n_values - last_written_index;
    if(in_memory && only_append && nb_new_values > 0 && pid == getpid()) {
      //only materialize new values
      file.open(file_path, std::fstream::out | std::fstream::app | std::fstream::binary);
      file.write(reinterpret_cast<char*>(store.data() + last_written_index), nb_new_values * sizeof(T));
      file.close();
    }

    if(nb_new_values > 0) {
      new_elements = true;
    }
  }

};

// Table for variable size element (supports also compression)
// T should be some kind of array/vector/string (with size(), data() methods)
template<typename T>
class VSizeTable : public Table<T> {
private:
  using Table<T>::file_path;
  using Table<T>::n_values;
  using Table<T>::in_memory;
  using Table<T>::new_elements;
  FSizeTable<uint64_t> offset_table;
  std::fstream file;// for the actual values
  // Layout:
  // 8 bytes (64 bit unsigned integer),  n bytes
  // Size, actual value
  T value;
public:
  VSizeTable(fs::path path) {
    open(path);
  }

  VSizeTable() {}

  void open(fs::path path) override {
    Table<T>::open(path);

    offset_table.open(path.parent_path() / (path.filename().string() + "_offset"));

    file.open(file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);

    if(!file) {
      Rf_error("Impossible to open the table file at %s\n", file_path.c_str());
    }

    file.exceptions(std::fstream::failbit);
  }

  void append(const T& value) override {
    uint64_t size = value.size();

    auto pos = file.tellp();

    file.write(reinterpret_cast<char*>(&size), sizeof(size));
    file.write(reinterpret_cast<const char*>(value.data()), sizeof(typename T::value_type) * size);

    offset_table.append(pos);

    n_values++;
  }



  void append(const std::vector<T>& values) override {
    for(const auto& value : values) {
      append(value);
    }
  }

  const T& read(uint64_t idx) override {
    read_in(idx, value);
    return value;
  }

  void read_in(uint64_t idx, T& val) override {
    int64_t offset = offset_table.read(idx);

    file.seekg(offset);
    uint64_t size = 0;
    file.read(reinterpret_cast<char*>(&size), sizeof(size));
    assert(size > 0);

    file.read(reinterpret_cast<char*>(val.data()), sizeof(typename T::value_type) * size);
  }

  void load_all() override {
    offset_table.load_all();
  }

  void write(uint64_t idx, const T& value) override {
    uint64_t offset = offset_table.read(idx);

    file.seekg(offset);
    uint64_t size = 0;
    file.read(reinterpret_cast<char*>(&size), sizeof(size));
    assert(size > 0 );

    if(value.size() != size) {
      Rf_warning("Cannot write at index %lu a value of a different size in table %s : %lu vs %ld.\n", idx, file_path.c_str(), size, value.size());
      return;
    }

    file.seekp(offset);
    file.write(reinterpret_cast<const char*>(value.data()), sizeof(typename T::value_type) * size);
    // seek back to the end
    file.seekp(0, std::ios_base::end);
  }

};



#endif
