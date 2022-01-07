#ifndef SXPDB_TABLE_H
#define SXPDB_TABLE_H

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include "Rversion.h"

#include <vector>
#include <cstdint>
#include <iostream>
#include <fstream>
#include <cstddef>
#include <filesystem>
#include <unordered_map>
#include <string>
#include <cassert>

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
public:
  Table(fs::path path) {
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
    if(new_elements) {
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
};

//Table for fixed size elements
template<typename T>
class FSizeTable : public Table<T> {
private:
  using Table<T>::file_path;
  using Table<T>::n_values;
  using Table<T>::in_memory;
  using Table<T>::new_elements;
  std::fstream file;
  std::vector<T> store;
  uint64_t last_written_index = 0;
  bool only_append = true;
  T data;
public:
  FSizeTable(fs::path path) : Table<T>(path) {
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


  virtual void append(const T& value) {
    if(in_memory) {
      store.push_back(value);
    }
    else {
      file.write(reinterpret_cast<char*>(&value), sizeof(value));
    }

    n_values++;
  }

  virtual void append(const std::vector<T>& values) {
    if(in_memory) {
        store.insert(store.end(), values.begin(), values.end());
    }
    else {
      file.write(reinterpret_cast<char*>(values.data()), sizeof(T) * values.size());
    }
    n_values += values.size();
  }

  virtual const T& read(uint64_t index) {
    if(in_memory) {
      return store[index];
    }
    else {
      file.seekg(index);
      file.read(reinterpret_cast<char*>(&data), sizeof(T));
      return data;
    }
  }

  virtual void read_in(uint64_t index, T& value) {
    if(in_memory) {
      value = store[index];
    }
    else {
      file.seekg(index);
      file.read(reinterpret_cast<char*>(&value), sizeof(T));
    }
  }

  virtual void write(uint64_t index, const T& value) {
    if(in_memory) {
      store[index] = value;
    }
    else {
      file.seekp(index * sizeof(T));
      file.write(reinterpret_cast<char*>(&value), sizeof(value));
      // seek back to the end
      file.seekp(0, std::ios_base::end);
    }
    only_append = false;
  }

  virtual void load_all() {
    store.resize(n_values);
    file.read(reinterpret_cast<char*>(store.data()), n_values * sizeof(T));
    in_memory = true;
    last_written_index = n_values - 1;// all the values up to that index are already in the file

    // Now we can close the backing file a
    // and open it only when when materializing the data back on disk
    file.close();
  }

  virtual ~FSizeTable() {
    uint64_t nb_new_values = n_values - last_written_index;
    if(in_memory && only_append && nb_new_values > 0) {
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
  // 8 bytes (64 bit unsigned integer), 1 byte, n bytes
  // Size, actual value
  T value;
public:
  VSizeTable(fs::path path) : Table<T>(path), offset_table(path.parent_path().append(path.filename().string() + "_offset")) {
    file.open(file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);

    if(!file) {
      Rf_error("Impossible to open the table file at %s\n", file_path.c_str());
    }

    file.exceptions(std::fstream::failbit);
  }

  virtual void append(const T& value) {
    uint64_t size = value.size();

    auto pos = file.tellp();

    file.write(reinterpret_cast<char*>(&size), sizeof(size));
    file.write(reinterpret_cast<char*>(&value.data()), sizeof(T::value_type) * size);

    offset_table.append(pos);

    n_values++;
  }

  virtual void append(const std::vector<T>& values) {
    for(const auto& value : values) {
      append(value);
    }
  }

  virtual T read(uint64_t idx) {
    read_in(idx, value);
    return value;
  }

  virtual void read_in(uint64_t idx, T& val) {
    int64_t offset = offset_table.read(idx);

    file.seekg(offset);
    uint64_t size = 0;
    file.read(reinterpret_cast<char*>(&size), sizeof(size));
    assert(size > 0);

    file.read(reinterpret_cast<char*>(val.data()), sizeof(T::value_type) * size);
  }

  virtual void load_all() {
    offset_table.load_all();
  }

  virtual void write(uint64_t idx, const T& value) {
    uint64_t offset = offset_table.read(idx);

    file.seekg(offset);
    uint64_t size = 0;
    file.read(reinterpret_cast<char*>(&size), sizeof(size));
    assert(size > 0 );

    if(value.size() != size) {
      Rf_warning("Cannot write at index %lu a value of a different size in table %s : %lu vs %lu ", index, file_path.c_str(), size, value.size());
      return;
    }

    file.seekp(offset);
    value.resize(size);
    file.write(reinterpret_cast<char*>(value.data()), sizeof(T::value_type) * size);
    // seek back to the end
    file.seekp(0, std::ios_base::end);
  }

};



#endif
