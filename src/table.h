#ifndef SXPDB_TABLE_H
#define SXPDB_TABLE_H

#include <vector>
#include <cstdint>
#include <iostream>
#include <fstream>
#include <cstdef>
#include <filesystem>

namespace fs = std::filesystem;

//TODO:
// Add an iterator interface
// Add conversion to an R vector (or even better, support for altrep)
template<typename T>
class Table {
protected:
  uint64_t n_values = 0;
  bool in_memory = false;
public:
  virtual void append(const T& value) = 0;
  virtual void append(const std::vector<T>& values) = 0;
  virtual T read(uint64_t index) = 0;
  virtual void write(uint64_t index, const T& value) = 0;
  //Should all the table be loaded in memory?
  // If yes, writes and reads will be performed in memory and
  // could be materialized to disk only at destruction time
  // depending on how the loading is performed.
  virtual void load_all() = 0;

  virtual uint64_t nb_values() const {return n_values; };
};

//Table for fixed size elements
template<typename T>
class FSizeTable : public Table<T> {
private:
  std::fstream file;
  fs::path file_path;
  // TODO add config file
  std::vector<T> store;
  uint64_t last_written_index = 0;
  bool only_append = true;
public:
  FSizeTable(fs::path path)  : file_path(path) {
    file.open(path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);

    // TODO: update nb_values with the number of values in the file
    // we can also check if the size in the config file and the size given by the size of the file are coherent
  }

  virtual void append(const T& value) {
    if(in_memory) {
      store.push_back(value);
    }
    else {
      file.write(static_cast<char*>(&value), sizeof(value));
    }

    nb_values++;
  }

  virtual void append(const std::vector<T>& values) {
    if(in_memory) {
        store.insert(store.end(), values.begin(), values.end());
    }
    else {
      file.write(static_cast<char*>(values.data()), sizeof(T) * values.size());
    }
    nb_values += values.size();
  }

  virtual T read(uint64_t index) {
    if(in_memory) {
      return store[index];
    }
    else {
      T data;
      file.read(static_cast<char*>(&data), sizeof(T));
      return data;
    }
  }

  virtual void write(uint64_t index, const T& value) {
    if(in_memory) {
      store[index] = value;
    }
    else {
      file.seekp(index * sizeof(T));
      file.write(static_cast<char*>(&value), sizeof(value));
      // seek back to the end
      file.seekp(0, std::ios_base::end);
    }
    only_append = false;
  }

  virtual void load_all() {
    store.resize(nb_values);
    file.read(static_cast<char*>(store.data()), nb_values * sizeof(T));
    in_memory = true;
    last_written_index = nb_values - 1;// all the values up to that index are already in the file

    // Now we can close the backing file a
    // and open it only when when materializing the data back on disk
    file.close();
  }

  virtual ~FSizeTable() {
    uint64_t nb_new_values = nb_values - last_written_index;
    if(in_memory && only_append && nb_new_values > 0) {
      //only materialize new values
      file.open(file_path, std::fstream::out | std::fstream::app | std::fstream::binary);
      file.write(static_cast<char*>(store.data() + last_written_index), nb_new_values * sizeof(T));
      file.close();
    }
  }

};

// Table for variable size element (supports also compression)
template<typename T>
class VSizeTable : public Table<T> {
private:
  FSizeTable<uint64_t> offset_table;
  std::fstream file;// for the actual values
  // Layout:
  // 8 bytes, 1 byte, n bytes
  // Size, flags, actual bytes or compressed bytes of the value
  // flags: currently, whether the value is compressed or not

};

#endif
