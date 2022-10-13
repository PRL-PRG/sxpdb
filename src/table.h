#ifndef SXPDB_TABLE_H
#define SXPDB_TABLE_H

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include <Rversion.h>

#include <vector>
#include <deque>
#include <cstdint>
#include <iostream>
#include <fstream>
#include <cstddef>
#include <filesystem>
#include <string>
#include <cassert>
#include <unistd.h>
#include <iterator>
#include <optional>


#include <fcntl.h>
#include <unistd.h>


#include "config.h"
#include "robin_hood.h"
#include "hasher.h"
#include "utils.h"

#include "stable_vector.h"

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

  bool write_mode = true;

  fs::path file_path;
  pid_t pid;
public:
  Table(const fs::path& path, bool write=true) : pid(getpid()), write_mode(write) {
    open(path);
  }

  Table() : pid(getpid()) {}

  virtual void open(const fs::path& path_, bool write=true) {
    write_mode = write;
    //Check if there is already a config file
    fs::path path = fs::path(path_).replace_extension("conf");
    if(fs::exists(path)) {
      Config config(path);
      fs::path bin_path = path.parent_path() / config["path"];
      file_path = fs::absolute(bin_path);
      n_values = std::stoul(config["nb_values"]);
    }
    else {
      std::unordered_map<std::string, std::string> conf;
      fs::path ext = "bin";
      file_path = fs::absolute(path);
      file_path.replace_extension(ext);
      conf["path"] = file_path.filename().string();
      conf["nb_values"] = "0";
      Config config(std::move(conf));
      config.write(fs::absolute(path));
    }
  }

  ~Table() {
    if(write_mode && new_elements && pid == getpid()) {
      std::unordered_map<std::string, std::string> conf;

      conf["path"] = file_path.filename().string();
      conf["nb_values"] = std::to_string(n_values);

      fs::path path = file_path;
      path.replace_extension("conf");
      Config config(std::move(conf));
      config.write(path);

      new_elements = false;
    }
  }

  virtual void flush() {
    if(write_mode && new_elements && pid == getpid()) {
      std::unordered_map<std::string, std::string> conf;

      conf["path"] = file_path.filename().string();
      conf["nb_values"] = std::to_string(n_values);

      fs::path path = file_path;
      path.replace_extension("conf");
      Config config(std::move(conf));
      config.write(path);

      new_elements = false;
    }
  }

  virtual void append(const T& value) = 0;
  virtual void append(const std::vector<T>& values) = 0;
  virtual const T& read(uint64_t index) const = 0;
  virtual void read_in(uint64_t index,  T& value) const = 0;
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

  virtual bool loaded() const { return in_memory; }
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
  using Table<T>::write_mode;

  mutable std::fstream file;
  std::vector<T> store;
  uint64_t last_written = 0;
  mutable T data;
public:
  FSizeTable(const fs::path& path, bool write) : Table<T>(path, write) {
    open(path, write);
  }

  FSizeTable() :Table<T>() {}

  void open(const fs::path& path, bool write = true) override {
    Table<T>::open(path, write);
    // We have to create the file if it does not exists; it requires other flags than the next ones
    if(!fs::exists(file_path)) {
      file.open(file_path, std::fstream::out | std::fstream::app);
      file.close();
    }

    file.open(file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);

    if(!file) {
      Rf_error("Impossible to open the table file at %s: %s\n", file_path.c_str(), strerror(errno));
    }


    // check if the size is coherent
    uint64_t n_values_file = fs::file_size(file_path) / sizeof(T);
    if(n_values != n_values_file) {
      Rf_error("Number of values in config file and file do not match for table %s: %lu vs %lu.\n", path.c_str(), n_values, n_values_file);
    }

    file.exceptions(std::fstream::failbit);

    last_written = n_values;
  }


  void append(const T& value) override {
    if(in_memory) {
      store.push_back(value);
    }
    else {
      file.write(reinterpret_cast<const char*>(&value), sizeof(value));
      last_written++;
    }

    n_values++;
  }

  void append(const std::vector<T>& values) override {
    if(in_memory) {
        store.insert(store.end(), values.begin(), values.end());
    }
    else {
      file.write(reinterpret_cast<const char*>(values.data()), sizeof(T) * values.size());
      last_written += values.size();
    }
    n_values += values.size();
  }

  const T& read(uint64_t index) const override {
    if(in_memory) {
      return store[index];
    }
    else {
      file.seekg(index * sizeof(T));
      file.read(reinterpret_cast<char*>(&data), sizeof(T));
      return data;
    }
  }

  void read_in(uint64_t index, T& value) const override {
    if(in_memory) {
      value = store[index];
    }
    else {
      file.seekg(index * sizeof(T));
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
    last_written = std::min(last_written, index + 1);// Invalidate from that index
  }

  void load_all() override {
    if(!in_memory) {
      store.resize(n_values);
      file.seekg(0);
      file.read(reinterpret_cast<char*>(store.data()), n_values * sizeof(T));
      in_memory = true;
      // all the values up to that index are already in the file

      // Now we can close the backing file a
      // and open it only when when materializing the data back on disk
      file.close();
    }
  }

  const std::vector<T>& memory_view() {
    if(!in_memory) {
      load_all();
    }

    return store;
  }

  void flush() override {
    uint64_t nb_new_values = n_values - last_written;
    if(write_mode && in_memory && nb_new_values > 0 && pid == getpid()) {
      //only materialize new values
      file.open(file_path, std::fstream::out | std::fstream::binary);
      file.seekp(last_written * sizeof(T));
      file.write(reinterpret_cast<char*>(store.data() + last_written - 1), nb_new_values * sizeof(T));
      file.close();
    }
    // Always rewrite the configuration file
    new_elements = true;
    Table<T>::flush();
    new_elements = false;
  }

  virtual ~FSizeTable() {
    uint64_t nb_new_values = n_values - last_written;
    if(write_mode && in_memory  && nb_new_values > 0 && pid == getpid()) {
      //only materialize new values
      file.open(file_path, std::fstream::out | std::fstream::app | std::fstream::binary);
      file.seekp(last_written * sizeof(T));
      file.write(reinterpret_cast<char*>(store.data() + last_written), nb_new_values * sizeof(T));
      file.close();
    }
    new_elements = true;// Always force writing of the config file
  }

};

//Table for fixed size elements and
// that does not invalidate pointers in its memory view
template<typename T>
class FSizeMemoryViewTable : public Table<T> {
private:
  using Table<T>::file_path;
  using Table<T>::n_values;
  using Table<T>::in_memory;
  using Table<T>::new_elements;
  using Table<T>::pid;
  using Table<T>::write_mode;

  mutable int fd = -1;
  StableVector<T> store;
  uint64_t last_written = 0;
  mutable T data;
public:
  FSizeMemoryViewTable(const fs::path& path, bool write) : Table<T>(path, write) {
    open(path, write);
  }

  FSizeMemoryViewTable() {}

  void open(const fs::path& path, bool write = true) override {
    Table<T>::open(path, write);
    // We have to create the file if it does not exists; it requires other flags than the next ones
    
    fd = ::open(file_path.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);

    if(fd == -1) {
      Rf_error("Impossible to open the table file at %s: %s\n", file_path.c_str(), strerror(errno));
    }
    // Now seek to the end for write to be able to append to the file
    uint64_t end_pos = lseek(fd, 0, SEEK_END);

    // check if the size is coherent
    uint64_t n_values_file = fs::file_size(file_path) / sizeof(T);
    if(n_values != n_values_file) {
      Rf_error("Number of values in config file and file do not match for table %s: %lu vs %lu.\n", path.c_str(), n_values, n_values_file);
    }

    last_written = n_values;
  }


  void append(const T& value) override {
    store.push_back(value);

    n_values++;
  }

  void append(const std::vector<T>& values) override {
    store.insert(store.end(), values.begin(), values.end());
    n_values += values.size();
  }

  const T& read(uint64_t index) const override {
    return store[index];
  }

  void read_in(uint64_t index, T& value) const override {
    value = store[index];
  }

  void write(uint64_t index, const T& value) override {
    store[index] = value;
    last_written = std::min(last_written, index + 1);
  }

  void load_all() override {
    store.resize(n_values);


    // We will read sequentially
    uint64_t end_pos = lseek(fd, 0, SEEK_END);
    posix_fadvise(fd, 0, end_pos, POSIX_FADV_SEQUENTIAL);

    lseek(fd, 0, SEEK_SET);

    for(auto& v : store) {
      std::ignore = ::read(fd, reinterpret_cast<char*>(&v), sizeof(T));
    }
    in_memory = true;
    // all the values up to that index are already in the file

    // Now we can close the backing file a
    // and open it only when when materializing the data back on disk
    ::close(fd);
  }

  const StableVector<T>& memory_view() {
    if(!in_memory) {
      load_all();
    }
    return store;
  }

  void flush() override {
    uint64_t nb_new_values = n_values - last_written;
    if(write_mode && in_memory && nb_new_values > 0 && pid == getpid()) {
      //only materialize new values
      fd = ::open(file_path.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
      lseek(fd, last_written * sizeof(T), SEEK_SET);
      for(auto it = store.begin() + last_written; it != store.end(); it++) {
        std::ignore = ::write(fd, reinterpret_cast<char*>(&(*it)), sizeof(T));
      }
      ::close(fd);
    }
    // Always rewrite the configuration file
    new_elements = true;
    Table<T>::flush();
    new_elements = false;
  }

  virtual ~FSizeMemoryViewTable() {
    uint64_t nb_new_values = n_values - last_written;
    if(write_mode && in_memory && nb_new_values > 0 && pid == getpid()) {
      //only materialize new values
      fd = ::open(file_path.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
      lseek(fd, last_written * sizeof(T), SEEK_SET);
      for(auto it = store.begin() + last_written; it != store.end(); it++) {
        std::ignore = ::write(fd, reinterpret_cast<char*>(&(*it)), sizeof(T));
      }
      ::close(fd);
    }
    new_elements = true;// Always force writing of the config file
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
  using Table<T>::write_mode;

  mutable FSizeTable<uint64_t> offset_table;
  int fd = -1;
  // Layout:
  // 8 bytes (64 bit unsigned integer),  n bytes
  // Size, actual value
  mutable T value;
public:
  VSizeTable(const fs::path& path, bool write=true) : Table<T>(path, write) {
    open(path, write);
  }

  VSizeTable()  {}

  void open(const fs::path& path, bool write = true) override {
    Table<T>::open(path, write);

    offset_table.open(file_path.parent_path() / (file_path.stem().string() + "_offsets.bin"), write_mode);

    fd = ::open(file_path.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);

    if(fd == -1) {
      Rf_error("Impossible to open the table file at %s: %s\n", file_path.c_str(), strerror(errno));
    }

    // Now seek to the end for write to be able to append to the file
    uint64_t end_pos = lseek(fd, 0, SEEK_END);
    // This is for the merge case
    // for the sampling case, we should use POSIX_FADV_RANDOM here
    posix_fadvise(fd, 0, end_pos, POSIX_FADV_SEQUENTIAL);
  }

  void append(const T& value) override {
    uint64_t size = value.size();

    auto pos = lseek(fd, 0, SEEK_CUR);// Get current position

    std::ignore = ::write(fd, reinterpret_cast<char*>(&size), sizeof(size));
    // TODO: have a variant for non-contiguous types that would use begin and end
    std::ignore = ::write(fd, reinterpret_cast<const char*>(value.data()), sizeof(typename T::value_type) * size);

    offset_table.append(pos);

    n_values++;
    new_elements = true;
  }



  void append(const std::vector<T>& values) override {
    for(const auto& value : values) {
      append(value);
    }
  }

  const T& read(uint64_t idx) const override {
    read_in(idx, value);
    return value;
  }

  // Can read in parallel because of pread
  void read_in(uint64_t idx, T& val) const override {
    uint64_t offset = offset_table.read(idx);

    uint64_t size = 0;
    std::ignore = pread(fd, reinterpret_cast<char*>(&size), sizeof(size), offset);
    assert(size > 0);

    val.resize(size);// Ensure the target element is big enough
    std::ignore = pread(fd, reinterpret_cast<char*>(val.data()), sizeof(typename T::value_type) * size, offset + sizeof(size));
  }

  void load_all() override {
    offset_table.load_all();
    in_memory = true;
  }

  std::vector<T> materialize() {
    std::vector<T> buf;
    buf.reserve(Table<T>::nb_values());
    for(uint64_t i = 0; i < Table<T>::nb_values() ; i++) {
      buf.push_back(read(i));
    }

    return buf;
  }

  void write(uint64_t idx, const T& value) override {
    uint64_t offset = offset_table.read(idx);

    uint64_t size = 0;
    std::ignore = pread(fd, reinterpret_cast<char*>(&size), sizeof(size), offset);
    assert(size > 0 );

    if(value.size() != size) {
      Rf_warning("Cannot write at index %lu a value of a different size in table %s : %lu vs %ld.\n", idx, file_path.c_str(), size, value.size());
      return;
    }

    std::ignore = pwrite(fd, reinterpret_cast<const char*>(value.data()), sizeof(typename T::value_type) * size, offset);
    // seek back to the end
    lseek(fd, 0, SEEK_END);
  }


  void flush() override {
      offset_table.flush();
      Table<T>::flush();
      new_elements = false;
      close(fd);
  }

  int get_fd() const { return fd; }

  virtual ~VSizeTable() {
    close(fd);
  }
};


// Store only unique values
class UniqTextTable : public Table<std::string> {
private:
  using Table<std::string>::file_path;
  using Table<std::string>::n_values;
  using Table<std::string>::in_memory;
  using Table<std::string>::new_elements;
  using Table<std::string>::pid;
  using Table<std::string>::write_mode;

  mutable std::fstream file;

  mutable std::string val;

  StableVector<std::string> store;//to not get invalidated pointers in the hash table
  robin_hood::unordered_map<std::string, uint64_t> unique_lines;
  uint64_t last_written = 0;

  bool unique_loaded = false;

public:
  class line {
    std::string data;
  public:
    friend std::istream &operator>>(std::istream &is, line &l) {
      std::getline(is, l.data);
      return is;
    }
    operator std::string() const { return data; }
  };

  UniqTextTable() {}
  UniqTextTable(const fs::path& path, bool write = true) {
    open(path, write);
  }

  void open(const fs::path& path, bool write = true) override {
    Table<std::string>::open(path, write);

    // Create the file if it does not exist
    if(!fs::exists(file_path)) {
      file.open(file_path, std::fstream::out | std::fstream::app);
      file.close();
    }
    else {
      file.open(file_path, std::fstream::in);

      if(!file) {
        Rf_error("Impossible to open the table file at %s: %s\n", file_path.c_str(), strerror(errno));
      }

      // There will be only one chunk at the beginning
      // because the default StableVector constructor does not allocate at all.
      // If there are no values at all, reserve will pre-allocate a page worth of values.
      store.reserve(n_values);

      std::copy(std::istream_iterator<line>(file),
                std::istream_iterator<line>(),
                std::back_inserter(store));

      file.close();

      last_written = store.size();
    }

    // Only populate the hash table in write mode
    if(write_mode) {
      load_unique();
    }

    throw_assert(n_values == store.size());

    in_memory = true;

  }

  void load_unique() {
    if(unique_loaded) {
      return;
    }
    // Populate the hash table
    unique_lines.reserve(store.size());
    throw_assert(store.nb_chunks() == 1);
    auto chunk = store.chunk(0);
    for(uint64_t i = 0; i < chunk.size() ; i++) {
      unique_lines.insert({chunk[i], i});
    }

    throw_assert(unique_lines.size() == n_values);// the file should contain unique names

    unique_loaded = true;
  }

  uint64_t append_index(const std::string& value) {
    auto it = unique_lines.find(value);

    if(it == unique_lines.end()) {
      store.push_back(value);
      auto res = unique_lines.insert({store.back(), store.size() - 1});

      n_values++;
      assert(store.size() == n_values);
      assert(res.second);//insertion actually happened
      return store.size() - 1;
    }
    else {
      return it->second;
    }
  }

  void append(const std::string& value) override {
    append_index(value);
  }

  void append(const std::vector<std::string>& values) override {
    for(const auto& value : values) {
     append(value);
    }
  }

  void read_in(uint64_t index, std::string& value) const override {
    value = store[index];
  }

  const std::string& read(uint64_t index) const override {
    read_in(index, val);
    return val;
  }

  std::optional<uint64_t> get_index(const std::string& value) const {
    assert(unique_loaded);
    auto it = unique_lines.find(value);
    if(it != unique_lines.end()) {
      return it->second;
    }
    else {
      return {};
    }
  }

  void write(uint64_t index, const std::string& value) override {
   Rf_error("Write for UniqTextTable is not implemented.\n");
  }

  // Nothing to do as it is already in memory
  void load_all() override {
    if(!unique_loaded) {
      load_unique();
    }
  }

  virtual ~UniqTextTable() {
    int nb_new_elements = n_values - last_written;
    if(write_mode && pid== getpid() && nb_new_elements > 0 ) {
      file.open(file_path, std::fstream::out | std::fstream::app);
      for(auto line = store.begin() + last_written; line != store.end() ; line++) {
        file << *line << "\n";
      }
      file << std::flush;
    }

    if(nb_new_elements > 0) {
      new_elements = true;
    }
  }

  void flush() override {
    int nb_new_elements = n_values - last_written;
    if(write_mode && pid== getpid() && nb_new_elements > 0 ) {
      file.open(file_path, std::fstream::out | std::fstream::app);
      for(auto line = store.begin() + last_written; line != store.end() ; line++) {
        file << *line << "\n";
      }
      file << std::flush;
    }

    if(nb_new_elements > 0) {
      new_elements = true;
      Table<std::string>::flush();
      last_written = n_values;
    }
    new_elements = false;
  }

  const SEXP to_sexp() const {
    SEXP s = PROTECT(Rf_allocVector(STRSXP, nb_values()));
    int i = 0;
    throw_assert(nb_values() == store.size());
    for(const auto& name : std::as_const(store)) {
      SET_STRING_ELT(s, i, Rf_mkChar(name.c_str()));
      i++;
    }

    UNPROTECT(1);
    return s;
  }

};



#endif
