#ifndef SXPDB_CLASSNAMES_H
#define SXPDB_CLASSNAMES_H

#include <vector>
#include <filesystem>
#include <unistd.h>

#include "table.h"


class ClassNames {
private:
  pid_t pid;
  bool write_mode = true;
  uint64_t last_written = 0;

  // The order of classes is important in R
  std::vector<std::vector<uint32_t>> classes;
  UniqTextTable class_names;

  std::vector<uint32_t> empty_class;
  std::vector<uint32_t> dummy_class = {0};

  fs::path base_path = "";
public:
  ClassNames() : pid(getpid()) {}

  ClassNames(const fs::path& base_path_, bool write = true) : pid(getpid()), write_mode(write) {
    open(base_path_, write);
  }

  void open(const fs::path& base_path_, bool write = true) {
    write_mode = write;
    base_path = fs::absolute(base_path_);

    VSizeTable<std::vector<uint32_t>> class_table(base_path / "classes.bin", write_mode);
    class_names.open(base_path / "classnames.bin", write_mode);

    class_names.load_all();

    // populate classes


    // if the class names are empty, we inject the empty class name, for values without class names
    // (we only store class names for objects, i.e. the old class name)
    classes.clear();
    classes.resize(class_table.nb_values());
    for(uint64_t i = 0 ; i < class_table.nb_values(); i++) {
        const std::vector<uint32_t>& names = class_table.read(i);
        assert(names.size() > 0);
        if(names.size() != 1 || names[0] != 0) { // the value has a class
          classes[i].reserve(names.size());
          classes[i].insert(classes[i].end(), names.begin(), names.end());
        }
    }

    // inject an empty string at position 0
    // it indicates the absence of a class
    if(class_names.nb_values() == 0) {
      class_names.append("");
    }

    last_written = classes.size();
  }

  void add_classnames(uint64_t index, SEXP klass) {
    assert(pid == getpid());
    if(index != classes.size()) {
      Rf_error("Cannot add a classname for a value that was not recorded in the main table."
                 " Last index is %lu, but the index of that new origin is %lu.\n",
                 classes.size(), index);
    }

    //class names is an attribute with a vector of strings
    if(klass == R_NilValue) {
      classes.push_back(empty_class);
    }
    else if (TYPEOF(klass) == STRSXP ){
      std::vector<uint32_t> new_classes;
      new_classes.reserve(LENGTH(klass));
      for(int i = 0; i < LENGTH(klass) ; i++) {
          uint32_t idx = class_names.append_index(CHAR(STRING_ELT(klass, i)));
          new_classes.push_back(idx);
      }
      classes.push_back(new_classes);
    }
    else {
      Rf_warning("The class attribute for value at index %lu has a surprising type: %s.\n", Rf_type2str(TYPEOF(klass)));
    }
  }

  const std::vector<uint32_t>& get_classnames(uint64_t index) const {
    assert(index < classes.size());
    return classes[index];
  }

  uint32_t nb_classnames() const {return class_names.nb_values() - 1; }

  const std::string& class_name(uint32_t i) const { return class_names.read(i); }

  uint64_t nb_values() const { return classes.size() ; }

  SEXP class_name_cache() const { return class_names.to_sexp();}

  virtual ~ClassNames() {
    //TODO: open in write mode a VSizeTable and dump the class ids there
    if(write_mode && pid == getpid() && last_written < classes.size()) {
      fs::rename(base_path / "classes.bin", base_path / "classes-old.bin");
      fs::rename(base_path / "classes.conf", base_path / "classes-old.conf");
      fs::rename(base_path / "classes_offsets.bin", base_path / "classes_offsets-old.bin");
      fs::rename(base_path / "classes_offsets.conf", base_path / "classes_offsets-old.conf");

      VSizeTable<std::vector<uint32_t>> class_table(base_path / "classes.bin");

      for(const auto& class_ids : classes) {
        if(class_ids.size() == 0) {// No classes
          class_table.append(dummy_class);
        }
        else {
          class_table.append(class_ids);
        }
      }

      class_table.flush();

      fs::remove(base_path / "classes-old.bin");
      fs::remove(base_path / "classes-old.conf");
      fs::remove(base_path / "classes_offets-old.bin");
      fs::remove(base_path / "classes_offsets-old.conf");
    }
  }
};

#endif
