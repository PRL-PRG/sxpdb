#include "csv_file.h"

#include <iostream>
#include <fstream>
#include <sstream>

CSVFile::CSVFile(const std::string& filename) {
  std::ifstream file(filename);
  if(!file) {
    std::cerr << "Impossible to open file " << filename << std::endl;
  }

  // read line by line
  std::string line;
  while(std::getline(file, line)) {
    std::istringstream is_line(line);
    std::string cell;

    std::vector<std::string> columns;

    while(std::getline(is_line, cell, ',')) {
      columns.push_back(cell);
    }

    rows.push_back(columns);
  }

  // We do not care if rows are unbalanced...
}


size_t CSVFile::nb_columns() const {
  if(rows.size() == 0) {
    return 0;
  }
  else {
    return rows[0].size();
  }
}

size_t CSVFile::nb_rows() const {
  return rows.size();
}


const std::string& CSVFile::cell(size_t column, size_t row) const {
  return rows.at(row).at(column);
}
