#include "csv_file.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <cassert>

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


void CSVFile::add_row(std::vector<std::string>&& row) {
  assert(rows.size() == 0 || rows.back().size() == row.size() );
  rows.push_back(row);
}

void CSVFile::write(const std::string& filename) {
  std::ofstream file(filename,std::ofstream::out | std::ofstream::trunc);
  if(!file) {
    std::cerr << "Impossible to open for write CSV file " << filename << std::endl;
  }

  for(auto& row: rows) {
    for(auto& column : row) {
      file << column << ",";
    }
    file.seekp(-1, std::ios_base::cur);
    file.put('\n');
  }
}
