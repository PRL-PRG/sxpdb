#ifndef SXPDB_CSVFILE_H
#define SXPDB_CSVFILE_H

#include <vector>
#include <string>

// A very simple CSV file class
// Separators are commas
// There are no column names
class CSVFile {
private:
  // will store row per row
  std::vector<std::vector<std::string>> rows;
public:
  CSVFile(const std::string& filename);
  CSVFile();

  void write(const std::string& filename);

  size_t nb_columns() const;
  size_t nb_rows() const;
  const std::string& cell(size_t column, size_t row) const;

  const std::vector<std::vector<std::string>>& get_rows() const { return rows;}

  void add_row(std::vector<std::string>&& row);
};

#endif
