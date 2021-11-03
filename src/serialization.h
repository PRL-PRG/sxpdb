#ifndef SXPDB_SERIALIZATION_H
#define SXPDB_SERIALIZATION_H

#include <cstddef>
#include <vector>

#include <R.h>
#include <Rinternals.h>
#include <Rdefines.h>

// Not static
// Will make it easier to parallelize (one serializer per thread)
class Serializer {
private:
  std::vector<std::byte> buf;
  struct read_buffer_t {
    std::vector<std::byte>* b;
    size_t read_index;
  } read_buffer;

  size_t bytes_serialized;
  size_t bytes_unserialized;


  static void append_byte(R_outpstream_t stream, int c);
  static void append_buf(R_outpstream_t stream, void *buf, int length);
  static int get_byte(R_inpstream_t stream);
  static void get_buf(R_inpstream_t stream, void *buf, int length);

public:
  Serializer(size_t size);

  const std::vector<std::byte>& serialize(SEXP val);

  SEXP unserialize(std::vector<std::byte>& buffer);

};

#endif
