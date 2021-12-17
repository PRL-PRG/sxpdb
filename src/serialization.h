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
  struct ReadBuffer {
    ReadBuffer(std::vector<std::byte>& v) : b(v) {}
    std::vector<std::byte>& b;
    size_t read_index  = 0;
  } ;

  size_t bytes_serialized = 0;
  size_t bytes_unserialized = 0;


  static void append_byte(R_outpstream_t stream, int c);
  static void append_buf(R_outpstream_t stream, void *buf, int length);
  static int get_byte(R_inpstream_t stream);
  static void get_buf(R_inpstream_t stream, void *buf, int length);

  //SEXP is a pointer to SEXPREC
  // We do not want here to build an external pointer so we will abuse it
  // and cast a good pointer to what we need
  // The R code from serialize.c does not access any field of it anyway
  // Several strategies:
  // - just serialize them out and return the empty environment when unserializing
  // - keep only one environment per value (the first one in the recursive traversal)
  // - keep only the small environments (in number of elements)
  // - record all environments but implement sharing of the environments
  static SEXP refhook_write(SEXP val, SEXP data);
  static SEXP refhook_read(SEXP val, SEXP data);

public:
  Serializer(size_t size);

  const std::vector<std::byte>& serialize(SEXP val);

  SEXP unserialize(std::vector<std::byte>& buffer);

  size_t current_buf_size() const{ return buf.size(); }

};

#endif
