#include "serialization.h"

#include <cassert>


Serializer::Serializer(size_t size) {
  buf.reserve(size);
}

void Serializer::append_byte(R_outpstream_t stream, int c)  { //add ints, not chars??
  std::vector<std::byte>* buf = static_cast<std::vector<std::byte>*>(stream->data);
  buf->push_back(static_cast<std::byte>(c));
}

void Serializer::append_buf(R_outpstream_t stream, void *buffer, int length) {
  std::vector<std::byte>* buf = static_cast<std::vector<std::byte>*>(stream->data);

  /*buf->reserve(buf->size() + length);

 std::byte* cbuf = static_cast<std::byte*>(buffer);

  for(int i = 0; i < length; i++) {
    buf->push_back(cbuf[i]);
  }*/

  std::byte* cbuf = static_cast<std::byte*>(buffer);

  buf->insert(buf->end(), cbuf, cbuf+length);
}

int Serializer::get_byte(R_inpstream_t stream) {
  ReadBuffer* rbf = static_cast<ReadBuffer*>(stream->data);

  return static_cast<int>(rbf->b.at(rbf->read_index++));
}


void Serializer::get_buf(R_inpstream_t stream, void *buffer, int length) {
  ReadBuffer* rbf = static_cast<ReadBuffer*>(stream->data);
  std::byte* buf = static_cast<std::byte*>(buffer);

  std::copy(rbf->b.begin() + rbf->read_index, rbf->b.begin() + rbf->read_index + length, buf);
  rbf->read_index += length;
}

SEXP Serializer::refhook_write(SEXP val, SEXP data) {
  if(TYPEOF(val) != ENVSXP) {
    return R_NilValue;//it means that R will serialize the value as usual
  }

  // We just do not serialize the environment and return a blank string
  return R_BlankScalarString;
}

SEXP Serializer::refhook_read(SEXP val, SEXP data) {
  if(CHAR(STRING_ELT(val, 0))[0] != '\0') {
    Rf_error("Uncorrect serialization of environment. Should have been erased, got %s\n", CHAR(STRING_ELT(val, 0)));
  }

  return R_EmptyEnv;// Or put a marker?
}


const std::vector<std::byte>& Serializer::serialize(SEXP val) {
  buf.clear();//hopefully, it keeps the capacity as it was
  R_outpstream_st out;

#ifndef KEEP_ENVIRONMENTS
  R_InitOutPStream(&out, reinterpret_cast<R_pstream_data_t>(&buf),
                   R_pstream_binary_format, 3,
                   append_byte, append_buf,
                   refhook_write, R_NilValue);
#else
  R_InitOutPStream(&out, reinterpret_cast<R_pstream_data_t>(&buf),
                   R_pstream_binary_format, 3,
                   append_byte, append_buf,
                   NULL, R_NilValue);
#endif

  R_Serialize(val, &out);
  bytes_serialized += buf.size();

  return buf;
}


SEXP Serializer::unserialize(std::vector<std::byte>& buffer) {
  R_inpstream_st in;

  ReadBuffer read_buffer(buffer);


  R_InitInPStream(&in, reinterpret_cast<R_pstream_data_t>(&read_buffer),
                  R_pstream_binary_format,
                  get_byte, get_buf,
                  refhook_read, R_NilValue);

  SEXP res = R_Unserialize(&in);

  assert(read_buffer.read_index == buffer.size());

  bytes_unserialized += buffer.size();

  return res;
}
