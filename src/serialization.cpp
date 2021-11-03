#include "serialization.h"

#include <cassert>


Serializer::Serializer(size_t size) :  bytes_serialized(0), bytes_unserialized(0) {
  buf.reserve(size);
}

void Serializer::append_byte(R_outpstream_t stream, int c)  { //add ints, not chars??
  std::vector<std::byte>* buf = static_cast<std::vector<std::byte>*>(stream->data);
  buf->push_back(static_cast<std::byte>(c));
}

void Serializer::append_buf(R_outpstream_t stream, void *buffer, int length) {
  std::vector<std::byte>* buf = static_cast<std::vector<std::byte>*>(stream->data);

  buf->reserve(buf->size() + length);
  std::byte* cbuf = static_cast<std::byte*>(buffer);

  for(int i = 0; i < length; i++) {
    buf->push_back(cbuf[i]);
  }
}

int Serializer::get_byte(R_inpstream_t stream) {
  read_buffer_t* rbf = static_cast<read_buffer_t*>(stream->data);

  return static_cast<int>(rbf->b->at(rbf->read_index++));
}


void Serializer::get_buf(R_inpstream_t stream, void *buffer, int length) {
  read_buffer_t* rbf = static_cast<read_buffer_t*>(stream->data);
  std::byte* buf = static_cast<std::byte*>(buffer);

  std::copy(rbf->b->begin() + rbf->read_index, rbf->b->begin() + rbf->read_index + length, buf);
  rbf->read_index += length;
}


const std::vector<std::byte>& Serializer::serialize(SEXP val) {
  buf.clear();//hopefully, it keeps the capacity as it was
  R_outpstream_st out;


  R_InitOutPStream(&out, reinterpret_cast<R_pstream_data_t>(&buf),
                   R_pstream_binary_format, 3,
                   append_byte, append_buf,
                   NULL, R_NilValue);

  R_Serialize(val, &out);
  bytes_serialized += buf.size();

  return buf;
}


SEXP Serializer::unserialize(std::vector<std::byte>& buffer) {
  R_inpstream_st in;

  read_buffer.b = &buffer;
  read_buffer.read_index = 0;

  R_InitInPStream(&in, reinterpret_cast<R_pstream_data_t>(&read_buffer),
                  R_pstream_binary_format,
                  get_byte, get_buf,
                  NULL, R_NilValue);

  assert(read_buffer.read_index == buffer.size());

  bytes_unserialized += buffer.size();

  return R_Unserialize(&in);
}
