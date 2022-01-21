#include "serialization.h"

#include <cassert>
#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include <Rversion.h>

#include <algorithm>


Serializer::Serializer(size_t size) {
  buf.reserve(size);
}

SEXP Serializer::analyze_header(std::vector<std::byte>& buf) {
  char * buffer;
  const int r_codeset_max = 63;
  int i = 0;
  if(buf.size() < 2 * sizeof(char)) {
    Rf_error("Not enough bytes for a valid format in the header");
  }
  buffer = reinterpret_cast<char*>(buf.data());
  if(buffer[0] != 'B') {
    Rf_error("Format %c is not supported.\n", buffer[0]);
  }
  if(buffer[1] != '\n') {
    Rf_error("Second format character should be \\n but is %c.\n", buffer[1]);
  }

  i += 2;

  if(buf.size() < 2 * sizeof(char) + 3 * sizeof(int)) {
    Rf_error("Not a valid header.\n");
  }
  int version = 0;
  std::memcpy(&version, buffer + i, sizeof(int));

  if(version == 3 && buf.size() < 3 * sizeof(char) + 4 * sizeof(int)) {
    Rf_error("Not a valid v3 header.\n");
  }
  i+= sizeof(int);

  int r_version = 0;
  std::memcpy(&r_version, buffer + i, sizeof(int));
  i+=sizeof(int);
  int r_min_version = 0;
  std::memcpy(&r_min_version, buffer + i, sizeof(int));
  i+= sizeof(int);

  // Native encoding
  if(version == 3) {
    char native_enc[r_codeset_max];
    int nelen = 0;
    std::memcpy(&nelen, buffer + i, sizeof(int));
    i+=sizeof(int);

    if(buf.size() < i + nelen ) {
      Rf_error("Buffer is too small for the encoding.\n");
    }
    std::memcpy(&native_enc, buffer + i, nelen);
    native_enc[nelen] = '\0';

    const char *names[] = {"version", "RVersion", "minRVersion", "encoding_size", "encoding", "header_size", ""};
    SEXP l = PROTECT(Rf_mkNamed(VECSXP, names));

    SET_VECTOR_ELT(l, 0, Rf_ScalarInteger(version));
    SET_VECTOR_ELT(l, 1, Rf_ScalarInteger(r_version));
    SET_VECTOR_ELT(l, 2, Rf_ScalarInteger(r_min_version));
    SET_VECTOR_ELT(l, 3, Rf_ScalarInteger(nelen));
    SET_VECTOR_ELT(l, 4, Rf_mkString(native_enc));
    SET_VECTOR_ELT(l, 5, Rf_ScalarInteger(i + nelen));

    UNPROTECT(1);

    return l;
  }
  else if (version == 2){
    const char *names[] = {"version", "RVersion", "minRVersion", "header_size", ""};
    SEXP l = PROTECT(Rf_mkNamed(VECSXP, names));

    SET_VECTOR_ELT(l, 0, Rf_ScalarInteger(version));
    SET_VECTOR_ELT(l, 1, Rf_ScalarInteger(r_version));
    SET_VECTOR_ELT(l, 2, Rf_ScalarInteger(r_min_version));
    SET_VECTOR_ELT(l, 3, Rf_ScalarInteger(i));

    UNPROTECT(1);

    return l;
  }
  return Rf_ScalarInteger(version);
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
