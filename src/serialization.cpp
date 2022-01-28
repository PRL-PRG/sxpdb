#include "serialization.h"

#include <cassert>
#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>
#include <Rversion.h>

#include <algorithm>


Serializer::Serializer(size_t size) {
  buf.reserve(size);

  int r_version = R_VERSION;
  std::memcpy(Serializer::header.data() + 6, &r_version, sizeof(int));
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

std::byte* Serializer::jump_header(std::vector<std::byte>& buffer) {
  // We assume binary, version 3
  char* buf = reinterpret_cast<char*>(buffer.data());

  buf += 2;//go past "B\n"
  buf += sizeof(int) * 3; //version, R version, R min version

  int nelen = 0;
  std::memcpy(&nelen, buf, sizeof(int));
  buf += sizeof(int); // the length

  buf += nelen;

  return reinterpret_cast<std::byte*>(buf);
}

const char* read_length(const char* buf, size_t& size) {
  int len = 0;
  std::memcpy(&len, buf, sizeof(int));
  buf += sizeof(int);
  if(len == -1) {
    unsigned int len1, len2;
    std::memcpy(&len1, buf, sizeof(int));
    buf += sizeof(int);
    std::memcpy(&len2, buf, sizeof(int));
    buf += sizeof(int);
    size = (((size_t) len1) << 32) + len2;
  }
  else {
    size = len;
  }

  return buf;
}

const sexp_view_t Serializer::unserialize_view(const std::vector<std::byte>& buf) {
  const char* data = reinterpret_cast<const char*>(buf.data());

  sexp_view_t sexp_view;

  // the header is already not there anymore

  int flags = 0;
  std::memcpy(&flags, data, sizeof(int));
  sexp_view.type = flags & 255;
  bool has_attr = flags & (1 << 9);

  data += sizeof(int);

  // We handle only vector types
  switch(sexp_view.type) {
    case LGLSXP:
    case INTSXP:
    case REALSXP:
    case CPLXSXP:
    case STRSXP:
      data = read_length(data, sexp_view.length);
      break;
    default:
      return sexp_view;
  }


  switch(sexp_view.type) {
    case LGLSXP:
    case INTSXP:
      sexp_view.data = data;
      sexp_view.element_size = sizeof(int);
      break;
    case REALSXP:
      sexp_view.data = data;
      sexp_view.element_size = sizeof(double);
      break;
    case CPLXSXP:
      sexp_view.data = data;
      sexp_view.element_size = sizeof(Rcomplex);
      break;
    case STRSXP:
      sexp_view.data = data;
      // the elements are CHARSXP and have variable size
      //it is easy to find NA though:
      // read the length, if is -1, it is NA< otherwise, jump length to the next item
      break;
  }


  return sexp_view;
}

void Serializer::append_byte(R_outpstream_t stream, int c)  { //add ints, not chars??
  WriteBuffer* wbf = static_cast<WriteBuffer*>(stream->data);

  if(wbf->out_of_header) {
    wbf->b.push_back(static_cast<std::byte>(c));
  }
  else {
    wbf->header_index++;
  }
}

void Serializer::append_buf(R_outpstream_t stream, void *buffer, int length) {
  WriteBuffer* wbf = static_cast<WriteBuffer*>(stream->data);

  std::byte* cbuf = static_cast<std::byte*>(buffer);

  if(wbf->out_of_header) {
    wbf->b.insert(wbf->b.end(), cbuf, cbuf+length);
  }
  else if(wbf->header_index == 2 + 3 * sizeof(int) ) {// decode the encoding length
    assert(length == sizeof(int));
    std::memcpy(&wbf->encoding_length, buffer, sizeof(int));
    wbf->header_index += length;
  }
  else if(wbf->header_index > 2 + 3 * sizeof(int)) { // we are reading the encoding now
    wbf->encoding_length -= length;
    wbf->header_index += length;// not actually needed...
    if(wbf->encoding_length <= 0) {// that should happen in just one go
      wbf->out_of_header = true;
    }
  }
  else {
    wbf->header_index += length;
  }
}

int Serializer::get_byte(R_inpstream_t stream) {
  ReadBuffer* rbf = static_cast<ReadBuffer*>(stream->data);

  if(rbf->header_index < header.size()) {
    return static_cast<int>(header[rbf->header_index++]);
  }
  else {
    return static_cast<int>(rbf->b[rbf->read_index++]);
  }
}


void Serializer::get_buf(R_inpstream_t stream, void *buffer, int length) {
  ReadBuffer* rbf = static_cast<ReadBuffer*>(stream->data);

  std::byte* buf = static_cast<std::byte*>(buffer);

  // This will be only used to read the encoding string
  // There should never be a reading that overlaps the header and the data after
  if(rbf->header_index < header.size()) {
      std::copy_n(reinterpret_cast<std::byte*>(header.data()) + rbf->header_index, length, buf);
      rbf->header_index += length;
      assert(rbf->header_index <= header.size());
  }
  else {
    std::copy_n(rbf->b.begin() + rbf->read_index, length, buf);
    rbf->read_index += length;
  }
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

  WriteBuffer write_buffer(buf);
  R_outpstream_st out;

#ifndef KEEP_ENVIRONMENTS
  R_InitOutPStream(&out, reinterpret_cast<R_pstream_data_t>(&write_buffer),
                   R_pstream_binary_format, 3,
                   append_byte, append_buf,
                   refhook_write, R_NilValue);
#else
  R_InitOutPStream(&out, reinterpret_cast<R_pstream_data_t>(&write_buffer),
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
