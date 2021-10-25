#include <Rinternals.h>

#ifndef RCRD_BYTE_VECTOR_H
#define RCRD_BYTE_VECTOR_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct byte_vector_st {
	size_t capacity;
	size_t size;
	unsigned char *buf;
} *byte_vector_t;

byte_vector_t make_vector(size_t capacity);

void free_vector(byte_vector_t vector);

// Clear content in a vector
void free_content(byte_vector_t vector);

// Required for make use of a R_outpstream_t
void append_byte(R_outpstream_t stream, int c);

// Required for make use of a R_outpstream_t
void append_buf(R_outpstream_t stream, void *buf, int length);

// Required for make use of a R_inpstream_t
int get_byte(R_inpstream_t stream);

// Required for make use of a R_inpstream_t
void get_buf(R_inpstream_t stream, void *buf, int length);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_BYTE_VECTOR_H
