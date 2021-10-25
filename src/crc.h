#include <stdint.h> // uint32_t

#ifndef RCRD_CRC_H
#define RCRD_CRC_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * This function perform CRC32 calculation on the input.
 * @method CRC32
 * @param  bytes a null terminated string
 * @return       CRC32 value in form of a 32 bit unsigned integer
 */
uint32_t CRC32(const char* bytes);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // RCRD_CRC_H
