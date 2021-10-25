#include <stdint.h>

#include "crc_table.h"
#include "crc.h"

/**
 * This function perform CRC32 calculation on the input.
 * @method CRC32
 * @param  bytes a null terminated string
 * @return       CRC32 value in form of a 32 bit unsigned integer
 */
uint32_t CRC32(const char* bytes) {
	uint32_t crc32 = 0xFFFFFFFF;
	for(int i = 0; bytes[i] != '\0'; ++i) {
		uint32_t byte = bytes[i];
		uint32_t lookup_index = (crc32 ^ byte) & 0xFF;

		crc32 = (crc32 >> 8) ^ crc_table[lookup_index];
	}
	return crc32 ^ 0xFFFFFFFF;
}
