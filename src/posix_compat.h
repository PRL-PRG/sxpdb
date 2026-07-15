#ifndef SXPDB_POSIX_COMPAT_H
#define SXPDB_POSIX_COMPAT_H

// Portability shims for the few POSIX file-I/O primitives sxpdb relies on.
//
// On Unix these come from the system headers and this file adds almost nothing.
// On Windows (mingw / Rtools) some are missing or spelled differently, so we
// declare equivalents here and implement them in posix_compat.cpp. The Windows
// bits are guarded by _WIN32 and must never shadow the real POSIX functions on
// Unix.
//
// The implementations live in the .cpp (not inline here) so that <windows.h> is
// pulled into a single translation unit rather than every file that includes
// table.h.

// --- O_BINARY ---------------------------------------------------------------
// POSIX has no text/binary distinction, so <fcntl.h> does not define O_BINARY;
// define it to 0 there so `open(..., O_BINARY)` stays portable. On Windows the
// flag IS defined and is essential: without it the CRT read()/write() perform
// CR/LF translation (and treat 0x1A as EOF), which corrupts the binary database.
#include <fcntl.h>
#ifndef O_BINARY
#define O_BINARY 0
#endif

#ifdef _WIN32

#include <sys/types.h> // ssize_t
#include <sys/stat.h>  // _S_IREAD / _S_IWRITE
#include <cstddef>     // size_t
#include <cstdint>     // uint64_t

// --- open() permission bits -------------------------------------------------
// Windows has no POSIX permission model. mingw's <sys/stat.h> defines only
// _S_IREAD / _S_IWRITE; the group/other bits do not exist. The mode argument to
// open() with O_CREAT is effectively advisory on Windows, so map the owner bits
// to their _S_* spellings and define the group bits to 0.
#ifndef S_IRUSR
#define S_IRUSR _S_IREAD
#endif
#ifndef S_IWUSR
#define S_IWUSR _S_IWRITE
#endif
#ifndef S_IRGRP
#define S_IRGRP 0
#endif
#ifndef S_IWGRP
#define S_IWGRP 0
#endif

// --- sysconf(_SC_PAGE_SIZE) -------------------------------------------------
// Only the page-size query is used. The shim ignores `name` and returns the
// system page size (see posix_compat.cpp). mingw declares neither symbol.
#ifndef _SC_PAGE_SIZE
#define _SC_PAGE_SIZE 0
#endif
long sysconf(int name);

// --- pread / pwrite ---------------------------------------------------------
// mingw provides neither. Implemented with Win32 overlapped I/O, which carries
// the offset in the OVERLAPPED struct and does NOT move the file's current
// position -- preserving the positioned, concurrent-read semantics the callers
// depend on. The offset is uint64_t so large files work regardless of off_t.
ssize_t pread(int fd, void* buf, size_t count, uint64_t offset);
ssize_t pwrite(int fd, const void* buf, size_t count, uint64_t offset);

#endif // _WIN32

#endif // SXPDB_POSIX_COMPAT_H
