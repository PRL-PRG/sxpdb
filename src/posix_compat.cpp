// Windows implementations of the POSIX file-I/O shims declared in
// posix_compat.h. On Unix this file compiles to nothing (everything is guarded
// by _WIN32); on Windows it is the single translation unit that pulls in
// <windows.h>.

#ifdef _WIN32

// Keep <windows.h> from dragging in the whole Win32 surface and, crucially,
// from defining min()/max() macros that clash with std::min/std::max.
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <io.h> // _get_osfhandle

#include "posix_compat.h"

long sysconf(int /*name*/) {
  SYSTEM_INFO si;
  GetSystemInfo(&si);
  return static_cast<long>(si.dwPageSize);
}

ssize_t pread(int fd, void* buf, size_t count, uint64_t offset) {
  HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
  if (h == INVALID_HANDLE_VALUE) return -1;
  OVERLAPPED ov;
  ZeroMemory(&ov, sizeof(ov));
  ov.Offset = static_cast<DWORD>(offset & 0xFFFFFFFFull);
  ov.OffsetHigh = static_cast<DWORD>(offset >> 32);
  DWORD got = 0;
  if (!ReadFile(h, buf, static_cast<DWORD>(count), &got, &ov)) {
    // Reading at/after EOF is reported as an error; treat it as 0 bytes read.
    if (GetLastError() == ERROR_HANDLE_EOF) return 0;
    return -1;
  }
  return static_cast<ssize_t>(got);
}

ssize_t pwrite(int fd, const void* buf, size_t count, uint64_t offset) {
  HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
  if (h == INVALID_HANDLE_VALUE) return -1;
  OVERLAPPED ov;
  ZeroMemory(&ov, sizeof(ov));
  ov.Offset = static_cast<DWORD>(offset & 0xFFFFFFFFull);
  ov.OffsetHigh = static_cast<DWORD>(offset >> 32);
  DWORD put = 0;
  if (!WriteFile(h, buf, static_cast<DWORD>(count), &put, &ov)) return -1;
  return static_cast<ssize_t>(put);
}

#endif // _WIN32
