#ifdef _WIN32
#include <windows.h>
inline unsigned long long now() {
  LARGE_INTEGER freq, counter;
  QueryPerformanceFrequency(&freq);
  QueryPerformanceCounter(&counter);
  return counter.QuadPart * 1000000000ULL / freq.QuadPart;
}
#else
#include <time.h>
inline unsigned long long now() {
  timespec ts;
  ::clock_gettime(CLOCK_REALTIME, &ts);
  return ts.tv_sec * 1000000000 + ts.tv_nsec;
}
#endif
