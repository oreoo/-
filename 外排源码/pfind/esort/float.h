#ifndef PFIND_ESORT_FLOAT_H_
#define PFIND_ESORT_FLOAT_H_

#include <cstdint>


uint64_t float_encode(const char *src, char **end_ptr);

size_t floats_encode(uint64_t *dst, int n, const char *src, char **end_ptr, size_t *invalid);

char *float_decode(char *dst, uint64_t src);

char *floats_decode(char *dst, int n, const uint64_t *src);


#endif  // PFIND_ESORT_FLOAT_H_
