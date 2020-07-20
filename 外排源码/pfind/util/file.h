#ifndef PFIND_UTIL_FILE_H_
#define PFIND_UTIL_FILE_H_

#include <cstdio>


int isdir(const char *path);

int makedir(const char *path);

int makedirs(const char *path);

FILE *openfile(const char *path, const char *modes, size_t buffer = 1u << 20u);

size_t getfilesize(const char *path);

size_t getfilepos(FILE *file);

size_t readlines(char *dst, size_t len, size_t tolerance, FILE *src);


#endif  // PFIND_UTIL_FILE_H_
